package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// codexControlEvidenceTimeout bounds how long a control call waits for the
// app-server to report that the controlled turn terminated. It is generous:
// steering a turn does not end it immediately — the agent keeps working with
// the new input and may run for a while before it completes — so this is sized
// for "the turn is still going" rather than for round-trip latency. Exceeding
// it is not a timeout on the RPC (which was already acknowledged) but a
// statement that we cannot prove the control action took effect.
const codexControlEvidenceTimeout = 5 * time.Minute

// codexControlEvidenceBuffer is the per-waiter queue depth. Terminal events for
// OTHER turns (Codex multiplexes subagent threads, and a thread can move on to
// a later turn) must not evict the one event a waiter actually needs, so the
// queue holds several rather than one.
const codexControlEvidenceBuffer = 8

// codexControlRequestTimeout bounds the control RPC itself — the round trip to
// the app-server, not the turn. turn/steer and turn/interrupt are deliberately
// NOT in isCodexHandshakeRPC (they are not handshake calls), so without this
// they inherit only the caller's context; a caller passing context.Background()
// to an app-server that accepts the frame and never answers would block
// forever, and the evidence timeout below would never get to run because the
// wait happens after the RPC returns.
const codexControlRequestTimeout = 30 * time.Second

// codexControlEvidenceBus fans terminal turn events out to the turn-control
// calls waiting on them. It is a broadcast rather than a single channel because
// more than one control call can legitimately be in flight — and because the
// publisher is the stdout reader goroutine, which must never block on a
// consumer: a control caller that has gone away, or is slow, cannot be allowed
// to stall the stream that carries the whole turn.
type codexControlEvidenceBus struct {
	mu   sync.Mutex
	subs map[int]chan TerminalEvidence
	next int
}

// subscribe registers a waiter and returns its channel plus the func that
// removes it. Callers MUST defer the returned func; without it the bus would
// keep publishing to a channel nobody reads.
func (b *codexControlEvidenceBus) subscribe() (<-chan TerminalEvidence, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs == nil {
		b.subs = make(map[int]chan TerminalEvidence)
	}
	id := b.next
	b.next++
	ch := make(chan TerminalEvidence, codexControlEvidenceBuffer)
	b.subs[id] = ch
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs, id)
	}
}

// publish delivers evidence to every current waiter, dropping it for any waiter
// whose queue is full rather than blocking the stdout reader. A dropped event
// degrades to a correlation timeout for that waiter, which fails closed.
func (b *codexControlEvidenceBus) publish(ev TerminalEvidence) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// codexTerminalKind maps a Codex turn status onto the provider-neutral
// classification. aborted is passed in rather than re-derived so this cannot
// drift from the aborted determination the turn lifecycle already made.
func codexTerminalKind(status string, aborted bool) TurnTerminalKind {
	switch {
	case aborted:
		return TurnTerminalAborted
	case status == "completed":
		return TurnTerminalCompleted
	default:
		// Fail CLOSED. Only a status we recognize as success is reported as a
		// completed turn; "failed", anything unrecognized ("error", "timeout",
		// "incomplete", a status a future Codex adds) and an absent status all
		// resolve to failed. The asymmetry is deliberate: a spurious "the turn
		// failed" costs a caller a retry, whereas a spurious "completed" tells
		// it the steer or interrupt took effect when nothing proves that — the
		// exact failure this whole evidence contract exists to prevent.
		return TurnTerminalFailed
	}
}

// Codex is the reference implementation of the provider-neutral turn-control
// capability declared in turn_control.go. It rides the same app-server
// JSON-RPC transport that starts and streams the turn, so steering or
// interrupting costs no extra process and leaves normal turn execution — start,
// stream, resume, the watchdogs — completely untouched.

// codexAtomicString is a race-free view of a string that one goroutine writes
// and another reads. codexClient's threadID/turnID are written without a lock
// because the goroutines that write them are also their only readers; turn
// control adds a reader from outside that set, and this is what it reads.
type codexAtomicString struct{ v atomic.Pointer[string] }

func (a *codexAtomicString) set(s string) { a.v.Store(&s) }

func (a *codexAtomicString) get() string {
	if p := a.v.Load(); p != nil {
		return *p
	}
	return ""
}

// setThreadID and setTurnID are the ONLY places threadID/turnID may be written.
// Assigning either field directly would leave its control mirror stale, and
// turn control would then address a turn that is no longer the live one — a
// failure that is invisible until someone tries to steer.
func (c *codexClient) setThreadID(id string) {
	c.threadID = id
	c.controlThreadID.set(id)
}

func (c *codexClient) setTurnID(id string) {
	c.turnID = id
	c.controlTurnID.set(id)
}

// retireControlTurn clears the control pointer once turnID's turn has reached a
// terminal state, so controlTurn stops accepting it as a live target.
//
// It deliberately clears ONLY the control mirror and leaves c.turnID intact:
// c.turnID is still read after the turn ends to attribute diagnostics and
// timeout reports to the turn they came from, and blanking it would degrade
// those messages. The mirror answers a different question — "is there a turn I
// may still steer or interrupt?" — whose honest answer here is no.
//
// A mismatched id is ignored so a stale or subagent terminal cannot retire the
// pointer for a turn that is genuinely still running.
func (c *codexClient) retireControlTurn(turnID string) {
	if turnID == "" || c.controlTurnID.get() != turnID {
		return
	}
	c.controlTurnID.set("")
}

// controlTurn performs one turn-control operation against the app-server. It is
// the func stored in Session.turnControl for every Codex session.
//
// The caller's TurnID is checked against the live turn BEFORE anything is sent.
// The app-server would reject a stale expectedTurnId on steer anyway, but
// interrupt takes a plain turnId and would happily cancel whatever turn was
// named — so the check is what makes both operations fail the same way when the
// thread has moved on, instead of one erroring and the other cancelling the
// wrong turn.
func (c *codexClient) controlTurn(ctx context.Context, req TurnControlRequest) (TurnControlResult, error) {
	c.mu.Lock()
	processErr := c.processErr
	c.mu.Unlock()
	if processErr != nil {
		return TurnControlResult{}, fmt.Errorf("codex turn control: %w", processErr)
	}

	threadID, turnID := c.controlThreadID.get(), c.controlTurnID.get()
	if threadID == "" || turnID == "" || turnID != req.TurnID {
		return TurnControlResult{}, ErrTurnControlInactive
	}

	var (
		method string
		params any
	)
	switch req.Op {
	case TurnControlSteer:
		method = "turn/steer"
		params = codexTurnSteerParams{
			ThreadID: threadID,
			TurnID:   turnID,
			Input:    codexTurnControlInput(req.Input),
		}
	case TurnControlInterrupt:
		method = "turn/interrupt"
		params = codexTurnInterruptParams{ThreadID: threadID, TurnID: turnID}
	default:
		// Unreachable through Session.ControlTurn, which validates first. Kept
		// so a future operation added to the neutral contract fails loudly here
		// rather than silently taking the steer branch.
		return TurnControlResult{}, fmt.Errorf("%w: codex cannot perform %q", ErrTurnControlInvalid, req.Op)
	}

	// Subscribe BEFORE sending. The app-server can emit the turn's terminal
	// notification as soon as it has processed the control request, so a
	// subscription taken after request() returns could miss the very event it
	// exists to wait for. Subscribing first makes the wait immune to that
	// ordering entirely.
	evidence, unsubscribe := c.controlEvidence.subscribe()
	defer unsubscribe()

	// Bound the RPC independently of the evidence wait: an app-server that
	// takes the frame and never replies must not hang the caller.
	requestCtx, cancelRequest := context.WithTimeout(ctx, codexControlRequestTimeout)
	defer cancelRequest()
	if _, err := c.request(requestCtx, method, params); err != nil {
		// A dead process is not a wire rejection — keep it matching
		// errCodexProcessExited so callers that distinguish "the agent died"
		// from "the agent said no" still can.
		if errors.Is(err, errCodexProcessExited) {
			return TurnControlResult{}, fmt.Errorf("codex %s: %w", method, err)
		}
		return TurnControlResult{}, fmt.Errorf("%w: codex %s: %w", ErrTurnControlRejected, method, err)
	}
	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("codex turn control sent",
			"method", method,
			"thread_id", threadID,
			"turn_id", turnID,
		)
	}

	// The RPC is acknowledged. That is NOT the answer: it proves the app-server
	// accepted the request, not that the turn was steered or cancelled. Wait
	// for the turn's own terminal event and correlate it before reporting
	// success.
	terminal, err := c.awaitTerminalEvidence(ctx, evidence, threadID, turnID)
	if err != nil {
		return TurnControlResult{}, err
	}
	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("codex turn control correlated",
			"method", method,
			"thread_id", terminal.ThreadID,
			"turn_id", terminal.TurnID,
			"status", terminal.Status,
			"kind", string(terminal.Kind),
		)
	}
	return TurnControlResult{
		Provider:         "codex",
		SessionID:        threadID,
		TurnID:           turnID,
		Op:               req.Op,
		TerminalEvidence: terminal,
	}, nil
}

// evidenceTimeout is the configured wait, falling back to the package default.
func (c *codexClient) evidenceTimeout() time.Duration {
	if c.controlEvidenceTimeout > 0 {
		return c.controlEvidenceTimeout
	}
	return codexControlEvidenceTimeout
}

// awaitTerminalEvidence blocks until the app-server reports that the named turn
// on the named thread reached a terminal state, and returns that as evidence.
//
// Every exit that is not a correlated non-failed terminal is an error with zero
// evidence. In particular a terminal event for a DIFFERENT turn or thread does
// not end the wait: it proves nothing about our target, and treating it as an
// answer is exactly the bug that would let a subagent's completion be credited
// as our steer taking effect. Such events are counted for diagnostics and the
// wait continues until the target terminates or the timeout expires.
func (c *codexClient) awaitTerminalEvidence(
	ctx context.Context,
	evidence <-chan TerminalEvidence,
	threadID, turnID string,
) (TerminalEvidence, error) {
	timer := time.NewTimer(c.evidenceTimeout())
	defer timer.Stop()

	uncorrelated := 0

	// classify turns one queued event into a verdict. ok=false means "not about
	// our target, keep waiting".
	classify := func(ev TerminalEvidence) (TerminalEvidence, error, bool) {
		if !ev.correlates(threadID, turnID) {
			uncorrelated++
			return TerminalEvidence{}, nil, false
		}
		if ev.Kind == TurnTerminalFailed {
			detail := c.getTurnError()
			if detail == "" {
				detail = ev.Status
			}
			return TerminalEvidence{}, fmt.Errorf("%w: thread %s turn %s: %s",
				ErrTurnControlTurnFailed, threadID, turnID, detail), true
		}
		return ev, nil, true
	}

	// drain empties whatever is already queued before we honour a competing
	// ready case. Go's select picks uniformly at random among ready cases, so
	// without this a turn that really did terminate loses a coin flip to
	// processDone/ctx/timer — the publisher buffers the evidence and then drives
	// the lifecycle straight into markProcessExited, leaving both ready. This
	// mirrors the same nested-drain guard request() already uses.
	drain := func() (TerminalEvidence, error, bool) {
		for {
			select {
			case ev := <-evidence:
				if res, err, done := classify(ev); done {
					return res, err, true
				}
			default:
				return TerminalEvidence{}, nil, false
			}
		}
	}

	for {
		select {
		case ev := <-evidence:
			if res, err, done := classify(ev); done {
				return res, err
			}

		case <-c.processDone:
			if res, err, done := drain(); done {
				return res, err
			}
			// The process died instead of the turn terminating. Process exit —
			// including a SIGTERM or SIGKILL that produced it — is NOT evidence
			// that the control action took effect, so it fails closed here
			// rather than being credited as an interrupt that worked.
			err := c.getProcessErr()
			if err == nil {
				err = errCodexProcessExited
			}
			return TerminalEvidence{}, fmt.Errorf(
				"%w: codex process exited before thread %s turn %s produced terminal evidence: %w",
				ErrTurnControlUncorrelated, threadID, turnID, err)

		case <-ctx.Done():
			if res, err, done := drain(); done {
				return res, err
			}
			return TerminalEvidence{}, fmt.Errorf(
				"%w: waiting for thread %s turn %s terminal evidence: %w",
				ErrTurnControlUncorrelated, threadID, turnID, ctx.Err())

		case <-timer.C:
			if res, err, done := drain(); done {
				return res, err
			}
			return TerminalEvidence{}, fmt.Errorf(
				"%w: no terminal event for thread %s turn %s within %s (%d unrelated terminal events seen)",
				ErrTurnControlUncorrelated, threadID, turnID, c.evidenceTimeout(), uncorrelated)
		}
	}
}

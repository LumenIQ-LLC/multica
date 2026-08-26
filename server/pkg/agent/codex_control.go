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

// codexSteerAcceptanceWindow bounds how long a STEER looks for the target turn
// to terminate before concluding that it did not — which for a steer is the
// success case, not a failure. It is short on purpose: a steered turn is
// expected to keep working, so this window only has to be long enough to catch
// a turn that dies ON the steer (a rejection wearing a success's clothes).
//
// It replaces codexControlEvidenceTimeout for steer only. Interrupt keeps the
// full terminal wait, because an interrupt's claim IS that the turn ended. See
// docs/design/turn-control-effect-contract.md.
const codexSteerAcceptanceWindow = 2 * time.Second

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
	// accepted the request, not that the turn was steered or cancelled.
	//
	// What counts as the answer differs by operation, because the two claim
	// different things (docs/design/turn-control-effect-contract.md):
	//   - interrupt claims the turn ENDED, so it waits for the turn's own
	//     terminal event and correlates it;
	//   - steer claims the input entered the LIVE turn, so it waits only long
	//     enough to catch a turn that died on the steer, and reports a turn
	//     that is still running as the success it is.
	var (
		terminal TerminalEvidence
		err      error
	)
	if req.Op == TurnControlSteer {
		terminal, err = c.awaitSteerEffect(ctx, evidence, threadID, turnID)
	} else {
		terminal, err = c.awaitTerminalEvidence(ctx, evidence, threadID, turnID, c.evidenceTimeout())
	}
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

// steerAcceptanceWindow is the steer wait. It never exceeds the configured
// evidence timeout, so a test that shortens the timeout to keep the fail-closed
// paths fast does not accidentally leave steer waiting the full default.
func (c *codexClient) steerAcceptanceWindow() time.Duration {
	if w := c.evidenceTimeout(); w < codexSteerAcceptanceWindow {
		return w
	}
	return codexSteerAcceptanceWindow
}

// awaitSteerEffect implements the STEER half of the effect contract.
//
// A correlated terminal inside the acceptance window is reported exactly as
// awaitTerminalEvidence would: a turn that ended on the steer is not a steer
// that took effect, and a failed turn is still ErrTurnControlTurnFailed.
//
// The window elapsing is the SUCCESS case — but only once "the turn is still
// live" has been checked rather than assumed. The caller's context must not be
// cancelled, the process must not have exited, and the control mirror must
// still name the target turn (every terminal path retires it, so a retired
// mirror means the turn ended and we simply did not see correlated evidence for
// it). Any of those failing keeps the original fail-closed error, which is what
// preserves the B1 guarantee that a bare acknowledgement is never success.
func (c *codexClient) awaitSteerEffect(
	ctx context.Context,
	evidence <-chan TerminalEvidence,
	threadID, turnID string,
) (TerminalEvidence, error) {
	ev, err := c.awaitTerminalEvidence(ctx, evidence, threadID, turnID, c.steerAcceptanceWindow())
	if err == nil {
		return ev, nil
	}
	// A correlated failure (ErrTurnControlTurnFailed) is a real verdict about
	// our turn, never "we ran out of time". Only the latter can become success.
	if !errors.Is(err, ErrTurnControlUncorrelated) {
		return TerminalEvidence{}, err
	}
	if ctx.Err() != nil {
		return TerminalEvidence{}, err
	}
	select {
	case <-c.processDone:
		return TerminalEvidence{}, err
	default:
	}
	if c.controlTurnID.get() != turnID {
		return TerminalEvidence{}, err
	}
	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("codex steer accepted, turn still running",
			"thread_id", threadID,
			"turn_id", turnID,
			"acceptance_window", c.steerAcceptanceWindow().String(),
		)
	}
	return TerminalEvidence{
		ThreadID: threadID,
		TurnID:   turnID,
		Status:   "running",
		Kind:     TurnStillRunning,
	}, nil
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
	wait time.Duration,
) (TerminalEvidence, error) {
	timer := time.NewTimer(wait)
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
				ErrTurnControlUncorrelated, threadID, turnID, wait, uncorrelated)
		}
	}
}

// publishControlTerminal is the ONE place a turn's terminal state is turned
// into evidence for waiting turn-control calls. Every notification that ends a
// turn must route through it, not just turn/completed: a waiter blocks until it
// sees correlated evidence, so a terminal path that publishes nothing does not
// merely lose detail — it strands the caller for the whole evidence timeout and
// then reports "uncorrelated", which reads as "we never found out" when in fact
// the app-server told us plainly that the turn was over.
//
// Ordering is load-bearing and matches the turn/completed path it was factored
// out of: any failure detail must already be recorded (setTurnError) BEFORE the
// publish, because the publish wakes a waiter that immediately reads
// getTurnError() to describe the failure. Retiring the control pointer also
// happens before the publish, so a control call that arrives after this one
// fails fast with ErrTurnControlInactive instead of addressing a dead turn.
//
// turnID may be empty on the paths whose notification carries no turn id
// (top-level `error`, thread/status/changed); the live control turn is used
// then. If there is no live turn there is nothing any waiter could correlate
// against, so it publishes nothing rather than emitting uncorrelatable evidence.
func (c *codexClient) publishControlTerminal(threadID, turnID, status string, aborted bool) {
	if turnID == "" {
		turnID = c.controlTurnID.get()
	}
	if turnID == "" {
		return
	}
	// threadId is absent on some app-server builds, and the caller has already
	// established that anything reaching here belongs to the tracked thread — so
	// an absent id means "ours" and resolves to c.threadID rather than to empty,
	// which would never correlate.
	if threadID == "" {
		threadID = c.threadID
	}
	if threadID == "" {
		return
	}
	c.retireControlTurn(turnID)
	c.controlEvidence.publish(TerminalEvidence{
		ThreadID: threadID,
		TurnID:   turnID,
		Status:   status,
		Kind:     codexTerminalKind(status, aborted),
	})
}

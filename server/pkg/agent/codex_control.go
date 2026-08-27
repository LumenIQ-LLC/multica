package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// codexControlEvidenceTimeout bounds how long a control call waits for the
// app-server to report that the controlled turn terminated. It is generous:
// steering a turn does not end it immediately — the agent keeps working with
// the new input and may run for a while before it completes — so this is sized
// for "the turn is still going" rather than for round-trip latency. Exceeding
// it is not a timeout on the RPC (which was already acknowledged) but a
// statement that we cannot prove the control action took effect.
const codexControlEvidenceTimeout = controlEvidenceTimeout

// codexControlEvidenceBuffer is the per-waiter queue depth. Terminal events for
// OTHER turns (Codex multiplexes subagent threads, and a thread can move on to
// a later turn) must not evict the one event a waiter actually needs, so the
// queue holds several rather than one.
const codexControlEvidenceBuffer = controlEvidenceBuffer

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
const codexSteerAcceptanceWindow = steerAcceptanceWindow

// codexControlEvidenceBus fans terminal turn events out to the turn-control
// calls waiting on them. It is a broadcast rather than a single channel because
// more than one control call can legitimately be in flight — and because the
// publisher is the stdout reader goroutine, which must never block on a
// consumer: a control caller that has gone away, or is slow, cannot be allowed
// to stall the stream that carries the whole turn.
// codexControlEvidenceBus is the shared broadcast bus.
type codexControlEvidenceBus = controlEvidenceBus

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
// codexAtomicString is the shared race-free string holder.
type codexAtomicString = controlAtomicString

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
	return resolveEvidenceTimeout(c.controlEvidenceTimeout)
}

func (c *codexClient) steerAcceptanceWindow() time.Duration {
	return resolveSteerWindow(c.controlEvidenceTimeout)
}

// awaitSteerEffect and awaitTerminalEvidence delegate to the shared,
// provider-neutral implementations in turn_control_evidence.go. The effect
// contract is identical for every backend; only the wire below is Codex.
func (c *codexClient) awaitSteerEffect(
	ctx context.Context,
	evidence <-chan TerminalEvidence,
	threadID, turnID string,
) (TerminalEvidence, error) {
	return awaitSteerEffect(ctx, c, evidence, threadID, turnID, c.steerAcceptanceWindow())
}

func (c *codexClient) awaitTerminalEvidence(
	ctx context.Context,
	evidence <-chan TerminalEvidence,
	threadID, turnID string,
	timeout time.Duration,
) (TerminalEvidence, error) {
	return awaitTerminalEvidence(ctx, c, evidence, threadID, turnID, timeout)
}

// controlHost implementation. These four questions are all the shared
// waiters need; nothing Codex-shaped crosses the seam.
func (c *codexClient) controlProcessDone() <-chan struct{} { return c.processDone }
func (c *codexClient) controlProcessErr() error            { return c.getProcessErr() }
func (c *codexClient) controlTurnFailureDetail() string    { return c.getTurnError() }
func (c *codexClient) controlLiveTurnID() string           { return c.controlTurnID.get() }
func (c *codexClient) controlProvider() string             { return "codex" }
func (c *codexClient) controlLogger() *slog.Logger         { return c.cfg.Logger }

var _ controlHost = (*codexClient)(nil)

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

package agent

import (
	"context"
	"fmt"
	"sync/atomic"
)

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

	if _, err := c.request(ctx, method, params); err != nil {
		return TurnControlResult{}, fmt.Errorf("codex %s: %w", method, err)
	}
	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("codex turn control sent",
			"method", method,
			"thread_id", threadID,
			"turn_id", turnID,
		)
	}
	return TurnControlResult{
		Provider:  "codex",
		SessionID: threadID,
		TurnID:    turnID,
		Op:        req.Op,
	}, nil
}

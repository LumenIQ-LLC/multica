package agent

import (
	"context"
	"errors"
	"fmt"
)

// Turn control lets a caller act on a turn that is ALREADY RUNNING, without
// restarting it: send the agent extra input mid-flight, or cancel it cleanly.
// It is provider-neutral — a caller holding a *Session never learns which
// backend answered — and optional: a backend that cannot address a live turn
// simply leaves the capability unset, and every entry point below reports
// ErrTurnControlUnsupported instead of panicking.
//
// Codex is the reference implementation (turn/steer and turn/interrupt against
// the app-server transport in codex.go). Other providers can adopt it by
// populating Session.turnControl the same way.

// TurnControlOp identifies a provider-neutral turn-control operation.
type TurnControlOp string

const (
	// TurnControlSteer delivers additional user input to a running turn. The
	// turn keeps its context and continues; it is NOT restarted, which is the
	// whole point of steering rather than cancelling and re-prompting.
	TurnControlSteer TurnControlOp = "steer"
	// TurnControlInterrupt cancels a running turn. The session's Result still
	// arrives — normally with an aborted status — so callers wait on Result as
	// usual rather than treating interrupt as the end of the session.
	TurnControlInterrupt TurnControlOp = "interrupt"
)

var (
	// ErrTurnControlUnsupported reports that this session's provider does not
	// implement turn control at all. It is a capability answer, not a failure
	// of the turn: the turn is still running and unaffected.
	ErrTurnControlUnsupported = errors.New("agent: turn control unsupported by provider")
	// ErrTurnControlInactive reports that the provider supports turn control
	// but has no turn matching the request to act on — the turn already
	// finished, never started, or the caller named a turn that is not the live
	// one. The last case is deliberately an error rather than a best-effort
	// retarget: acting on a turn the caller did not name is worse than
	// refusing, because steer input meant for one turn would land in another.
	ErrTurnControlInactive = errors.New("agent: no matching active turn")
	// ErrTurnControlInvalid reports a malformed request. It is detected before
	// any provider I/O, so a rejected request never reaches the agent.
	ErrTurnControlInvalid = errors.New("agent: invalid turn control request")
	// ErrTurnControlUncorrelated reports that the provider acknowledged the
	// control RPC but never produced terminal evidence that the NAMED turn
	// actually reached a terminal state — the evidence never arrived, or what
	// arrived belonged to a different thread or turn.
	//
	// This is the error that makes turn control an effect contract rather than
	// a delivery contract. An acknowledgement only proves the request was
	// accepted for processing; it says nothing about whether the turn was
	// steered or cancelled. Treating an ACK as success would report a control
	// action that may never have taken effect, so the absence of correlated
	// evidence fails closed here instead.
	ErrTurnControlUncorrelated = errors.New("agent: no correlated terminal evidence for the controlled turn")
	// ErrTurnControlRejected reports that the provider rejected the control
	// request itself — a protocol/wire-level refusal, distinct from the request
	// being accepted and then failing to take effect.
	ErrTurnControlRejected = errors.New("agent: provider rejected the turn control request")
	// ErrTurnControlTurnFailed reports that the named turn DID terminate and
	// the evidence correlates, but it terminated in provider-reported failure.
	// The control action is not a success: the caller asked to steer or cancel
	// a turn, and the turn instead died. Kept distinct from
	// ErrTurnControlUncorrelated because the difference matters — here the
	// provider told us what happened, it just was not what was asked for.
	ErrTurnControlTurnFailed = errors.New("agent: controlled turn terminated in failure")
)

// TurnTerminalKind classifies a terminal turn state in provider-neutral terms.
// The provider's own status string is preserved alongside it in
// TerminalEvidence.Status; this is the classification callers switch on.
type TurnTerminalKind string

const (
	// TurnTerminalCompleted is a turn that ran to completion. It is the
	// expected outcome of a successful steer: the turn kept going and finished.
	TurnTerminalCompleted TurnTerminalKind = "completed"
	// TurnTerminalAborted is a turn that was cancelled or interrupted. It is
	// the expected outcome of a successful interrupt.
	TurnTerminalAborted TurnTerminalKind = "aborted"
	// TurnTerminalFailed is a turn that ended in provider-reported failure.
	TurnTerminalFailed TurnTerminalKind = "failed"
)

// TerminalEvidence is the provider's own proof that a specific turn on a
// specific thread reached a terminal state. It is what separates "the control
// RPC was acknowledged" from "the control action took effect on the turn I
// named", and it is deliberately built only from identifiers the provider
// emitted — never from anything the caller supplied and never from an
// out-of-band signal such as process exit, which proves the agent died rather
// than that the turn was controlled.
type TerminalEvidence struct {
	// ThreadID and TurnID are the provider's identifiers as they appeared on
	// the terminal event, so a caller can correlate against a transcript.
	ThreadID string
	TurnID   string
	// Status is the provider's raw terminal status string, preserved verbatim
	// for diagnostics.
	Status string
	// Kind is the provider-neutral classification of Status.
	Kind TurnTerminalKind
}

// correlates reports whether this evidence is proof about the named target.
// Empty identifiers never correlate: absent evidence must not read as matching
// evidence just because the target it is compared against is also empty.
func (e TerminalEvidence) correlates(threadID, turnID string) bool {
	if e.ThreadID == "" || e.TurnID == "" {
		return false
	}
	return e.ThreadID == threadID && e.TurnID == turnID
}

// TurnControlRequest asks a live session to act on one in-flight turn.
type TurnControlRequest struct {
	// Op is the operation to perform. Required.
	Op TurnControlOp
	// TurnID is the turn the caller believes is running. Required, and checked
	// against the provider's live turn before anything is sent: it is the
	// caller's statement of WHICH turn it means, so a provider that has since
	// moved on rejects the request instead of acting on the wrong one. Codex
	// spends it as turn/steer's expectedTurnId and turn/interrupt's turnId.
	TurnID string
	// Input is the additional user input for TurnControlSteer. Required for
	// steer, and must be empty for interrupt, which carries no payload.
	Input string
}

func (r TurnControlRequest) validate() error {
	switch r.Op {
	case TurnControlSteer:
		if r.Input == "" {
			return fmt.Errorf("%w: steer requires input", ErrTurnControlInvalid)
		}
	case TurnControlInterrupt:
		if r.Input != "" {
			return fmt.Errorf("%w: interrupt takes no input", ErrTurnControlInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown operation %q", ErrTurnControlInvalid, r.Op)
	}
	if r.TurnID == "" {
		return fmt.Errorf("%w: turn id is required", ErrTurnControlInvalid)
	}
	return nil
}

// TurnControlResult reports what the provider actually acted on. It records the
// provider's own identifiers so a caller correlating against a transcript can
// tell which conversation and turn were touched.
//
// A TurnControlResult is only ever returned with a nil error when its
// TerminalEvidence correlates to the requested turn — see ControlTurn. A
// zero-valued result therefore always accompanies a failure, and carries no
// evidence a caller could mistake for proof of effect.
type TurnControlResult struct {
	Provider  string // provider that handled the request, e.g. "codex"
	SessionID string // provider-side conversation id (Codex: thread id)
	TurnID    string // turn the provider acted on
	Op        TurnControlOp
	// TerminalEvidence is the provider's proof that TurnID reached a terminal
	// state. This field — not the absence of an error from the underlying RPC —
	// is what proves the control action took effect.
	TerminalEvidence TerminalEvidence
}

// TurnController is the provider-neutral turn-control capability. *Session
// implements it for every backend; SupportsTurnControl is what separates a
// session that can really act from one that will report
// ErrTurnControlUnsupported.
type TurnController interface {
	SupportsTurnControl() bool
	ControlTurn(ctx context.Context, req TurnControlRequest) (TurnControlResult, error)
	SteerTurn(ctx context.Context, turnID, input string) error
	InterruptTurn(ctx context.Context, turnID string) error
}

var _ TurnController = (*Session)(nil)

// SupportsTurnControl reports whether this session's provider can act on an
// in-flight turn. Callers should use it to decide whether to OFFER steering or
// interruption; it does not promise a particular turn is still live, which only
// ControlTurn can answer.
func (s *Session) SupportsTurnControl() bool {
	return s != nil && s.turnControl != nil
}

// ControlTurn performs one turn-control operation against the running turn and
// returns the provider's correlated terminal evidence for it.
//
// ControlTurn is THE API that proves a control action took effect. It returns a
// nil error only when the provider produced terminal evidence for the exact
// turn the caller named; an acknowledged RPC with no such evidence is
// ErrTurnControlUncorrelated, not success. Callers that need to know a turn was
// really steered or really cancelled must use ControlTurn and inspect
// TerminalEvidence — the SteerTurn/InterruptTurn wrappers below discard it and
// are convenience only.
//
// The request is validated before the capability check so a malformed request
// is reported as malformed regardless of which provider is behind the session —
// otherwise the same bad call would surface as "unsupported" on one backend and
// "invalid" on another.
//
// The correlation check below is enforced HERE, at the neutral boundary, rather
// than being left to each provider. Providers still correlate for themselves
// (Codex waits on the terminal event it can actually observe), but centralising
// the final check means no backend — present or future, however it is wired —
// can hand back a successful result whose evidence does not match the requested
// target. Every failure path returns a zero result, so a caller can never read
// evidence out of a failed call.
func (s *Session) ControlTurn(ctx context.Context, req TurnControlRequest) (TurnControlResult, error) {
	if err := req.validate(); err != nil {
		return TurnControlResult{}, err
	}
	if !s.SupportsTurnControl() {
		return TurnControlResult{}, ErrTurnControlUnsupported
	}
	result, err := s.turnControl(ctx, req)
	if err != nil {
		return TurnControlResult{}, err
	}
	if !result.TerminalEvidence.correlates(result.SessionID, req.TurnID) {
		return TurnControlResult{}, fmt.Errorf(
			"%w: provider reported success for turn %q with evidence %+v",
			ErrTurnControlUncorrelated, req.TurnID, result.TerminalEvidence,
		)
	}
	return result, nil
}

// SteerTurn sends additional user input to the turn identified by turnID.
//
// It is a convenience wrapper that DISCARDS the terminal evidence ControlTurn
// returns. A nil error here still means the steer took effect on turnID — the
// same correlation gate runs — but the proof is not handed back, so this is not
// the API to use when the evidence itself must be recorded or inspected.
func (s *Session) SteerTurn(ctx context.Context, turnID, input string) error {
	_, err := s.ControlTurn(ctx, TurnControlRequest{Op: TurnControlSteer, TurnID: turnID, Input: input})
	return err
}

// InterruptTurn cancels the turn identified by turnID. The session's Result
// still arrives afterwards.
//
// Like SteerTurn, this discards the terminal evidence; use ControlTurn when the
// proof of effect matters.
func (s *Session) InterruptTurn(ctx context.Context, turnID string) error {
	_, err := s.ControlTurn(ctx, TurnControlRequest{Op: TurnControlInterrupt, TurnID: turnID})
	return err
}

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
)

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
type TurnControlResult struct {
	Provider  string // provider that handled the request, e.g. "codex"
	SessionID string // provider-side conversation id (Codex: thread id)
	TurnID    string // turn the provider acted on
	Op        TurnControlOp
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

// ControlTurn performs one turn-control operation against the running turn.
//
// The request is validated before the capability check so a malformed request
// is reported as malformed regardless of which provider is behind the session —
// otherwise the same bad call would surface as "unsupported" on one backend and
// "invalid" on another.
func (s *Session) ControlTurn(ctx context.Context, req TurnControlRequest) (TurnControlResult, error) {
	if err := req.validate(); err != nil {
		return TurnControlResult{}, err
	}
	if !s.SupportsTurnControl() {
		return TurnControlResult{}, ErrTurnControlUnsupported
	}
	return s.turnControl(ctx, req)
}

// SteerTurn sends additional user input to the turn identified by turnID.
func (s *Session) SteerTurn(ctx context.Context, turnID, input string) error {
	_, err := s.ControlTurn(ctx, TurnControlRequest{Op: TurnControlSteer, TurnID: turnID, Input: input})
	return err
}

// InterruptTurn cancels the turn identified by turnID. The session's Result
// still arrives afterwards.
func (s *Session) InterruptTurn(ctx context.Context, turnID string) error {
	_, err := s.ControlTurn(ctx, TurnControlRequest{Op: TurnControlInterrupt, TurnID: turnID})
	return err
}

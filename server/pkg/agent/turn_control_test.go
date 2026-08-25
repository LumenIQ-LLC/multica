// Canonical tests for the provider-neutral turn-control contract. Codex's
// implementation of it is covered separately in codex_control_test.go.
package agent

import (
	"context"
	"errors"
	"testing"
)

// stubControlSession builds a Session whose provider records what it received,
// standing in for any backend that implements the capability.
func stubControlSession(result TurnControlResult, err error) (*Session, *[]TurnControlRequest) {
	var seen []TurnControlRequest
	s := &Session{turnControl: func(_ context.Context, req TurnControlRequest) (TurnControlResult, error) {
		seen = append(seen, req)
		return result, err
	}}
	return s, &seen
}

func TestSupportsTurnControlReflectsProviderCapability(t *testing.T) {
	t.Parallel()

	if (&Session{}).SupportsTurnControl() {
		t.Fatal("session without a provider hook reports turn control supported")
	}
	var nilSession *Session
	if nilSession.SupportsTurnControl() {
		t.Fatal("nil session reports turn control supported")
	}
	supported, _ := stubControlSession(TurnControlResult{}, nil)
	if !supported.SupportsTurnControl() {
		t.Fatal("session with a provider hook reports turn control unsupported")
	}
}

func TestUnsupportedProviderReportsUnsupportedAndNeverPanics(t *testing.T) {
	t.Parallel()

	session := &Session{}
	if _, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	}); !errors.Is(err, ErrTurnControlUnsupported) {
		t.Fatalf("ControlTurn error = %v, want ErrTurnControlUnsupported", err)
	}
	if err := session.SteerTurn(context.Background(), "turn-1", "more context"); !errors.Is(err, ErrTurnControlUnsupported) {
		t.Fatalf("SteerTurn error = %v, want ErrTurnControlUnsupported", err)
	}
	if err := session.InterruptTurn(context.Background(), "turn-1"); !errors.Is(err, ErrTurnControlUnsupported) {
		t.Fatalf("InterruptTurn error = %v, want ErrTurnControlUnsupported", err)
	}
}

// A malformed request must be rejected before the provider is contacted:
// otherwise a bad call reaches the agent, and the same bad call would report
// differently depending on which backend happened to be behind the session.
func TestMalformedRequestIsRejectedBeforeProviderIO(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  TurnControlRequest
	}{
		{"steer without input", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1"}},
		{"steer without turn id", TurnControlRequest{Op: TurnControlSteer, Input: "more context"}},
		{"interrupt without turn id", TurnControlRequest{Op: TurnControlInterrupt}},
		{"interrupt carrying input", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1", Input: "nope"}},
		{"unknown operation", TurnControlRequest{Op: TurnControlOp("restart"), TurnID: "turn-1"}},
		{"empty operation", TurnControlRequest{TurnID: "turn-1"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			session, seen := stubControlSession(TurnControlResult{}, nil)
			_, err := session.ControlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlInvalid) {
				t.Fatalf("error = %v, want ErrTurnControlInvalid", err)
			}
			if len(*seen) != 0 {
				t.Fatalf("provider was contacted with %+v for a malformed request", *seen)
			}
		})
	}
}

// Validation must precede the capability check so an unsupported provider does
// not mask a caller's malformed request.
func TestMalformedRequestOutranksUnsupportedProvider(t *testing.T) {
	t.Parallel()

	_, err := (&Session{}).ControlTurn(context.Background(), TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1"})
	if !errors.Is(err, ErrTurnControlInvalid) {
		t.Fatalf("error = %v, want ErrTurnControlInvalid", err)
	}
}

func TestSteerAndInterruptBuildTheirRequests(t *testing.T) {
	t.Parallel()

	session, seen := stubControlSession(TurnControlResult{Provider: "stub"}, nil)
	if err := session.SteerTurn(context.Background(), "turn-7", "check the config too"); err != nil {
		t.Fatalf("SteerTurn: %v", err)
	}
	if err := session.InterruptTurn(context.Background(), "turn-7"); err != nil {
		t.Fatalf("InterruptTurn: %v", err)
	}

	want := []TurnControlRequest{
		{Op: TurnControlSteer, TurnID: "turn-7", Input: "check the config too"},
		{Op: TurnControlInterrupt, TurnID: "turn-7"},
	}
	if len(*seen) != len(want) {
		t.Fatalf("provider saw %d requests, want %d: %+v", len(*seen), len(want), *seen)
	}
	for i := range want {
		if (*seen)[i] != want[i] {
			t.Fatalf("request %d = %+v, want %+v", i, (*seen)[i], want[i])
		}
	}
}

func TestProviderResultAndErrorReachTheCaller(t *testing.T) {
	t.Parallel()

	want := TurnControlResult{Provider: "codex", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlInterrupt}
	session, _ := stubControlSession(want, nil)
	got, err := session.ControlTurn(context.Background(), TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"})
	if err != nil {
		t.Fatalf("ControlTurn: %v", err)
	}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}

	failing, _ := stubControlSession(TurnControlResult{}, ErrTurnControlInactive)
	if err := failing.InterruptTurn(context.Background(), "turn-1"); !errors.Is(err, ErrTurnControlInactive) {
		t.Fatalf("error = %v, want ErrTurnControlInactive", err)
	}
}

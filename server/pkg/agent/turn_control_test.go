// Canonical tests for the provider-neutral turn-control contract. Codex's
// implementation of it is covered separately in codex_control_test.go.
package agent

import (
	"context"
	"errors"
	"fmt"
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

// correlatedResult is what a well-behaved provider returns: a result whose
// terminal evidence is for the exact thread and turn that was controlled.
func correlatedResult(op TurnControlOp, threadID, turnID, status string, kind TurnTerminalKind) TurnControlResult {
	return TurnControlResult{
		Provider:  "stub",
		SessionID: threadID,
		TurnID:    turnID,
		Op:        op,
		TerminalEvidence: TerminalEvidence{
			ThreadID: threadID,
			TurnID:   turnID,
			Status:   status,
			Kind:     kind,
		},
	}
}

// assertNeutralFailureCarriesNoEvidence is the fail-closed assertion for the
// neutral surface: no failure path may hand back anything a caller could read
// as proof that a control action took effect.
func assertNeutralFailureCarriesNoEvidence(t *testing.T, result TurnControlResult, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a failure, got success with result %+v", result)
	}
	if result != (TurnControlResult{}) {
		t.Fatalf("failed ControlTurn returned a populated result: %+v", result)
	}
	if result.TerminalEvidence != (TerminalEvidence{}) {
		t.Fatalf("failed ControlTurn returned terminal evidence: %+v", result.TerminalEvidence)
	}
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
	result, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if !errors.Is(err, ErrTurnControlUnsupported) {
		t.Fatalf("ControlTurn error = %v, want ErrTurnControlUnsupported", err)
	}
	// An unsupported provider must never manufacture synthetic effect evidence.
	assertNeutralFailureCarriesNoEvidence(t, result, err)

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
			// The stub is primed to return a fully correlated success. A
			// malformed request must not be able to reach it and come back
			// holding that evidence.
			session, seen := stubControlSession(
				correlatedResult(tc.req.Op, "thread-1", "turn-1", "completed", TurnTerminalCompleted), nil)
			result, err := session.ControlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlInvalid) {
				t.Fatalf("error = %v, want ErrTurnControlInvalid", err)
			}
			assertNeutralFailureCarriesNoEvidence(t, result, err)
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

	// The stub returns correlated evidence for turn-7 so both wrappers pass the
	// neutral correlation gate and the request shapes are what is under test.
	session, seen := stubControlSession(
		correlatedResult(TurnControlSteer, "thread-1", "turn-7", "completed", TurnTerminalCompleted), nil)
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

// SteerTurn and InterruptTurn are convenience wrappers that discard the
// evidence. They are documented as NOT being the B1-proof API; this test exists
// so that contract is asserted rather than only asserted in prose.
func TestWrappersDiscardEvidenceSoControlTurnIsTheProofAPI(t *testing.T) {
	t.Parallel()

	want := correlatedResult(TurnControlSteer, "thread-1", "turn-1", "completed", TurnTerminalCompleted)
	session, _ := stubControlSession(want, nil)

	// The wrapper reports only success or failure...
	if err := session.SteerTurn(context.Background(), "turn-1", "more context"); err != nil {
		t.Fatalf("SteerTurn: %v", err)
	}

	// ...while ControlTurn hands back the evidence that proves the effect.
	got, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "more context",
	})
	if err != nil {
		t.Fatalf("ControlTurn: %v", err)
	}
	if got.TerminalEvidence != want.TerminalEvidence {
		t.Fatalf("evidence = %+v, want %+v", got.TerminalEvidence, want.TerminalEvidence)
	}
}

func TestProviderResultAndErrorReachTheCaller(t *testing.T) {
	t.Parallel()

	want := correlatedResult(TurnControlInterrupt, "thread-1", "turn-1", "cancelled", TurnTerminalAborted)
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

	// A provider's typed uncorrelated-evidence failure must remain
	// distinguishable after crossing the neutral surface.
	uncorrelated, _ := stubControlSession(TurnControlResult{},
		fmt.Errorf("%w: nothing arrived", ErrTurnControlUncorrelated))
	result, err := uncorrelated.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if !errors.Is(err, ErrTurnControlUncorrelated) {
		t.Fatalf("error = %v, want ErrTurnControlUncorrelated", err)
	}
	assertNeutralFailureCarriesNoEvidence(t, result, err)
}

// The neutral surface must not soften a provider's "I could not prove this
// took effect" into a success, nor rewrite it into some other failure.
func TestNeutralControlContractRejectsSuccessfulAckWithoutTerminalEvidence(t *testing.T) {
	t.Parallel()

	// A backend that got its ACK and then could not correlate a terminal.
	provider := fmt.Errorf("%w: acknowledged but no terminal event for turn-1", ErrTurnControlUncorrelated)

	for _, op := range []TurnControlOp{TurnControlSteer, TurnControlInterrupt} {
		req := TurnControlRequest{Op: op, TurnID: "turn-1"}
		if op == TurnControlSteer {
			req.Input = "more context"
		}
		session, _ := stubControlSession(TurnControlResult{}, provider)
		result, err := session.ControlTurn(context.Background(), req)
		if !errors.Is(err, ErrTurnControlUncorrelated) {
			t.Fatalf("%s: error = %v, want ErrTurnControlUncorrelated", op, err)
		}
		assertNeutralFailureCarriesNoEvidence(t, result, err)
	}
}

// No backend — however it is wired — may manufacture a successful result whose
// evidence does not correlate to the turn the caller asked about. The check is
// centralised in Session.ControlTurn precisely so this holds for providers that
// have not been written yet.
func TestNeutralControlResultRequiresTargetCorrelation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		result TurnControlResult
	}{
		{
			"no evidence at all",
			TurnControlResult{Provider: "stub", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlInterrupt},
		},
		{
			"evidence for a different turn",
			TurnControlResult{
				Provider: "stub", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlInterrupt,
				TerminalEvidence: TerminalEvidence{
					ThreadID: "thread-1", TurnID: "turn-other", Status: "cancelled", Kind: TurnTerminalAborted,
				},
			},
		},
		{
			"evidence for a different thread",
			TurnControlResult{
				Provider: "stub", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlInterrupt,
				TerminalEvidence: TerminalEvidence{
					ThreadID: "thread-other", TurnID: "turn-1", Status: "cancelled", Kind: TurnTerminalAborted,
				},
			},
		},
		{
			"evidence with empty identifiers",
			TurnControlResult{
				Provider: "stub", SessionID: "", TurnID: "turn-1", Op: TurnControlInterrupt,
				TerminalEvidence: TerminalEvidence{Status: "cancelled", Kind: TurnTerminalAborted},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := stubControlSession(tc.result, nil)
			result, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlInterrupt, TurnID: "turn-1",
			})
			if !errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("error = %v, want ErrTurnControlUncorrelated — a backend manufactured an uncorrelated success", err)
			}
			assertNeutralFailureCarriesNoEvidence(t, result, err)
		})
	}
}

// The correlation predicate itself, pinned directly: empty identifiers must
// never read as a match just because the target is also empty.
func TestTerminalEvidenceCorrelates(t *testing.T) {
	t.Parallel()

	full := TerminalEvidence{ThreadID: "thread-1", TurnID: "turn-1", Status: "completed", Kind: TurnTerminalCompleted}
	if !full.correlates("thread-1", "turn-1") {
		t.Fatal("matching evidence did not correlate")
	}
	if full.correlates("thread-1", "turn-2") {
		t.Fatal("evidence correlated against the wrong turn")
	}
	if full.correlates("thread-2", "turn-1") {
		t.Fatal("evidence correlated against the wrong thread")
	}
	if (TerminalEvidence{}).correlates("", "") {
		t.Fatal("empty evidence correlated against an empty target")
	}
	if (TerminalEvidence{TurnID: "turn-1"}).correlates("", "turn-1") {
		t.Fatal("evidence with no thread id correlated")
	}
	if (TerminalEvidence{ThreadID: "thread-1"}).correlates("thread-1", "") {
		t.Fatal("evidence with no turn id correlated")
	}
}

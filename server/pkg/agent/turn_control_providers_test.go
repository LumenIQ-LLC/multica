package agent

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Cross-provider conformance suite.
//
// Every case here drives BOTH backends through the neutral TurnController
// surface and asserts the SAME outcome. That is the whole claim of "one
// abstraction over many providers": a caller cannot tell which backend it
// steered except by asking SupportsTurnControl.
//
// Each provider supplies a harness that can (a) put a turn in flight, (b) make
// the provider answer a control request the way the real one does, and (c)
// script a terminal. The wire shapes underneath are deliberately different —
// Codex speaks JSON-RPC turn/steer + turn/interrupt, Claude speaks a
// control_request frame and a bare user frame — and the assertions never
// mention either.

// controlHarness is one provider wired up for the conformance cases.
type controlHarness struct {
	name string
	// session returns a Session exposing the provider's turn control, plus the
	// turn id that is live on it.
	//
	// terminal decides what the fake provider does when a control request
	// arrives: "completed", "aborted", "failed", or "" for "stay running".
	session func(t *testing.T, terminal string) (*Session, string)
	// unsupported returns a session from the same provider family that does NOT
	// implement turn control.
	unsupported func(t *testing.T) *Session
}

func codexHarness() controlHarness {
	return controlHarness{
		name: "codex",
		session: func(t *testing.T, terminal string) (*Session, string) {
			t.Helper()
			var respond codexControlResponder
			switch terminal {
			case "":
				respond = ackOnly()
			case "completed":
				respond = ackThenTerminal(codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "completed"})
			case "aborted":
				respond = ackThenTerminal(codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "cancelled"})
			case "failed":
				respond = ackThenTerminal(codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "failed"})
			default:
				t.Fatalf("unknown terminal %q", terminal)
			}
			c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", respond)
			return &Session{turnControl: c.controlTurn}, "turn-1"
		},
		unsupported: func(t *testing.T) *Session { return &Session{} },
	}
}

func claudeHarness() controlHarness {
	return controlHarness{
		name: "claude",
		session: func(t *testing.T, terminal string) (*Session, string) {
			t.Helper()
			s, _ := newScriptedClaudeControl(t, "session-1", terminal)
			return &Session{turnControl: s.controlTurn}, "session-1"
		},
		unsupported: func(t *testing.T) *Session { return &Session{} },
	}
}

func allControlHarnesses() []controlHarness {
	return []controlHarness{codexHarness(), claudeHarness()}
}

// Interrupt claims the turn ENDED, so it resolves only on correlated terminal
// evidence — identically for both providers.
func TestProvidersInterruptReportsCorrelatedTerminal(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session, turnID := h.session(t, "aborted")

			if !session.SupportsTurnControl() {
				t.Fatal("provider reports turn control unsupported")
			}
			result, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlInterrupt, TurnID: turnID,
			})
			if err != nil {
				t.Fatalf("ControlTurn: %v", err)
			}
			if result.TerminalEvidence.Kind != TurnTerminalAborted {
				t.Fatalf("kind = %q, want %q", result.TerminalEvidence.Kind, TurnTerminalAborted)
			}
			if result.TerminalEvidence.TurnID != turnID {
				t.Fatalf("evidence turn = %q, want %q", result.TerminalEvidence.TurnID, turnID)
			}
			if result.Op != TurnControlInterrupt {
				t.Fatalf("op = %q", result.Op)
			}
		})
	}
}

// A bare acknowledgement with no terminal is NOT success, on either provider.
// This is the B1 guarantee, and it is the case most likely to regress when a
// new backend is added.
func TestProvidersAckWithoutTerminalIsNotSuccess(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session, turnID := h.session(t, "") // acknowledges, never terminates

			result, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlInterrupt, TurnID: turnID,
			})
			if err == nil {
				t.Fatalf("acknowledgement alone reported success: %+v", result)
			}
			if !errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("error = %v, want ErrTurnControlUncorrelated", err)
			}
			if result != (TurnControlResult{}) {
				t.Fatalf("failed control returned a populated result: %+v", result)
			}
			if result.TerminalEvidence != (TerminalEvidence{}) {
				t.Fatalf("failed control returned terminal evidence: %+v", result.TerminalEvidence)
			}
		})
	}
}

// A turn that FAILS is a correlated verdict about our turn, not a timeout, and
// both providers must say so with the same typed error.
func TestProvidersFailedTurnIsCorrelatedFailure(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session, turnID := h.session(t, "failed")

			result, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlInterrupt, TurnID: turnID,
			})
			if !errors.Is(err, ErrTurnControlTurnFailed) {
				t.Fatalf("error = %v, want ErrTurnControlTurnFailed", err)
			}
			if errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("a reported failure was recorded as uncorrelated: %v", err)
			}
			if result != (TurnControlResult{}) {
				t.Fatalf("failed control returned a populated result: %+v", result)
			}
		})
	}
}

// Steer claims only that the input entered the LIVE turn, so a turn that keeps
// running is the success case on both providers.
func TestProvidersSteerOnLiveTurnSucceeds(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session, turnID := h.session(t, "") // stays running

			result, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlSteer, TurnID: turnID, Input: "also check the config",
			})
			if err != nil {
				t.Fatalf("steer on a live turn failed: %v", err)
			}
			if result.TerminalEvidence.Kind != TurnStillRunning {
				t.Fatalf("kind = %q, want %q", result.TerminalEvidence.Kind, TurnStillRunning)
			}
		})
	}
}

// A turn that dies ON the steer did not accept it. Both providers report the
// terminal rather than the still-running success.
func TestProvidersSteerThatKillsTurnIsNotSuccess(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session, turnID := h.session(t, "failed")

			_, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlSteer, TurnID: turnID, Input: "more",
			})
			if !errors.Is(err, ErrTurnControlTurnFailed) {
				t.Fatalf("error = %v, want ErrTurnControlTurnFailed", err)
			}
		})
	}
}

// A turn id the provider has moved past is refused identically, with no wire
// traffic at all.
func TestProvidersStaleTurnIDIsRefused(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session, _ := h.session(t, "")

			_, err := session.ControlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlInterrupt, TurnID: "not-the-live-turn",
			})
			if !errors.Is(err, ErrTurnControlInactive) {
				t.Fatalf("error = %v, want ErrTurnControlInactive", err)
			}
		})
	}
}

// The neutral validation layer behaves the same in front of every provider.
func TestProvidersRejectMalformedRequestsIdentically(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  TurnControlRequest
	}{
		{"steer without input", TurnControlRequest{Op: TurnControlSteer, TurnID: "x"}},
		{"interrupt carrying input", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "x", Input: "no"}},
		{"missing turn id", TurnControlRequest{Op: TurnControlInterrupt}},
		{"unknown op", TurnControlRequest{Op: TurnControlOp("restart"), TurnID: "x"}},
	}

	for _, h := range allControlHarnesses() {
		for _, tc := range cases {
			t.Run(h.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				session, _ := h.session(t, "")
				if _, err := session.ControlTurn(context.Background(), tc.req); !errors.Is(err, ErrTurnControlInvalid) {
					t.Fatalf("error = %v, want ErrTurnControlInvalid", err)
				}
			})
		}
	}
}

// A backend without the capability answers the same way for every provider
// family, and never panics.
func TestProvidersUnsupportedIsUniform(t *testing.T) {
	t.Parallel()

	for _, h := range allControlHarnesses() {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()
			session := h.unsupported(t)
			if session.SupportsTurnControl() {
				t.Fatal("unsupported session claims support")
			}
			if err := session.InterruptTurn(context.Background(), "t"); !errors.Is(err, ErrTurnControlUnsupported) {
				t.Fatalf("error = %v, want ErrTurnControlUnsupported", err)
			}
			if err := session.SteerTurn(context.Background(), "t", "x"); !errors.Is(err, ErrTurnControlUnsupported) {
				t.Fatalf("error = %v, want ErrTurnControlUnsupported", err)
			}
		})
	}
}

// Process death is never credited as a control action working, on any provider.
func TestProvidersProcessExitIsNotEvidence(t *testing.T) {
	t.Parallel()

	t.Run("codex", func(t *testing.T) {
		t.Parallel()
		c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", func(c *codexClient, id int, _ string) {
			c.handleLine(ackLine(id))
			c.markProcessExited(errCodexProcessExited)
		})
		session := &Session{turnControl: c.controlTurn}
		_, err := session.ControlTurn(context.Background(), TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"})
		if !errors.Is(err, ErrTurnControlUncorrelated) {
			t.Fatalf("error = %v, want ErrTurnControlUncorrelated", err)
		}
	})

	t.Run("claude", func(t *testing.T) {
		t.Parallel()
		s, fs := newScriptedClaudeControl(t, "session-1", "")
		fs.afterWrite = func() { s.markProcessExited(errAgentProcessExited) }
		session := &Session{turnControl: s.controlTurn}
		_, err := session.ControlTurn(context.Background(), TurnControlRequest{Op: TurnControlInterrupt, TurnID: "session-1"})
		if err == nil {
			t.Fatal("process exit was credited as a working interrupt")
		}
	})
}

// ── claude harness ──

// scriptedClaudeStdin plays the provider side: it answers an interrupt
// control_request the way a live CLI does, then optionally emits a terminal.
type scriptedClaudeStdin struct {
	state      *claudeControlState
	terminal   string
	afterWrite func()
	lastSteer  string
}

func (w *scriptedClaudeStdin) Write(p []byte) (int, error) {
	frame := string(p)
	// Answer an interrupt control_request with a correlated success ack, exactly
	// as claude 2.1.247 does (observed: {"subtype":"success","request_id":…}).
	if strings.Contains(frame, `"control_request"`) {
		id := extractJSONStringField(frame, "request_id")
		go func() {
			w.state.handleControlResponse([]byte(
				`{"type":"control_response","response":{"subtype":"success","request_id":"` + id + `","response":{"still_queued":[]}}}`))
			w.emitTerminal()
		}()
	} else if strings.Contains(frame, `"user"`) {
		w.lastSteer = frame
		// A steer is a bare user frame; the CLI sends no acknowledgement.
		go w.emitTerminal()
	}
	if w.afterWrite != nil {
		w.afterWrite()
	}
	return len(p), nil
}

func (w *scriptedClaudeStdin) emitTerminal() {
	switch w.terminal {
	case "completed":
		w.state.publishTerminal("completed", false)
	case "aborted":
		// A real interrupt reports is_error=true with terminal_reason
		// aborted_streaming. Scripting the true shape is the point.
		w.state.publishTerminal("aborted_streaming", true)
	case "failed":
		w.state.setFailureDetail("claude turn failed")
		w.state.publishTerminal("error_during_execution", true)
	default:
		// stays running
	}
}

// extractJSONStringField pulls one string field out of a JSON frame without a
// full decode. Test-only.
func extractJSONStringField(frame, field string) string {
	key := `"` + field + `":"`
	i := strings.Index(frame, key)
	if i < 0 {
		return ""
	}
	rest := frame[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func newScriptedClaudeControl(t *testing.T, sessionID, terminal string) (*claudeControlState, *scriptedClaudeStdin) {
	t.Helper()
	s := newClaudeControlState(slog.Default())
	s.evidenceTimeout = 200 * time.Millisecond
	w := &scriptedClaudeStdin{state: s, terminal: terminal}
	s.attachStdin(w)
	s.beginTurn(sessionID)
	if s.controlLiveTurnID() != sessionID {
		t.Fatalf("beginTurn did not arm control: %q", s.controlLiveTurnID())
	}
	return s, w
}

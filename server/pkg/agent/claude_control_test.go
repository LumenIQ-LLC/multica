package agent

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Frame shapes and ordering below were captured from a LIVE claude 2.1.247 in
// --input-format stream-json mode, driving a long-running tool call and then
// interrupting it. Abridged from the probe transcript:
//
//	--> {"type":"control_request","request_id":"probe-int-1","request":{"subtype":"interrupt"}}
//	<-- {"type":"control_response","response":{"subtype":"success","request_id":"probe-int-1",
//	                                          "response":{"still_queued":[]}}}          (+7ms)
//	<-- {"type":"user","message":{...,"content":[{"type":"text",
//	                              "text":"[Request interrupted by user]"}]}}            (+8ms)
//	<-- {"type":"result","subtype":"error_during_execution","is_error":true,
//	     "terminal_reason":"aborted_streaming","num_turns":2}                           (+10ms)
//
// Two things in that transcript drive the tests here:
//   1. a SUCCESSFUL interrupt reports is_error=true, so is_error cannot decide
//      the classification on its own;
//   2. a `user` frame arrives BEFORE the result, so anything that treated it as
//      terminal would preempt the real verdict — the Codex #8 bug, on a new
//      path.

// A successful interrupt must classify as ABORTED even though the provider
// flags is_error=true. Mutation check: make claudeTerminalKind consult isError
// before terminalReason and this fails with kind "failed".
func TestClaudeInterruptClassifiesAbortedDespiteIsError(t *testing.T) {
	t.Parallel()

	s, _ := newScriptedClaudeControl(t, "session-1", "aborted")
	session := &Session{turnControl: s.controlTurn}

	result, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "session-1",
	})
	if err != nil {
		t.Fatalf("interrupt of a live turn failed: %v", err)
	}
	if got := result.TerminalEvidence.Kind; got != TurnTerminalAborted {
		t.Fatalf("kind = %q, want %q — a successful interrupt reports is_error=true and must not read as failure", got, TurnTerminalAborted)
	}
	if got := result.TerminalEvidence.Status; got != "aborted_streaming" {
		t.Fatalf("status = %q, want the provider's raw terminal_reason", got)
	}
}

// The `user "[Request interrupted by user]"` frame is turn CONTENT. Only the
// result frame is a verdict. This is the Claude-path guard against the bug
// fixed for Codex in #8.
func TestClaudeUserFrameIsNotTerminalEvidence(t *testing.T) {
	t.Parallel()

	s := newClaudeControlState(slog.Default())
	s.evidenceTimeout = 150 * time.Millisecond
	s.beginTurn("session-1")

	evidence, unsubscribe := s.evidence.subscribe()
	defer unsubscribe()

	// Everything a real interrupt emits BEFORE the result — an ack and the
	// interrupted-user frame — must leave the waiter unresolved.
	s.handleControlResponse([]byte(
		`{"type":"control_response","response":{"subtype":"success","request_id":"x","response":{"still_queued":[]}}}`))

	_, err := awaitTerminalEvidence(context.Background(), s, evidence, "session-1", "session-1", 120*time.Millisecond)
	if !errors.Is(err, ErrTurnControlUncorrelated) {
		t.Fatalf("error = %v, want ErrTurnControlUncorrelated — nothing before the result frame is a verdict", err)
	}
	// The turn is still live precisely because nothing terminal has happened.
	if s.controlLiveTurnID() != "session-1" {
		t.Fatalf("live turn retired by a non-terminal frame: %q", s.controlLiveTurnID())
	}

	// Now the real terminal arrives and IS a verdict.
	evidence2, unsubscribe2 := s.evidence.subscribe()
	defer unsubscribe2()
	s.publishTerminal("aborted_streaming", true)
	ev, err := awaitTerminalEvidence(context.Background(), s, evidence2, "session-1", "session-1", time.Second)
	if err != nil {
		t.Fatalf("result frame did not resolve the wait: %v", err)
	}
	if ev.Kind != TurnTerminalAborted {
		t.Fatalf("kind = %q, want aborted", ev.Kind)
	}
	if s.controlLiveTurnID() != "" {
		t.Fatalf("terminal did not retire the live turn: %q", s.controlLiveTurnID())
	}
}

// Fail closed: only a reason we affirmatively recognize as success is a
// completed turn.
func TestClaudeTerminalKindFailsClosed(t *testing.T) {
	t.Parallel()

	aborted := []string{"aborted_streaming", "aborted", "interrupted", "cancelled", "canceled"}
	for _, r := range aborted {
		if got := claudeTerminalKind(r, true); got != TurnTerminalAborted {
			t.Errorf("claudeTerminalKind(%q, true) = %q, want aborted", r, got)
		}
	}
	failed := []string{"error_during_execution", "error_max_turns", "timeout", "", "unknown_future_reason"}
	for _, r := range failed {
		if got := claudeTerminalKind(r, false); got != TurnTerminalFailed {
			t.Errorf("claudeTerminalKind(%q, false) = %q, want failed (fail closed)", r, got)
		}
	}
	if got := claudeTerminalKind("completed", false); got != TurnTerminalCompleted {
		t.Errorf(`claudeTerminalKind("completed", false) = %q, want completed`, got)
	}
	// "completed" with an error flag is not proof a control action worked.
	if got := claudeTerminalKind("completed", true); got != TurnTerminalFailed {
		t.Errorf(`claudeTerminalKind("completed", true) = %q, want failed`, got)
	}
}

// Steer has NO acknowledgement on this provider — the CLI does not answer a
// user frame. Success therefore rests entirely on the turn being verifiably
// live afterwards.
func TestClaudeSteerSucceedsWithoutAnyAck(t *testing.T) {
	t.Parallel()

	s, w := newScriptedClaudeControl(t, "session-1", "") // never terminates, never acks
	session := &Session{turnControl: s.controlTurn}

	result, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "session-1", Input: "also check the config",
	})
	if err != nil {
		t.Fatalf("steer failed: %v", err)
	}
	if result.TerminalEvidence.Kind != TurnStillRunning {
		t.Fatalf("kind = %q, want %q", result.TerminalEvidence.Kind, TurnStillRunning)
	}
	if w.lastSteer == "" {
		t.Fatal("no user frame was written for the steer")
	}
	if !strings.Contains(w.lastSteer, "also check the config") {
		t.Fatalf("steer frame did not carry the input: %s", w.lastSteer)
	}
}

// A steer whose process dies right after the write is NOT success, even though
// nothing ever refused it.
func TestClaudeSteerFailsClosedWhenProcessDies(t *testing.T) {
	t.Parallel()

	s, w := newScriptedClaudeControl(t, "session-1", "")
	w.afterWrite = func() { s.markProcessExited(errAgentProcessExited) }
	session := &Session{turnControl: s.controlTurn}

	_, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "session-1", Input: "more",
	})
	if err == nil {
		t.Fatal("a steer followed by process death reported success")
	}
}

// An interrupt the provider REFUSES must surface, not be swallowed.
func TestClaudeInterruptSurfacesProviderRefusal(t *testing.T) {
	t.Parallel()

	s := newClaudeControlState(slog.Default())
	s.evidenceTimeout = 200 * time.Millisecond
	s.beginTurn("session-1")
	s.attachStdin(&refusingClaudeStdin{state: s})

	session := &Session{turnControl: s.controlTurn}
	_, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "session-1",
	})
	if !errors.Is(err, ErrTurnControlRejected) {
		t.Fatalf("error = %v, want ErrTurnControlRejected", err)
	}
}

type refusingClaudeStdin struct{ state *claudeControlState }

func (w *refusingClaudeStdin) Write(p []byte) (int, error) {
	id := extractJSONStringField(string(p), "request_id")
	go w.state.handleControlResponse([]byte(
		`{"type":"control_response","response":{"subtype":"error","request_id":"` + id + `","error":"no active turn"}}`))
	return len(p), nil
}

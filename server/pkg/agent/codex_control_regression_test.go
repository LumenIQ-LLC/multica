package agent

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

// Regression tests for defects found reviewing the turn-control contract. Each
// one failed before its fix and is kept because none of them is visible from
// reading the happy path: two are scheduling races, and two are classification
// rules whose wrong answer still returns a well-formed result.

// A correlated terminal event that is ALREADY queued must win over a
// simultaneously-ready process exit.
//
// This is not a hypothetical ordering. On a successful interrupt the stdout
// reader publishes the evidence and then calls onTurnDone, which drives the
// lifecycle goroutine into markProcessExited and closes processDone — so by the
// time the waiter runs, both cases are ready. Go's select picks uniformly at
// random among ready cases, so without an explicit drain a genuinely successful
// interrupt reported ErrTurnControlUncorrelated about half the time. The loop
// count is what makes a 50/50 race a deterministic failure.
func TestCodexControlPrefersReadyEvidenceOverProcessExit(t *testing.T) {
	t.Parallel()

	const runs = 500
	for i := 0; i < runs; i++ {
		c := &codexClient{
			cfg:                    Config{Logger: slog.Default()},
			processDone:            make(chan struct{}),
			controlEvidenceTimeout: 2 * time.Second,
		}
		ch := make(chan TerminalEvidence, 1)
		ch <- TerminalEvidence{
			ThreadID: "thread-1", TurnID: "turn-1",
			Status: "cancelled", Kind: TurnTerminalAborted,
		}
		c.markProcessExited(errCodexProcessExited)

		ev, err := c.awaitTerminalEvidence(context.Background(), ch, "thread-1", "turn-1", c.evidenceTimeout())
		if err != nil {
			t.Fatalf("run %d: ready correlated evidence was discarded in favour of process exit: %v", i, err)
		}
		if ev.TurnID != "turn-1" {
			t.Fatalf("run %d: evidence = %+v, want turn-1", i, ev)
		}
	}
}

// The same guard for the other two competing cases. A context that is already
// cancelled, or an expired timer, must not beat evidence that is sitting in the
// queue — the answer exists, so reporting "could not correlate" is wrong.
func TestCodexControlPrefersReadyEvidenceOverCancellationAndTimeout(t *testing.T) {
	t.Parallel()

	queued := func() chan TerminalEvidence {
		ch := make(chan TerminalEvidence, 1)
		ch <- TerminalEvidence{
			ThreadID: "thread-1", TurnID: "turn-1",
			Status: "completed", Kind: TurnTerminalCompleted,
		}
		return ch
	}

	t.Run("cancelled context", func(t *testing.T) {
		t.Parallel()
		c := &codexClient{cfg: Config{Logger: slog.Default()}, processDone: make(chan struct{}), controlEvidenceTimeout: time.Minute}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for i := 0; i < 200; i++ {
			if _, err := c.awaitTerminalEvidence(ctx, queued(), "thread-1", "turn-1", c.evidenceTimeout()); err != nil {
				t.Fatalf("run %d: ready evidence lost to a cancelled context: %v", i, err)
			}
		}
	})

	t.Run("expired timeout", func(t *testing.T) {
		t.Parallel()
		c := &codexClient{cfg: Config{Logger: slog.Default()}, processDone: make(chan struct{}), controlEvidenceTimeout: time.Nanosecond}
		for i := 0; i < 200; i++ {
			ch := queued()
			time.Sleep(time.Microsecond) // let the timer fire first
			if _, err := c.awaitTerminalEvidence(context.Background(), ch, "thread-1", "turn-1", c.evidenceTimeout()); err != nil {
				t.Fatalf("run %d: ready evidence lost to an expired timer: %v", i, err)
			}
		}
	})
}

// Once a turn reaches a terminal state it is no longer a legal control target,
// so the control pointer must be retired. Otherwise the live-turn guard passes
// for a finished turn and we send it an interrupt — then wait out the full
// evidence timeout for an event that already fired.
func TestCodexControlRetiresTurnPointerOnTermination(t *testing.T) {
	t.Parallel()

	fs := &fakeStdin{}
	c := &codexClient{
		cfg:                    Config{Logger: slog.Default()},
		stdin:                  fs,
		pending:                make(map[int]*pendingRPC),
		processDone:            make(chan struct{}),
		notificationProtocol:   "unknown",
		onTurnDone:             func(bool) {},
		controlEvidenceTimeout: 150 * time.Millisecond,
	}
	c.setThreadID("thread-1")
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-1"}}}`)
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}}`)

	if got := c.controlTurnID.get(); got != "" {
		t.Fatalf("control mirror = %q after turn/completed, want empty", got)
	}
	// c.turnID is deliberately NOT cleared: it still attributes diagnostics to
	// the turn they came from.
	if c.turnID != "turn-1" {
		t.Fatalf("turnID = %q, want turn-1 — diagnostics attribution must survive", c.turnID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.controlTurn(ctx, TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"})
	if !errors.Is(err, ErrTurnControlInactive) {
		t.Fatalf("error = %v, want ErrTurnControlInactive", err)
	}
	if n := len(fs.Lines()); n != 0 {
		t.Fatalf("wrote %d frame(s) to interrupt a finished turn, want 0", n)
	}
}

// An unrecognized terminal status must not read as a completed turn. A spurious
// "failed" costs a caller a retry; a spurious "completed" tells it the steer or
// interrupt took effect when nothing proves that.
func TestCodexTerminalKindFailsClosedOnUnknownStatus(t *testing.T) {
	t.Parallel()

	for _, status := range []string{"error", "timeout", "incomplete", "", "rejected", "failed"} {
		if got := codexTerminalKind(status, false); got != TurnTerminalFailed {
			t.Errorf("codexTerminalKind(%q) = %s, want %s", status, got, TurnTerminalFailed)
		}
	}
	if got := codexTerminalKind("completed", false); got != TurnTerminalCompleted {
		t.Errorf(`codexTerminalKind("completed") = %s, want %s`, got, TurnTerminalCompleted)
	}
	// aborted still outranks the status string.
	if got := codexTerminalKind("cancelled", true); got != TurnTerminalAborted {
		t.Errorf(`codexTerminalKind("cancelled", true) = %s, want %s`, got, TurnTerminalAborted)
	}
}

// The control RPC is bounded independently of the evidence wait. turn/steer and
// turn/interrupt are not handshake RPCs, so before this they inherited only the
// caller's context — and a caller passing context.Background() to an app-server
// that accepted the frame and never answered blocked forever, never reaching
// the evidence timeout that was supposed to bound it.
func TestCodexControlRPCIsBounded(t *testing.T) {
	t.Parallel()

	if !isCodexHandshakeRPC("turn/start") {
		t.Fatal("precondition: turn/start should be a handshake RPC")
	}
	for _, m := range []string{"turn/steer", "turn/interrupt"} {
		if isCodexHandshakeRPC(m) {
			t.Fatalf("%s is not a handshake RPC; it must be bounded by codexControlRequestTimeout instead", m)
		}
	}
	if codexControlRequestTimeout <= 0 {
		t.Fatalf("codexControlRequestTimeout = %s, want a positive bound", codexControlRequestTimeout)
	}
}

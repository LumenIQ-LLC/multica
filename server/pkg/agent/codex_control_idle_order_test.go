package agent

import (
	"context"
	"testing"
	"time"
)

// realIdleThenCompleted is the notification order a live codex-cli 0.150.0
// app-server emits for an ordinary successful turn, captured by driving a real
// `codex app-server --listen stdio://` and recording the raw frames:
//
//	thread/status/changed -> active
//	turn/started
//	thread/status/changed -> idle        emittedAtMs 1787792256479  <- FIRST
//	turn/completed status="completed"    emittedAtMs 1787792256479  <- SECOND
//
// The two terminal frames land in the same millisecond, and idle is written
// first. This matters because the client resolves a control call on the FIRST
// correlated terminal evidence it sees, so anything published on idle preempts
// the turn's real verdict.
const realIdleNotification = `{"jsonrpc":"2.0","method":"thread/status/changed","params":{"threadId":"thread-1","status":{"type":"idle"}}}`

// An interrupt of a turn that completed successfully must report the turn's
// real terminal, not the idle that preceded it.
//
// Regression: idle used to publish control evidence with status "idle", which
// codexTerminalKind classifies as a failed turn (correctly — idle is not a
// success status). Because idle arrives first, that verdict reached the waiter
// before turn/completed ever published, so EVERY successful turn's interrupt
// returned ErrTurnControlTurnFailed. The scripted acceptance tests never caught
// it because they emit turn/completed without a preceding idle.
func TestCodexControlIdleDoesNotPreemptCompleted(t *testing.T) {
	t.Parallel()

	c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackThenTerminal(
		codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "completed"},
		realIdleNotification,
	))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.controlTurn(ctx, TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if err != nil {
		t.Fatalf("interrupt of a successfully completed turn returned an error: %v", err)
	}
	if got := result.TerminalEvidence.Status; got != "completed" {
		t.Fatalf("evidence status = %q, want completed — idle preempted the real terminal", got)
	}
	if got := result.TerminalEvidence.Kind; got != TurnTerminalCompleted {
		t.Fatalf("evidence kind = %q, want %q", got, TurnTerminalCompleted)
	}
}

// The same ordering must not corrupt a steer. Steer resolves on the first of a
// correlated terminal or its acceptance window; an idle-sourced terminal would
// turn a healthy steer into a reported failure.
func TestCodexSteerSurvivesIdleBeforeCompleted(t *testing.T) {
	t.Parallel()

	c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackThenTerminal(
		codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "completed"},
		realIdleNotification,
	))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.controlTurn(ctx, TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "also check the config",
	})
	if err != nil {
		t.Fatalf("steer returned an error on the real idle-then-completed ordering: %v", err)
	}
	if got := result.TerminalEvidence.Status; got != "completed" {
		t.Fatalf("evidence status = %q, want completed", got)
	}
}

// An idle with no turn/completed behind it must NOT be manufactured into a
// terminal verdict. It resolves the only way an unproven outcome honestly can:
// no correlated evidence, so the call fails closed rather than claiming the
// interrupt worked.
func TestCodexControlIdleAloneIsNotTerminalEvidence(t *testing.T) {
	t.Parallel()

	fs := &fakeStdinWithHook{}
	var c *codexClient
	fs.afterWrite = func() {
		lines := fs.Lines()
		if len(lines) == 0 {
			return
		}
		c.handleLine(`{"jsonrpc":"2.0","id":1,"result":{}}`)
		c.handleLine(realIdleNotification)
	}
	c = &codexClient{
		cfg:                    Config{Logger: nil},
		stdin:                  fs,
		pending:                make(map[int]*pendingRPC),
		processDone:            make(chan struct{}),
		notificationProtocol:   "unknown",
		onTurnDone:             func(bool) {},
		controlEvidenceTimeout: 200 * time.Millisecond,
	}
	c.setThreadID("thread-1")
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-1"}}}`)

	_, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if err == nil {
		t.Fatal("idle alone was accepted as proof the interrupt took effect")
	}
}

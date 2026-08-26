package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

// testControlEvidenceTimeout bounds the fail-closed paths. It is the PRODUCTION
// timeout mechanism with a small value — not a sleep — so a test that asserts
// "no terminal evidence ever arrives" resolves in milliseconds instead of
// waiting out codexControlEvidenceTimeout.
const testControlEvidenceTimeout = 200 * time.Millisecond

// codexTerminal is one scripted terminal `turn/completed` notification. Every
// field is explicit: the whole point of the negative tests is to emit a
// terminal for the WRONG thread or turn, so nothing here may be inferred from
// the client's live ids.
type codexTerminal struct {
	threadID string
	turnID   string
	status   string
}

func (e codexTerminal) line() string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":%q}}}`,
		e.threadID, e.turnID, e.status,
	)
}

// codexControlResponder stands in for the app-server. It is handed the client,
// the JSON-RPC id of the control frame just written, and that frame's method,
// and is responsible for feeding back whatever the app-server would have sent.
type codexControlResponder func(c *codexClient, id int, method string)

func ackLine(id int) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{}}`, id)
}

// ackOnly acknowledges the RPC and then says nothing more. This is the shape of
// the pre-mc-S2726 fixture, and it is exactly what must NOT read as success:
// an acknowledgement proves the request was accepted, not that the turn was
// steered or cancelled.
func ackOnly() codexControlResponder {
	return func(c *codexClient, id int, _ string) {
		c.handleLine(ackLine(id))
	}
}

// ackThenTerminal acknowledges and then emits one terminal event verbatim.
func ackThenTerminal(terminal codexTerminal, notifications ...string) codexControlResponder {
	return func(c *codexClient, id int, _ string) {
		c.handleLine(ackLine(id))
		for _, n := range notifications {
			c.handleLine(n)
		}
		c.handleLine(terminal.line())
	}
}

// ackThenMatchingTerminal answers the way a healthy app-server does: it
// acknowledges, then terminates the targeted turn with the status appropriate
// to the operation — completed for a steer (the turn kept going and finished),
// cancelled for an interrupt.
func ackThenMatchingTerminal(threadID, turnID string) codexControlResponder {
	return func(c *codexClient, id int, method string) {
		status := "completed"
		if method == "turn/interrupt" {
			status = "cancelled"
		}
		ackThenTerminal(codexTerminal{threadID: threadID, turnID: turnID, status: status})(c, id, method)
	}
}

// rejectWith answers the control RPC with a JSON-RPC error, the app-server
// refusing the request outright.
func rejectWith(code int, message string) codexControlResponder {
	return func(c *codexClient, id int, _ string) {
		c.handleLine(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":%d,"message":%q}}`, id, code, message))
	}
}

// newScriptedTurnCodexClient returns a client with a live thread and turn — the
// turn established through the real turn/started path, so the test exercises
// the same wiring production does — whose fake app-server answers control RPCs
// with the supplied responder.
//
// The responder is deliberately a parameter with no default that auto-completes
// the target turn. If every client terminated its own turn, an implementation
// that treated the acknowledgement as success would pass every test here, which
// is the exact bug this file exists to catch.
func newScriptedTurnCodexClient(
	t *testing.T,
	threadID, turnID string,
	respond codexControlResponder,
) (*codexClient, *fakeStdinWithHook) {
	t.Helper()

	var c *codexClient
	fs := &fakeStdinWithHook{}
	fs.afterWrite = func() {
		lines := fs.Lines()
		if len(lines) == 0 {
			return
		}
		var frame struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &frame); err != nil {
			t.Errorf("decode written frame %q: %v", lines[len(lines)-1], err)
			return
		}
		if respond != nil {
			respond(c, frame.ID, frame.Method)
		}
	}
	c = &codexClient{
		cfg:                    Config{Logger: slog.Default()},
		stdin:                  fs,
		pending:                make(map[int]*pendingRPC),
		processDone:            make(chan struct{}),
		notificationProtocol:   "unknown",
		controlEvidenceTimeout: testControlEvidenceTimeout,
	}
	c.setThreadID(threadID)
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"` + turnID + `"}}}`)
	if c.controlTurnID.get() != turnID {
		t.Fatalf("turn/started did not arm turn control: mirror = %q, want %q", c.controlTurnID.get(), turnID)
	}
	return c, fs
}

// newActiveTurnCodexClient is the healthy-provider client: the app-server
// acknowledges and then really does terminate the turn it was asked to control.
func newActiveTurnCodexClient(t *testing.T, threadID, turnID string) (*codexClient, *fakeStdinWithHook) {
	t.Helper()
	return newScriptedTurnCodexClient(t, threadID, turnID, ackThenMatchingTerminal(threadID, turnID))
}

// sentFrame decodes the single JSON-RPC frame the control call wrote.
func sentFrame(t *testing.T, fs *fakeStdinWithHook) (string, map[string]any) {
	t.Helper()

	lines := fs.Lines()
	if len(lines) != 1 {
		t.Fatalf("wrote %d frames, want exactly 1: %v", len(lines), lines)
	}
	var frame struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &frame); err != nil {
		t.Fatalf("decode frame %q: %v", lines[0], err)
	}
	return frame.Method, frame.Params
}

// assertNoSuccessEvidence is the fail-closed assertion every negative test
// makes: a failed control call must hand back nothing a caller could mistake
// for proof that the operation took effect.
func assertNoSuccessEvidence(t *testing.T, result TurnControlResult, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a failure, got success with result %+v", result)
	}
	if result != (TurnControlResult{}) {
		t.Fatalf("failed control returned a populated result: %+v", result)
	}
	if result.TerminalEvidence != (TerminalEvidence{}) {
		t.Fatalf("failed control returned terminal evidence: %+v", result.TerminalEvidence)
	}
}

// countStarts reports how many turn/started frames the provider was told about,
// used to prove a steer did not silently restart the turn as a replacement.
func countStarts(c *codexClient) int { return len(c.completedTurnIDs) }

// turn/steer is specified in terms of expectedTurnId, and the app-server
// rejects the turnId that turn/interrupt takes. This is the single most
// load-bearing detail of the Codex adapter.
func TestCodexSteerSendsExpectedTurnIDNotTurnID(t *testing.T) {
	t.Parallel()

	c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "also check the config",
	})
	if err != nil {
		t.Fatalf("controlTurn: %v", err)
	}

	method, params := sentFrame(t, fs)
	if method != "turn/steer" {
		t.Fatalf("method = %q, want turn/steer", method)
	}
	if got := params["expectedTurnId"]; got != "turn-1" {
		t.Fatalf("expectedTurnId = %v, want turn-1", got)
	}
	if _, present := params["turnId"]; present {
		t.Fatalf("turn/steer carried turnId, which the app-server rejects: %v", params)
	}
	if got := params["threadId"]; got != "thread-1" {
		t.Fatalf("threadId = %v, want thread-1", got)
	}

	want := TurnControlResult{
		Provider: "codex", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlSteer,
		TerminalEvidence: TerminalEvidence{
			ThreadID: "thread-1", TurnID: "turn-1", Status: "completed", Kind: TurnTerminalCompleted,
		},
	}
	if result != want {
		t.Fatalf("result = %+v, want %+v", result, want)
	}
}

// Steer input reaches the agent as the single text block turn input uses, the
// same shape codexTurnInput builds for turn/start.
func TestCodexSteerSendsSingleTextBlockInput(t *testing.T) {
	t.Parallel()

	c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")
	if _, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "also check the config",
	}); err != nil {
		t.Fatalf("controlTurn: %v", err)
	}

	_, params := sentFrame(t, fs)
	input, ok := params["input"].([]any)
	if !ok || len(input) != 1 {
		t.Fatalf("input = %#v, want a single-element array", params["input"])
	}
	block, ok := input[0].(map[string]any)
	if !ok {
		t.Fatalf("input block = %#v, want an object", input[0])
	}
	if block["type"] != "text" || block["text"] != "also check the config" {
		t.Fatalf("input block = %#v, want {type:text, text:also check the config}", block)
	}
}

// turn/interrupt takes the plain turnId, and must NOT send expectedTurnId.
func TestCodexInterruptSendsTurnID(t *testing.T) {
	t.Parallel()

	c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if err != nil {
		t.Fatalf("controlTurn: %v", err)
	}

	method, params := sentFrame(t, fs)
	if method != "turn/interrupt" {
		t.Fatalf("method = %q, want turn/interrupt", method)
	}
	if got := params["turnId"]; got != "turn-1" {
		t.Fatalf("turnId = %v, want turn-1", got)
	}
	if _, present := params["expectedTurnId"]; present {
		t.Fatalf("turn/interrupt carried expectedTurnId: %v", params)
	}
	if got := params["threadId"]; got != "thread-1" {
		t.Fatalf("threadId = %v, want thread-1", got)
	}
	if _, present := params["input"]; present {
		t.Fatalf("turn/interrupt carried input: %v", params)
	}

	want := TurnControlResult{
		Provider: "codex", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlInterrupt,
		TerminalEvidence: TerminalEvidence{
			ThreadID: "thread-1", TurnID: "turn-1", Status: "cancelled", Kind: TurnTerminalAborted,
		},
	}
	if result != want {
		t.Fatalf("result = %+v, want %+v", result, want)
	}
}

// ── B1 (a, b): steer takes effect on the turn that was named ────────────────
//
// The turn the caller named must be the turn that terminally completes, and it
// must complete rather than be replaced: steering exists precisely so a turn
// keeps its context instead of being cancelled and re-prompted.
func TestCodexSteerReturnsCorrelatedSameTurnTerminalEvidence(t *testing.T) {
	t.Parallel()

	c, fs := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackThenTerminal(
		codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "completed"},
		// Ordinary in-turn traffic between the ack and the terminal must not
		// confuse the correlation.
		`{"jsonrpc":"2.0","method":"item/started","params":{"item":{"id":"item-1","type":"agentMessage"}}}`,
	))

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "also check the config",
	})
	if err != nil {
		t.Fatalf("controlTurn: %v", err)
	}

	if result.TurnID != "turn-1" {
		t.Fatalf("result.TurnID = %q, want turn-1", result.TurnID)
	}
	ev := result.TerminalEvidence
	if ev.ThreadID != "thread-1" || ev.TurnID != "turn-1" {
		t.Fatalf("evidence = %+v, want thread-1/turn-1 — evidence must be for the requested target", ev)
	}
	if ev.Kind != TurnTerminalCompleted {
		t.Fatalf("evidence kind = %q, want %q — a steered turn completes, it is not aborted", ev.Kind, TurnTerminalCompleted)
	}
	if ev.Status != "completed" {
		t.Fatalf("evidence status = %q, want completed", ev.Status)
	}

	// Exactly one steer went out, and no replacement turn was started: the
	// SAME turn is the one that finished.
	method, _ := sentFrame(t, fs)
	if method != "turn/steer" {
		t.Fatalf("method = %q, want turn/steer", method)
	}
	// c.turnID, not the control mirror: the mirror is deliberately retired once
	// a turn reaches a terminal state (a finished turn is not a legal control
	// target), and this fixture's own turn/completed does exactly that. c.turnID
	// is what still names the last turn the provider started, so it is the field
	// that actually answers "was a replacement turn issued?".
	if got := c.turnID; got != "turn-1" {
		t.Fatalf("last started turn = %q after steer, want turn-1 — the turn was replaced, not steered", got)
	}
	if got := c.controlTurnID.get(); got != "" {
		t.Fatalf("control mirror = %q after the turn terminated, want empty — a finished turn must not stay a control target", got)
	}
	if n := countStarts(c); n != 1 {
		t.Fatalf("%d turns terminated, want exactly 1 — a replacement turn was issued", n)
	}
}

// ── B1 (a, c, d): interrupt terminates the turn that was named ──────────────
func TestCodexInterruptReturnsCorrelatedSameTurnAbortEvidence(t *testing.T) {
	t.Parallel()

	c, fs := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackThenTerminal(
		codexTerminal{threadID: "thread-1", turnID: "turn-1", status: "cancelled"},
	))

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if err != nil {
		t.Fatalf("controlTurn: %v", err)
	}

	ev := result.TerminalEvidence
	if ev.ThreadID != "thread-1" || ev.TurnID != "turn-1" {
		t.Fatalf("evidence = %+v, want thread-1/turn-1", ev)
	}
	if ev.Kind != TurnTerminalAborted {
		t.Fatalf("evidence kind = %q, want %q", ev.Kind, TurnTerminalAborted)
	}
	if ev.Status != "cancelled" {
		t.Fatalf("evidence status = %q, want cancelled", ev.Status)
	}

	// The credit came from the provider's own terminal event, not from the
	// process dying: the process is still very much alive.
	select {
	case <-c.processDone:
		t.Fatal("process exited during the interrupt — abort evidence must come from turn/completed, not process death")
	default:
	}
	if err := c.getProcessErr(); err != nil {
		t.Fatalf("process error %v recorded — interrupt success must not be a process-signal substitute", err)
	}

	method, _ := sentFrame(t, fs)
	if method != "turn/interrupt" {
		t.Fatalf("method = %q, want turn/interrupt", method)
	}
	if n := countStarts(c); n != 1 {
		t.Fatalf("%d turns terminated, want exactly 1 — a replacement turn was issued", n)
	}
}

// ── B1 (a): an acknowledgement is not an effect ─────────────────────────────
//
// THE regression test for the mc-S2718 gap. The app-server accepts the request
// and then nothing further happens. The old implementation returned success
// here; the contract now requires a typed failure.
func TestCodexControlFailsWhenTerminalEvidenceNeverArrives(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  TurnControlRequest
	}{
		{"steer", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1", Input: "more"}},
		{"interrupt", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, fs := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackOnly())

			result, err := c.controlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("error = %v, want ErrTurnControlUncorrelated — an ACK alone was treated as control success", err)
			}
			assertNoSuccessEvidence(t, result, err)

			// The request really was sent and really was acknowledged; the
			// failure is the missing effect, not a missing round trip.
			if len(fs.Lines()) != 1 {
				t.Fatalf("wrote %d frames, want exactly 1", len(fs.Lines()))
			}
		})
	}
}

// ── B1 (a, b, c, e): a terminal for another turn proves nothing about ours ──
func TestCodexControlRejectsMismatchedTerminalTurnEvidence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		req    TurnControlRequest
		status string
	}{
		{"steer", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1", Input: "more"}, "completed"},
		{"interrupt", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"}, "cancelled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackThenTerminal(
				codexTerminal{threadID: "thread-1", turnID: "turn-other", status: tc.status},
			))

			result, err := c.controlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("error = %v, want ErrTurnControlUncorrelated — a foreign turn's terminal was credited to turn-1", err)
			}
			assertNoSuccessEvidence(t, result, err)
		})
	}
}

// ── B1 (a, e): a terminal on another thread proves nothing either ───────────
//
// Codex multiplexes subagent threads onto the same stdio pipe, so this is a
// real shape and not a hypothetical one.
func TestCodexControlRejectsMismatchedTerminalThreadEvidence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		req    TurnControlRequest
		status string
	}{
		{"steer", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1", Input: "more"}, "completed"},
		{"interrupt", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"}, "cancelled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", ackThenTerminal(
				codexTerminal{threadID: "thread-other", turnID: "turn-1", status: tc.status},
			))

			result, err := c.controlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("error = %v, want ErrTurnControlUncorrelated — a foreign thread's terminal was credited", err)
			}
			assertNoSuccessEvidence(t, result, err)
		})
	}
}

// ── B1 (a, e): the target terminated, but in failure ───────────────────────
//
// The evidence correlates, so this is NOT an uncorrelated failure — the
// provider told us exactly what happened. It just is not what was asked for,
// and must not be reported as a successful steer or interrupt.
func TestCodexControlRejectsTerminalFailureForTargetTurn(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  TurnControlRequest
	}{
		{"steer", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1", Input: "more"}},
		{"interrupt", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", func(c *codexClient, id int, _ string) {
				c.handleLine(ackLine(id))
				c.handleLine(`{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1",` +
					`"turn":{"id":"turn-1","status":"failed","error":{"message":"model provider returned 500"}}}}`)
			})

			result, err := c.controlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlTurnFailed) {
				t.Fatalf("error = %v, want ErrTurnControlTurnFailed", err)
			}
			if errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("a correlated failure was reported as uncorrelated: %v", err)
			}
			assertNoSuccessEvidence(t, result, err)
		})
	}
}

// ── B1 (d): process death is never control success ─────────────────────────
//
// The agent dying is not proof the turn was steered or cancelled. This covers
// the generic exit plus the two signal classifications the package produces.
// It is deliberately NOT the only signal-related test: the paired positive,
// TestCodexInterruptReturnsCorrelatedSameTurnAbortEvidence, proves a real
// interrupt succeeds on provider-native terminal evidence instead.
func TestCodexControlRejectsProcessExitAsTerminalEvidence(t *testing.T) {
	t.Parallel()

	exits := []struct {
		name string
		err  error
	}{
		{"generic exit", errCodexProcessExited},
		{"sigterm", fmt.Errorf("%w: %w", errCodexProcessExited, errors.New("signal: terminated"))},
		{"sigkill", fmt.Errorf("%w: %w", errCodexProcessExited, errors.New("signal: killed"))},
	}
	ops := []struct {
		name string
		req  TurnControlRequest
	}{
		{"steer", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1", Input: "more"}},
		{"interrupt", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"}},
	}

	for _, exit := range exits {
		for _, op := range ops {
			t.Run(exit.name+"/"+op.name, func(t *testing.T) {
				t.Parallel()

				// The app-server acknowledges, then the process dies WITHOUT
				// ever reporting the turn terminated.
				c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", func(c *codexClient, id int, _ string) {
					c.handleLine(ackLine(id))
					c.markProcessExited(exit.err)
				})

				result, err := c.controlTurn(context.Background(), op.req)
				assertNoSuccessEvidence(t, result, err)
				if !errors.Is(err, ErrTurnControlUncorrelated) {
					t.Fatalf("error = %v, want ErrTurnControlUncorrelated — process exit was credited as a control effect", err)
				}
				if !errors.Is(err, errCodexProcessExited) {
					t.Fatalf("error = %v, want it to also match errCodexProcessExited so callers can tell the agent died", err)
				}
				// No terminal turn event was ever observed.
				if n := countStarts(c); n != 0 {
					t.Fatalf("%d turns terminated, want 0 — nothing should have completed", n)
				}
			})
		}
	}
}

// ── B1 (e): a provider protocol rejection is typed ─────────────────────────
//
// The app-server rejects the request shape — complaining about the very field
// each RPC renames. The code under test still sends the CORRECT field; this
// asserts that a protocol-level refusal is surfaced as a typed wire rejection
// rather than as a panic or a successful-looking result.
func TestCodexControlProviderWrongWireFieldIsTypedFailure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		req     TurnControlRequest
		message string
	}{
		{
			"steer rejects turnId",
			TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-1", Input: "more"},
			"unexpected field 'turnId': turn/steer requires expectedTurnId",
		},
		{
			"interrupt rejects expectedTurnId",
			TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-1"},
			"unexpected field 'expectedTurnId': turn/interrupt requires turnId",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", rejectWith(-32602, tc.message))

			result, err := c.controlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlRejected) {
				t.Fatalf("error = %v, want ErrTurnControlRejected", err)
			}
			// A wire rejection is not an effect failure: the request never got
			// far enough to have an effect to be missing.
			if errors.Is(err, ErrTurnControlUncorrelated) {
				t.Fatalf("wire rejection misreported as uncorrelated evidence: %v", err)
			}
			assertNoSuccessEvidence(t, result, err)
		})
	}
}

// A turn id the thread has moved past must be refused for BOTH operations
// without any provider I/O. Interrupt is the reason this check exists locally:
// the app-server would happily cancel whatever turn a stale turnId named.
func TestCodexControlRefusesStaleTurnIDWithoutSending(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		req  TurnControlRequest
	}{
		{"steer", TurnControlRequest{Op: TurnControlSteer, TurnID: "turn-0", Input: "late"}},
		{"interrupt", TurnControlRequest{Op: TurnControlInterrupt, TurnID: "turn-0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")
			result, err := c.controlTurn(context.Background(), tc.req)
			if !errors.Is(err, ErrTurnControlInactive) {
				t.Fatalf("error = %v, want ErrTurnControlInactive", err)
			}
			assertNoSuccessEvidence(t, result, err)
			if lines := fs.Lines(); len(lines) != 0 {
				t.Fatalf("stale turn id reached the app-server: %v", lines)
			}
		})
	}
}

func TestCodexControlRefusesBeforeAnyTurnStarted(t *testing.T) {
	t.Parallel()

	fs := &fakeStdinWithHook{}
	c := &codexClient{
		cfg:                    Config{Logger: slog.Default()},
		stdin:                  fs,
		pending:                make(map[int]*pendingRPC),
		processDone:            make(chan struct{}),
		controlEvidenceTimeout: testControlEvidenceTimeout,
	}
	c.setThreadID("thread-1")

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if !errors.Is(err, ErrTurnControlInactive) {
		t.Fatalf("error = %v, want ErrTurnControlInactive", err)
	}
	assertNoSuccessEvidence(t, result, err)
	if lines := fs.Lines(); len(lines) != 0 {
		t.Fatalf("control reached the app-server with no live turn: %v", lines)
	}
}

// A process that is already dead is refused before any I/O. This is the
// pre-flight half of the signal story; the mid-flight half is
// TestCodexControlRejectsProcessExitAsTerminalEvidence.
func TestCodexControlFailsAfterProcessExit(t *testing.T) {
	t.Parallel()

	exits := []struct {
		name string
		err  error
	}{
		{"generic exit", errCodexProcessExited},
		{"sigterm", fmt.Errorf("%w: %w", errCodexProcessExited, errors.New("signal: terminated"))},
		{"sigkill", fmt.Errorf("%w: %w", errCodexProcessExited, errors.New("signal: killed"))},
	}
	for _, exit := range exits {
		t.Run(exit.name, func(t *testing.T) {
			t.Parallel()

			c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")
			c.markProcessExited(exit.err)

			result, err := c.controlTurn(context.Background(), TurnControlRequest{
				Op: TurnControlInterrupt, TurnID: "turn-1",
			})
			if !errors.Is(err, errCodexProcessExited) {
				t.Fatalf("error = %v, want errCodexProcessExited", err)
			}
			assertNoSuccessEvidence(t, result, err)
			if lines := fs.Lines(); len(lines) != 0 {
				t.Fatalf("control wrote to a dead process: %v", lines)
			}
		})
	}
}

// An RPC rejection from the app-server must surface as a typed wire rejection,
// not be swallowed into a successful-looking result.
func TestCodexControlSurfacesProviderRejection(t *testing.T) {
	t.Parallel()

	c, _ := newScriptedTurnCodexClient(t, "thread-1", "turn-1", rejectWith(-32602, "turn is not active"))

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "more",
	})
	if !errors.Is(err, ErrTurnControlRejected) {
		t.Fatalf("error = %v, want ErrTurnControlRejected", err)
	}
	assertNoSuccessEvidence(t, result, err)
}

// The whole point of the capability: a Codex session can be steered and
// interrupted through the provider-neutral surface, with no Codex knowledge at
// the call site — and the terminal evidence survives that boundary.
func TestCodexSessionSatisfiesNeutralTurnControl(t *testing.T) {
	t.Parallel()

	c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")
	session := &Session{turnControl: c.controlTurn}

	if !session.SupportsTurnControl() {
		t.Fatal("codex session reports turn control unsupported")
	}

	// ControlTurn is the B1-proof API: it returns the evidence.
	result, err := session.ControlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "also check the config",
	})
	if err != nil {
		t.Fatalf("ControlTurn: %v", err)
	}
	ev := result.TerminalEvidence
	if ev.ThreadID != "thread-1" || ev.TurnID != "turn-1" || ev.Kind != TurnTerminalCompleted {
		t.Fatalf("evidence = %+v, want thread-1/turn-1 completed — the neutral surface erased it", ev)
	}
	if method, _ := sentFrame(t, fs); method != "turn/steer" {
		t.Fatalf("method = %q, want turn/steer", method)
	}
}

// The error-only wrapper still works, but it is a convenience smoke check, NOT
// B1 effect proof: it discards the evidence that constitutes the proof.
func TestCodexSessionWrapperIsNotEffectProof(t *testing.T) {
	t.Parallel()

	c, _ := newActiveTurnCodexClient(t, "thread-1", "turn-1")
	session := &Session{turnControl: c.controlTurn}

	if err := session.SteerTurn(context.Background(), "turn-1", "also check the config"); err != nil {
		t.Fatalf("SteerTurn: %v", err)
	}
}

// The mirrors exist so a control caller on another goroutine reads the live
// ids race-free; setTurnID is what keeps them in step with the originals.
// Regression coverage for addressing the correct target — not effect proof.
func TestCodexControlMirrorsFollowTurnLifecycle(t *testing.T) {
	t.Parallel()

	c, _ := newActiveTurnCodexClient(t, "thread-1", "turn-1")
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-2"}}}`)

	if c.turnID != "turn-2" {
		t.Fatalf("turnID = %q, want turn-2", c.turnID)
	}
	if got := c.controlTurnID.get(); got != "turn-2" {
		t.Fatalf("control mirror = %q, want turn-2 — mirror drifted from turnID", got)
	}
	if got := c.controlThreadID.get(); got != "thread-1" {
		t.Fatalf("thread mirror = %q, want thread-1", got)
	}
}

// codexTerminalKind is the single place a provider status becomes a neutral
// classification, so it is worth pinning directly.
func TestCodexTerminalKindClassifiesProviderStatuses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status  string
		aborted bool
		want    TurnTerminalKind
	}{
		{"completed", false, TurnTerminalCompleted},
		{"failed", false, TurnTerminalFailed},
		{"cancelled", true, TurnTerminalAborted},
		{"canceled", true, TurnTerminalAborted},
		{"aborted", true, TurnTerminalAborted},
		{"interrupted", true, TurnTerminalAborted},
	}
	for _, tc := range cases {
		if got := codexTerminalKind(tc.status, tc.aborted); got != tc.want {
			t.Fatalf("codexTerminalKind(%q, %v) = %q, want %q", tc.status, tc.aborted, got, tc.want)
		}
	}
}

func TestCodexTurnSteerParamsMarshalRenamesTurnID(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(codexTurnSteerParams{
		ThreadID: "thread-1",
		TurnID:   "turn-1",
		Input:    codexTurnControlInput("hello"),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"threadId":"thread-1","expectedTurnId":"turn-1","input":[{"text":"hello","type":"text"}]}`
	if string(data) != want {
		t.Fatalf("marshal = %s\nwant     = %s", data, want)
	}
}

func TestCodexTurnInterruptParamsMarshalKeepsTurnID(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(codexTurnInterruptParams{ThreadID: "thread-1", TurnID: "turn-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"threadId":"thread-1","turnId":"turn-1"}`
	if string(data) != want {
		t.Fatalf("marshal = %s\nwant     = %s", data, want)
	}
}

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

// newActiveTurnCodexClient returns a client that has a live thread and turn,
// with the turn established through the real turn/started path so the test
// exercises the same wiring production does. afterWrite answers whatever RPC
// the control call sends, standing in for the app-server.
func newActiveTurnCodexClient(t *testing.T, threadID, turnID string) (*codexClient, *fakeStdinWithHook) {
	t.Helper()

	var c *codexClient
	fs := &fakeStdinWithHook{}
	fs.afterWrite = func() {
		c.handleLine(`{"jsonrpc":"2.0","id":1,"result":{}}`)
	}
	c = &codexClient{
		cfg:                  Config{Logger: slog.Default()},
		stdin:                fs,
		pending:              make(map[int]*pendingRPC),
		processDone:          make(chan struct{}),
		notificationProtocol: "unknown",
	}
	c.setThreadID(threadID)
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"` + turnID + `"}}}`)
	if c.controlTurnID.get() != turnID {
		t.Fatalf("turn/started did not arm turn control: mirror = %q, want %q", c.controlTurnID.get(), turnID)
	}
	return c, fs
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

	want := TurnControlResult{Provider: "codex", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlSteer}
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

	want := TurnControlResult{Provider: "codex", SessionID: "thread-1", TurnID: "turn-1", Op: TurnControlInterrupt}
	if result != want {
		t.Fatalf("result = %+v, want %+v", result, want)
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
			if _, err := c.controlTurn(context.Background(), tc.req); !errors.Is(err, ErrTurnControlInactive) {
				t.Fatalf("error = %v, want ErrTurnControlInactive", err)
			}
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
		cfg:         Config{Logger: slog.Default()},
		stdin:       fs,
		pending:     make(map[int]*pendingRPC),
		processDone: make(chan struct{}),
	}
	c.setThreadID("thread-1")

	if _, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	}); !errors.Is(err, ErrTurnControlInactive) {
		t.Fatalf("error = %v, want ErrTurnControlInactive", err)
	}
	if lines := fs.Lines(); len(lines) != 0 {
		t.Fatalf("control reached the app-server with no live turn: %v", lines)
	}
}

func TestCodexControlFailsAfterProcessExit(t *testing.T) {
	t.Parallel()

	c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")
	c.markProcessExited(errCodexProcessExited)

	_, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlInterrupt, TurnID: "turn-1",
	})
	if !errors.Is(err, errCodexProcessExited) {
		t.Fatalf("error = %v, want errCodexProcessExited", err)
	}
	if lines := fs.Lines(); len(lines) != 0 {
		t.Fatalf("control wrote to a dead process: %v", lines)
	}
}

// An RPC rejection from the app-server must surface, not be swallowed into a
// successful-looking result.
func TestCodexControlSurfacesProviderRejection(t *testing.T) {
	t.Parallel()

	var c *codexClient
	fs := &fakeStdinWithHook{}
	fs.afterWrite = func() {
		c.handleLine(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"turn is not active"}}`)
	}
	c = &codexClient{
		cfg:                  Config{Logger: slog.Default()},
		stdin:                fs,
		pending:              make(map[int]*pendingRPC),
		processDone:          make(chan struct{}),
		notificationProtocol: "unknown",
	}
	c.setThreadID("thread-1")
	c.handleLine(`{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-1"}}}`)

	result, err := c.controlTurn(context.Background(), TurnControlRequest{
		Op: TurnControlSteer, TurnID: "turn-1", Input: "more",
	})
	if err == nil {
		t.Fatalf("expected the app-server rejection to surface, got result %+v", result)
	}
	if (result != TurnControlResult{}) {
		t.Fatalf("failed control returned a populated result: %+v", result)
	}
}

// The whole point of the capability: a Codex session can be steered and
// interrupted through the provider-neutral surface, with no Codex knowledge at
// the call site.
func TestCodexSessionSatisfiesNeutralTurnControl(t *testing.T) {
	t.Parallel()

	c, fs := newActiveTurnCodexClient(t, "thread-1", "turn-1")
	session := &Session{turnControl: c.controlTurn}

	if !session.SupportsTurnControl() {
		t.Fatal("codex session reports turn control unsupported")
	}
	if err := session.SteerTurn(context.Background(), "turn-1", "also check the config"); err != nil {
		t.Fatalf("SteerTurn: %v", err)
	}
	if method, _ := sentFrame(t, fs); method != "turn/steer" {
		t.Fatalf("method = %q, want turn/steer", method)
	}
}

// The mirrors exist so a control caller on another goroutine reads the live
// ids race-free; setTurnID is what keeps them in step with the originals.
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

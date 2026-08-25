package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type controlWriteCloser struct {
	strings.Builder
	writes atomic.Int32
}

func (w *controlWriteCloser) Write(p []byte) (int, error) {
	w.writes.Add(1)
	return w.Builder.Write(p)
}

func (w *controlWriteCloser) Close() error { return nil }

func newControlClient(w io.WriteCloser) *codexClient {
	return &codexClient{
		stdin:            w,
		pending:          make(map[int64]chan codexRPCResponse),
		closed:           make(chan struct{}),
		completedEvents:  make(chan codexTurnEventParams, 8),
		controlResults:   make(map[string]ControlResult),
		firstItemWaitObs: &codexFirstItemWaitObservation{},
		stderrTail:       newStderrTail(io.Discard, 1024),
	}
}

func activeControlSession(w io.WriteCloser) (*Session, *codexClient, string, string) {
	client := newControlClient(w)
	threadID, turnID := "thread-1", "turn-1"
	client.setThreadID(threadID)
	client.setTurnID(turnID)
	return &Session{control: client.control}, client, threadID, turnID
}

func completeControl(client *codexClient, threadID, turnID string) {
	client.completedEvents <- codexTurnEventParams{ThreadID: threadID, Turn: codexTurnEvent{ID: turnID}}
}

func decodeControlWrite(t *testing.T, raw string) map[string]any {
	t.Helper()
	line, err := bufio.NewReader(strings.NewReader(raw)).ReadString('\n')
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	var request map[string]any
	if err := json.Unmarshal([]byte(line), &request); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	return request
}

func TestCodexControlCheckpointAndRedirectUseTurnSteer(t *testing.T) {
	for _, operation := range []ControlOperation{ControlCheckpoint, ControlCancelAndRedirect} {
		t.Run(string(operation), func(t *testing.T) {
			writer := &controlWriteCloser{}
			session, client, threadID, turnID := activeControlSession(writer)
			client.nextID.Store(0)
			completeControl(client, threadID, turnID)

			requestID := "request-" + string(operation)
			result, err := session.Control(context.Background(), ControlRequest{
				RequestID:                 requestID,
				Operation:                 operation,
				Instruction:               "commit and push the current checkpoint",
				ExpectedProviderSessionID: threadID,
				ExpectedProviderTurnID:    turnID,
			})
			if err != nil {
				t.Fatalf("Control: %v", err)
			}
			if !result.Accepted || result.BoundaryKind != "codex.turn_completed" || result.ProviderSessionID != threadID || result.ProviderTurnID != turnID {
				t.Fatalf("result = %+v", result)
			}
			request := decodeControlWrite(t, writer.String())
			if request["method"] != "turn/steer" {
				t.Fatalf("method = %v", request["method"])
			}
			params := request["params"].(map[string]any)
			if params["threadId"] != threadID || params["expectedTurnId"] != turnID {
				t.Fatalf("wrong steer turn params: %#v", params)
			}
			if _, exists := params["turnId"]; exists {
				t.Fatalf("turn/steer emitted interrupt-only turnId field: %#v", params)
			}
			input := params["input"].([]any)[0].(map[string]any)
			if input["type"] != "text" || input["text"] != "commit and push the current checkpoint" {
				t.Fatalf("input = %#v", input)
			}
		})
	}
}

func TestCodexControlInterruptUsesTurnInterrupt(t *testing.T) {
	writer := &controlWriteCloser{}
	session, client, threadID, turnID := activeControlSession(writer)
	completeControl(client, threadID, turnID)
	result, err := session.Control(context.Background(), ControlRequest{
		RequestID:                 "request-interrupt",
		Operation:                 ControlInterrupt,
		ExpectedProviderSessionID: threadID,
		ExpectedProviderTurnID:    turnID,
	})
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	if !result.Accepted || result.TerminalEvidence != "codex.turn/completed" {
		t.Fatalf("result = %+v", result)
	}
	request := decodeControlWrite(t, writer.String())
	if request["method"] != "turn/interrupt" {
		t.Fatalf("method = %v", request["method"])
	}
}

func TestCodexControlWrongIdentityAndInactiveTurnDoNoWrites(t *testing.T) {
	cases := []struct {
		name    string
		request ControlRequest
		setup   func(*codexClient)
	}{
		{
			name: "wrong session",
			request: ControlRequest{RequestID: "wrong-session", Operation: ControlInterrupt,
				ExpectedProviderSessionID: "thread-other", ExpectedProviderTurnID: "turn-1"},
		},
		{
			name: "stale turn",
			request: ControlRequest{RequestID: "stale-turn", Operation: ControlInterrupt,
				ExpectedProviderSessionID: "thread-1", ExpectedProviderTurnID: "turn-old"},
		},
		{
			name: "inactive turn",
			request: ControlRequest{RequestID: "inactive", Operation: ControlInterrupt,
				ExpectedProviderSessionID: "thread-1", ExpectedProviderTurnID: "turn-1"},
			setup: func(client *codexClient) { client.setTurnID("") },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writer := &controlWriteCloser{}
			session, client, _, _ := activeControlSession(writer)
			if tc.setup != nil {
				tc.setup(client)
			}
			if _, err := session.Control(context.Background(), tc.request); err == nil {
				t.Fatal("Control succeeded")
			}
			if writer.writes.Load() != 0 {
				t.Fatalf("provider writes = %d", writer.writes.Load())
			}
		})
	}
}

func TestCodexControlRequestIDIsIdempotent(t *testing.T) {
	writer := &controlWriteCloser{}
	session, client, threadID, turnID := activeControlSession(writer)
	completeControl(client, threadID, turnID)
	request := ControlRequest{
		RequestID:                 "same-request",
		Operation:                 ControlInterrupt,
		ExpectedProviderSessionID: threadID,
		ExpectedProviderTurnID:    turnID,
	}
	first, err := session.Control(context.Background(), request)
	if err != nil {
		t.Fatalf("first Control: %v", err)
	}
	second, err := session.Control(context.Background(), request)
	if err != nil {
		t.Fatalf("second Control: %v", err)
	}
	if first != second {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if writer.writes.Load() != 1 {
		t.Fatalf("provider writes = %d, want 1", writer.writes.Load())
	}
}

func TestCodexControlRejectsProcessExitWithoutProviderEvidence(t *testing.T) {
	writer := &controlWriteCloser{}
	session, client, threadID, turnID := activeControlSession(writer)
	close(client.closed)
	_, err := session.Control(context.Background(), ControlRequest{
		RequestID:                 "process-exit",
		Operation:                 ControlInterrupt,
		ExpectedProviderSessionID: threadID,
		ExpectedProviderTurnID:    turnID,
	})
	if !errors.Is(err, ErrControlInactive) {
		t.Fatalf("error = %v, want inactive", err)
	}
	if writer.writes.Load() != 1 {
		t.Fatalf("provider writes = %d, want request only", writer.writes.Load())
	}
}

func TestCodexControlContextCancellationDoesNotAccept(t *testing.T) {
	writer := &controlWriteCloser{}
	session, _, threadID, turnID := activeControlSession(writer)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := session.Control(ctx, ControlRequest{
		RequestID:                 "cancelled-context",
		Operation:                 ControlInterrupt,
		ExpectedProviderSessionID: threadID,
		ExpectedProviderTurnID:    turnID,
	})
	if err == nil || result.Accepted {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

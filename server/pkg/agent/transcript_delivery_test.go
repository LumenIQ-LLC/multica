package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClaudeTranscriptOverflowHasExplicitCompleteness(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend, err := New("claude", Config{ExecutablePath: self,
		Env: map[string]string{"CLAUDE_FAKE_MODE": "transcript_overflow", "IS_SANDBOX": "1"}, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "fixture", ExecOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately do not drain the 256-message stream until completion.
	select {
	case result := <-session.Result:
		if result.Status != "completed" || !result.TranscriptDeliveryChecked || result.DroppedMessages != 344 {
			t.Fatalf("silent loss or blocked completion: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("completion blocked on a slow transcript consumer")
	}
	for range session.Messages {
	}
}

func TestTranscriptDeliveryReportsBurstLossWithoutConsumer(t *testing.T) {
	ch := make(chan Message, 256)
	var dropped atomic.Int64
	var writers sync.WaitGroup
	for range 10 {
		writers.Go(func() {
			for range 100 {
				trySend(ch, Message{Type: MessageText, Content: "fixture"}, &dropped)
			}
		})
	}
	writers.Wait()
	close(ch)
	if got := dropped.Load(); got != 744 {
		t.Fatalf("loss=%d want744", got)
	}
	resultCh := make(chan Result, 1)
	resultCh <- Result{Status: "completed", Output: "final answer", DroppedMessages: 2}
	close(resultCh)
	select {
	case result := <-transcriptResult(resultCh, &dropped):
		if !result.TranscriptDeliveryChecked || result.DroppedMessages != 746 || result.Output != "final answer" {
			t.Fatalf("completeness metadata missing: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("completion blocked by absent transcript consumer")
	}
}

func TestTranscriptDeliveryReportsNoLossAndClosedResult(t *testing.T) {
	var dropped atomic.Int64
	ch := make(chan Message, 1)
	trySend(ch, Message{Type: MessageStatus}, &dropped)
	<-ch
	input := make(chan Result, 1)
	input <- Result{Status: "cancelled"}
	close(input)
	result := <-transcriptResult(input, &dropped)
	if !result.TranscriptDeliveryChecked || result.DroppedMessages != 0 || result.Status != "cancelled" {
		t.Fatalf("incorrect result %+v", result)
	}
	empty := make(chan Result)
	close(empty)
	if _, ok := <-transcriptResult(empty, &dropped); ok {
		t.Fatal("fabricated result")
	}
}

func TestTranscriptLossCounterIsBounded(t *testing.T) {
	if got := AccumulateDroppedMessages(MaxDroppedMessages-1, 10); got != MaxDroppedMessages {
		t.Fatalf("unbounded count %d", got)
	}
	if got := AccumulateDroppedMessages(3, -5, 4); got != 7 {
		t.Fatalf("known loss erased %d", got)
	}
}

func TestCodexRelayPreservesSessionPinAtCapacity(t *testing.T) {
	ch := make(chan Message, 256)
	var dropped atomic.Int64
	for range 256 {
		relayCodexMessage(ch, Message{Type: MessageText, Content: "fixture"}, &dropped)
	}
	relayCodexMessage(ch, Message{Type: MessageStatus, Status: "running", SessionID: "pin-fixture"}, &dropped)
	if dropped.Load() != 1 {
		t.Fatalf("evicted transcript event not counted: %d", dropped.Load())
	}
	close(ch)
	found := false
	for message := range ch {
		if message.SessionID == "pin-fixture" {
			found = true
		}
	}
	if !found {
		t.Fatal("session pin lost behind full transcript relay")
	}
}

func runFakeCodexTranscriptOverflow() {
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request struct {
			ID     json.RawMessage
			Method string
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil || len(request.ID) == 0 {
			continue
		}
		result := map[string]any{}
		if request.Method == "thread/start" || request.Method == "thread/resume" {
			result["thread"] = map[string]any{"id": "pin-fixture"}
		}
		_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
		if request.Method == "turn/start" {
			emit := func(msg map[string]any) {
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "codex/event", "params": map[string]any{"msg": msg}})
			}
			emit(map[string]any{"type": "task_started"})
			for range 600 {
				emit(map[string]any{"type": "agent_message", "message": "fixture"})
			}
			emit(map[string]any{"type": "task_started"})
			emit(map[string]any{"type": "task_complete"})
		}
	}
}

func TestCodexTranscriptOverflowPreservesFinalSessionIdentity(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if err := os.MkdirAll(filepath.Join(home, "sessions"), 0700); err != nil {
		t.Fatal(err)
	}
	backend, err := New("codex", Config{ExecutablePath: self, CodexVersion: "fixture", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Env: map[string]string{"CLAUDE_FAKE_MODE": "codex_transcript_overflow", "CODEX_HOME": home, "HOME": home, "USERPROFILE": home}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "fixture", ExecOptions{Cwd: home, Timeout: 5 * time.Second, HandshakeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// An absent consumer must not deadlock either backend or relay completion.
	select {
	case result := <-session.Result:
		if result.Status != "completed" || result.SessionID != "pin-fixture" || !result.TranscriptDeliveryChecked || result.DroppedMessages == 0 {
			t.Fatalf("lost final identity/completeness: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("completion blocked on full transcript relay")
	}
	found := false
	for message := range session.Messages {
		if message.SessionID == "pin-fixture" {
			found = true
		}
	}
	if !found {
		t.Fatal("bounded relay discarded every session pin")
	}
}

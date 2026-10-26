package agent

import (
	"context"
	"log/slog"
	"os"
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

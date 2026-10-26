package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestTranscriptLossSurvivesRetryErrorWithKnownRetryDrops(t *testing.T) {
	first := agent.Result{Status: "failed", TranscriptDeliveryChecked: true, DroppedMessages: 3}
	for _, checked := range []bool{true, false} {
		retry := agent.Result{TranscriptDeliveryChecked: checked, DroppedMessages: 4}
		result, _ := reconcileFreshRetryResult(first, nil, 0, retry, 0, errors.New("retry failed after draining"))
		if result.DroppedMessages != 7 {
			t.Fatalf("retry error erased known loss: %+v", result)
		}
		if result.TranscriptDeliveryChecked != checked {
			t.Fatalf("unknown retry delivery was promoted: %+v", result)
		}
	}
}

func overflowSession() *agent.Session {
	messages := make(chan agent.Message, 256)
	var dropped int64
	for i := range 600 {
		select {
		case messages <- agent.Message{Type: agent.MessageText, Content: fmt.Sprintf("fixture%d ", i)}:
		default:
			dropped++
		}
	}
	close(messages)
	results := make(chan agent.Result, 1)
	results <- agent.Result{Status: "completed", Output: "done", SessionID: "fixture-session", TranscriptDeliveryChecked: true, DroppedMessages: dropped}
	close(results)
	return &agent.Session{Messages: messages, Result: results}
}

func TestTranscriptGapReportedAfterDrainBeforeTerminalHandoff(t *testing.T) {
	d, rec := newTranscriptRecorder(t)
	result, _, err := d.executeAndDrain(context.Background(), sessionBackend{session: overflowSession()}, "fixture", agent.ExecOptions{}, slog.Default(), "task-gap", "", new(atomic.Int32))
	if err != nil {
		t.Fatal(err)
	}
	if result.DroppedMessages != 344 {
		t.Fatalf("lost count %+v", result)
	}
	messages := rec.snapshot()
	if len(messages) != 2 || messages[0].Type != "text" {
		t.Fatalf("drain tail missing: %+v", messages)
	}
	gap := messages[1]
	if gap.Type != "error" || gap.Seq != messages[0].Seq+1 || gap.Input["event_type"] != "transcript.gap" || gap.Input["complete"] != false || gap.Input["dropped_messages"] != float64(344) {
		t.Fatalf("durable message path did not receive gap after tail: %+v", gap)
	}
}

func TestTranscriptGapPersistenceFailureCannotCompleteSuccessfully(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/daemon/tasks/task-gap/messages" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	d := &Daemon{client: NewClient(server.URL), logger: slog.Default()}
	result, _, err := d.executeAndDrain(context.Background(), sessionBackend{session: overflowSession()}, "fixture", agent.ExecOptions{}, slog.Default(), "task-gap", "", new(atomic.Int32))
	if err == nil {
		t.Fatalf("missing gap marker allowed successful terminal handoff: %+v", result)
	}
}

func TestTranscriptCancellationRetainsKnownResultLossAndMarksIncomplete(t *testing.T) {
	d, rec := newTranscriptRecorder(t)
	messages := make(chan agent.Message)
	results := make(chan agent.Result, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan agent.Result, 1)
	go func() {
		result, _, err := d.executeAndDrain(ctx, sessionBackend{session: &agent.Session{Messages: messages, Result: results}}, "fixture", agent.ExecOptions{}, slog.Default(), "task-cancel", "", new(atomic.Int32))
		if err != nil {
			t.Errorf("cancel drain: %v", err)
		}
		done <- result
	}()
	messages <- agent.Message{Type: agent.MessageText, Content: "tail"}
	// Make both terminal and cancellation paths possible. Either must preserve
	// this settled count and report the gap before the caller can transition.
	results <- agent.Result{Status: "cancelled", SessionID: "pin-fixture", TranscriptDeliveryChecked: true, DroppedMessages: 17}
	cancel()
	close(messages)
	select {
	case result := <-done:
		if result.DroppedMessages != 17 || result.SessionID != "pin-fixture" {
			t.Fatalf("cancel discarded known identity/loss: %+v", result)
		}
		reported := rec.snapshot()
		if len(reported) < 2 || reported[len(reported)-1].Input["complete"] != false {
			t.Fatalf("cancel hid incomplete transcript: %+v", reported)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation blocked on transcript delivery")
	}
}

func TestTranscriptKnownRetryLossPersistsEvenWhenCompletenessIsUnknown(t *testing.T) {
	first := agent.Result{Status: "failed", TranscriptDeliveryChecked: true, DroppedMessages: 3}
	retry := agent.Result{TranscriptDeliveryChecked: false, DroppedMessages: 4}
	merged, _ := reconcileFreshRetryResult(first, nil, 0, retry, 0, errors.New("retry result incomplete"))
	messages := make(chan agent.Message)
	close(messages)
	results := make(chan agent.Result, 1)
	results <- merged
	close(results)
	d, rec := newTranscriptRecorder(t)
	result, _, err := d.executeAndDrain(context.Background(), sessionBackend{session: &agent.Session{Messages: messages, Result: results}}, "fixture", agent.ExecOptions{}, slog.Default(), "task-unknown", "", new(atomic.Int32))
	if err != nil {
		t.Fatal(err)
	}
	if result.TranscriptDeliveryChecked || result.DroppedMessages != 7 {
		t.Fatalf("unknownness or known count lost: %+v", result)
	}
	recorded := rec.snapshot()
	if len(recorded) != 1 || recorded[0].Input["dropped_messages"] != float64(7) || recorded[0].Input["delivery_checked"] != false || recorded[0].Input["complete"] != false {
		t.Fatalf("known retry loss not durably reported: %+v", recorded)
	}
}

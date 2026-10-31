package daemon

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

// reportTranscriptGap uses the existing durable task-message endpoint and its
// rendered error type. Failure is returned to prevent a successful terminal
// handoff from hiding the missing completeness marker. No producer waits on it.
func (d *Daemon) reportTranscriptGap(taskID string, seq *atomic.Int32, result agent.Result) error {
	count := agent.AccumulateDroppedMessages(result.DroppedMessages)
	reason := "bounded_delivery_buffer_overflow"
	if result.Status == "cancelled" || result.Status == "timeout" || result.Status == "idle_watchdog" {
		reason = "transcript_drain_interrupted"
	}
	eventType := "transcript.gap"
	content := fmt.Sprintf("Transcript incomplete: %s; at least %d messages were dropped. Final output does not certify transcript completeness.", reason, count)
	if count == 0 && !result.TranscriptDeliveryChecked {
		eventType = "transcript.completeness_unknown"
		content = "Transcript completeness is unknown: the drain was interrupted. No dropped-message count was confirmed."
	}
	message := TaskMessageData{
		Seq: int(seq.Add(1)), Type: "error",
		Content: content,
		Input: map[string]any{
			"event_type": eventType, "complete": false,
			"reason": reason, "dropped_messages": count,
			"count_capped":     count == agent.MaxDroppedMessages,
			"delivery_checked": result.TranscriptDeliveryChecked,
		},
	}
	sendCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.client.ReportTaskMessages(sendCtx, taskID, []TaskMessageData{message})
}

package daemon

import (
	"errors"
	"testing"

	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestTranscriptLossSurvivesFreshSessionRetry(t *testing.T) {
	for _, status := range []string{"completed", "failed"} {
		t.Run(status, func(t *testing.T) {
			first := agent.Result{Status: "failed", TranscriptDeliveryChecked: true, DroppedMessages: 3}
			retry := agent.Result{Status: status, TranscriptDeliveryChecked: true, DroppedMessages: 4}
			result, _ := reconcileFreshRetryResult(first, nil, 0, retry, 0, nil)
			if result.DroppedMessages != 7 || !result.TranscriptDeliveryChecked {
				t.Fatalf("retry erased transcript loss: %+v", result)
			}
		})
	}
	first := agent.Result{TranscriptDeliveryChecked: true, DroppedMessages: 3}
	result, _ := reconcileFreshRetryResult(first, nil, 0, agent.Result{}, 0, errors.New("spawn failed"))
	if result.DroppedMessages != 3 || !result.TranscriptDeliveryChecked {
		t.Fatal("spawn failure erased original delivery metadata")
	}
}

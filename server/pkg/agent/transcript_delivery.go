package agent

import "sync/atomic"

// transcriptResult reports bounded-buffer loss independently of the selected
// final output. The result channel stays buffered, so a slow/absent transcript
// consumer cannot deadlock completion or cancellation.
func transcriptResult(input <-chan Result, dropped *atomic.Int64) <-chan Result {
	output := make(chan Result, 1)
	go func() {
		defer close(output)
		if result, ok := <-input; ok {
			result.TranscriptDeliveryChecked = true
			result.DroppedMessages += dropped.Load()
			output <- result
		}
	}()
	return output
}

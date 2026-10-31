package agent

import "sync/atomic"

// MaxDroppedMessages bounds exported loss counters; a capped value is a lower bound.
const MaxDroppedMessages int64 = 1_000_000_000

// AccumulateDroppedMessages combines known loss without overflowing or erasing it.
func AccumulateDroppedMessages(counts ...int64) int64 {
	var total int64
	for _, count := range counts {
		if count <= 0 {
			continue
		}
		if count >= MaxDroppedMessages-total {
			return MaxDroppedMessages
		}
		total += count
	}
	return total
}

// relayCodexMessage keeps the newest session pin available even when ordinary
// transcript output fills the outer relay. Eviction is counted as transcript loss;
// neither output backpressure nor cancellation can block a backend producer.
func relayCodexMessage(ch chan Message, msg Message, dropped *atomic.Int64) {
	if msg.Type != MessageStatus || msg.SessionID == "" {
		trySend(ch, msg, dropped)
		return
	}
	select {
	case ch <- msg:
		return
	default:
	}
	select {
	case <-ch:
		dropped.Add(1)
	default:
	}
	trySend(ch, msg, dropped)
}

// transcriptResult reports bounded-buffer loss independently of the selected
// final output. The result channel stays buffered, so a slow/absent transcript
// consumer cannot deadlock completion or cancellation.
func transcriptResult(input <-chan Result, dropped *atomic.Int64) <-chan Result {
	output := make(chan Result, 1)
	go func() {
		defer close(output)
		if result, ok := <-input; ok {
			result.TranscriptDeliveryChecked = true
			result.DroppedMessages = AccumulateDroppedMessages(result.DroppedMessages, dropped.Load())
			output <- result
		}
	}()
	return output
}

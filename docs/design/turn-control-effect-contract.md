# Turn-control effect contract (mc-S2726 B1)

**Status:** decided, implemented.
**Applies to:** `Session.ControlTurn` / `SteerTurn` / `InterruptTurn` and the Codex
adapter in `server/pkg/agent/codex_control.go`.

## The question

B1 established that an acknowledgement is not an effect: `turn/steer` and
`turn/interrupt` are accepted by the app-server long before anything has
actually happened to the turn, and the pre-B1 adapter reported that ack as
success. B1's fix was to wait for the turn's own terminal event and correlate
it.

Applying that single rule to BOTH operations produced two defects:

1. **False failure on a healthy steer.** The evidence timeout is 5 minutes and
   sits under a 10-minute semantic inactivity budget. A steered turn that
   accepts the input and then works for six more minutes — the *expected*
   outcome of steering — returns `ErrTurnControlUncorrelated`. A caller that
   retries on failure then sends the input a second time and the turn receives
   it twice.
2. **A surprising contract.** `SteerTurn` is documented as "send additional
   input to the turn". Waiting for termination makes it block for the entire
   remainder of the turn.

Both follow from conflating two different claims under one word, "effect".

## The decision

**Interrupt and steer assert different things, so they wait for different
evidence.**

### Interrupt — unchanged

An interrupt's effect *is* termination. Waiting for a correlated terminal event
is exactly right, and the 5-minute window is generous: an interrupt that has not
ended the turn within it has failed. `InterruptTurn` keeps the full
terminal-evidence wait and every existing fail-closed rule.

### Steer — resolves on acceptance, not termination

A steer's effect is *the additional input entered the live turn*. That is what
the operation claims, and it is provable without waiting for the turn to end.

`SteerTurn` resolves on the first of:

- **a correlated terminal event for the target turn, within a short acceptance
  window** — the turn ended right after the steer. This is reported exactly as
  before: `completed` is a success, `failed`/unknown is `ErrTurnControlTurnFailed`,
  an abort is an abort. A turn that dies on the steer did not accept it.
- **the acceptance window elapsing with the target turn still live** — success,
  with `Kind = TurnStillRunning`. The claim being made is precise: *the
  app-server accepted the steer for this thread and turn, and that turn had not
  terminated when we stopped looking.*

"Still live" is checked, not assumed: the context is not cancelled, the process
has not exited, and the control mirror still names the target turn (it is
retired the instant any terminal path fires). If any of those fail, the steer
fails closed exactly as before.

### What this does and does not preserve

It preserves the B1 guarantee that started all this — **an ack alone is never
success**. A bare ack from an app-server whose process then dies, or whose turn
terminates in failure, or that names a turn we are not steering, still fails.
What changed is that "the turn is still running" is now recognised as the
*positive* answer for steer rather than as absence of an answer.

It gives up one thing: a steer whose input the app-server acked and then
silently dropped, on a turn that keeps running for unrelated reasons, reads as
success. Codex emits no "input accepted" notification, so no available evidence
distinguishes that case, and the previous behaviour did not distinguish it
either — it merely failed *everything* after five minutes, including every
healthy steer. Trading an indiscriminate false failure for a narrow, documented
false success is the right side of that trade, because the false failure is the
one that causes double-injection.

## Consequences for the acceptance suite

`TestCodexControlFailsWhenTerminalEvidenceNeverArrives` splits by operation:
interrupt keeps the fail-closed assertion; steer now asserts the
`TurnStillRunning` success and the identity of the evidence. Tests that fed a
foreign turn's or thread's terminal and asserted "the call failed" now assert
the stronger and still-correct thing: **the foreign evidence was not credited**
— the returned evidence names our thread and our turn.

package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Shared turn-control machinery.
//
// The effect contract — an acknowledgement is never success, only a correlated
// terminal is — is provider-neutral, so it is implemented ONCE here and used by
// every backend that supports turn control. What stays provider-specific is
// only the wire: how a control request is sent, and how that provider's
// terminal frame maps onto TurnTerminalKind.
//
// Keeping the waiter shared is what makes "one abstraction over many providers"
// a fact rather than a claim: a caller cannot tell which backend it steered,
// because the fail-closed reasoning is literally the same code.

// controlEvidenceTimeout bounds how long a control call waits for the provider
// to report that the controlled turn terminated. It is generous: steering does
// not end a turn — the agent keeps working with the new input and may run for a
// while — so this is sized for "the turn is still going" rather than round-trip
// latency. Exceeding it is not an RPC timeout but a statement that we cannot
// prove the control action took effect.
const controlEvidenceTimeout = 5 * time.Minute

// controlEvidenceBuffer is the per-waiter queue depth. Terminal events for
// OTHER turns (providers multiplex subagent threads, and a thread can move on
// to a later turn) must not evict the one event a waiter needs, so the queue
// holds several rather than one.
const controlEvidenceBuffer = 8

// steerAcceptanceWindow bounds how long a STEER looks for the target turn to
// terminate before concluding that it did not — which for a steer is the
// success case, not a failure. Short on purpose: a steered turn is expected to
// keep working, so this only has to be long enough to catch a turn that dies ON
// the steer (a rejection wearing a success's clothes).
//
// Interrupt keeps the full terminal wait, because an interrupt's claim IS that
// the turn ended. See docs/design/turn-control-effect-contract.md.
const steerAcceptanceWindow = 2 * time.Second

// controlHost is everything the shared waiters need from a backend. It is
// deliberately tiny: four questions, none of them provider-shaped.
type controlHost interface {
	// controlProcessDone closes when the agent process has exited.
	controlProcessDone() <-chan struct{}
	// controlProcessErr reports why the process exited, or nil.
	controlProcessErr() error
	// controlTurnFailureDetail is the provider's own description of why the
	// turn failed, used to describe ErrTurnControlTurnFailed. Empty is fine —
	// the raw terminal status is the fallback.
	controlTurnFailureDetail() string
	// controlLiveTurnID is the turn this host would accept a control request
	// for right now, or "" when no turn is live. Every terminal path must
	// retire it; a retired mirror is how a steer learns the turn ended even
	// when it never saw correlated evidence.
	controlLiveTurnID() string
	// controlLogger may be nil.
	controlLogger() *slog.Logger
	// controlProvider names the backend for logs and results ("codex").
	controlProvider() string
}

// controlEvidenceBus fans terminal turn events out to the turn-control calls
// waiting on them. It is a broadcast rather than a single channel because more
// than one control call can legitimately be in flight — and because the
// publisher is the stdout reader goroutine, which must never block on a
// consumer: a control caller that has gone away, or is slow, cannot be allowed
// to stall the stream that carries the whole turn.
type controlEvidenceBus struct {
	mu   sync.Mutex
	subs map[int]chan TerminalEvidence
	next int
}

// subscribe registers a waiter and returns its channel plus the func that
// removes it. Callers MUST defer the returned func; without it the bus would
// keep publishing to a channel nobody reads.
func (b *controlEvidenceBus) subscribe() (<-chan TerminalEvidence, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs == nil {
		b.subs = make(map[int]chan TerminalEvidence)
	}
	id := b.next
	b.next++
	ch := make(chan TerminalEvidence, controlEvidenceBuffer)
	b.subs[id] = ch
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs, id)
	}
}

// publish delivers evidence to every current waiter, dropping it for any waiter
// whose queue is full rather than blocking the stdout reader. A dropped event
// degrades to a correlation timeout for that waiter, which fails closed.
func (b *controlEvidenceBus) publish(ev TerminalEvidence) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// controlAtomicString is a race-free view of a string that one goroutine writes
// and another reads. Backends keep their session/turn ids in ordinary fields
// written without a lock (safe because their writers are their only readers);
// turn control adds a reader from outside that set, and this is what it reads.
type controlAtomicString struct{ v atomic.Pointer[string] }

func (a *controlAtomicString) set(s string) { a.v.Store(&s) }

func (a *controlAtomicString) get() string {
	if p := a.v.Load(); p != nil {
		return *p
	}
	return ""
}

// resolveEvidenceTimeout applies the configured override, falling back to the
// package default. Zero means "use the default".
func resolveEvidenceTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return controlEvidenceTimeout
}

// resolveSteerWindow never exceeds the evidence timeout, so a test that
// shortens the timeout to keep the fail-closed paths fast does not accidentally
// leave steer waiting the full default.
func resolveSteerWindow(configured time.Duration) time.Duration {
	if w := resolveEvidenceTimeout(configured); w < steerAcceptanceWindow {
		return w
	}
	return steerAcceptanceWindow
}

// awaitTerminalEvidence blocks until the provider reports that the named turn
// on the named session reached a terminal state, and returns that as evidence.
//
// Every exit that is not a correlated non-failed terminal is an error with zero
// evidence. In particular a terminal event for a DIFFERENT turn or session does
// not end the wait: it proves nothing about our target, and treating it as an
// answer is exactly the bug that would let a subagent's completion be credited
// as our steer taking effect. Such events are counted for diagnostics and the
// wait continues until the target terminates or the timeout expires.
func awaitTerminalEvidence(
	ctx context.Context,
	host controlHost,
	evidence <-chan TerminalEvidence,
	sessionID, turnID string,
	timeout time.Duration,
) (TerminalEvidence, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	uncorrelated := 0

	// classify turns one queued event into a verdict. ok=false means "not about
	// our target, keep waiting".
	classify := func(ev TerminalEvidence) (TerminalEvidence, error, bool) {
		if !ev.correlates(sessionID, turnID) {
			uncorrelated++
			return TerminalEvidence{}, nil, false
		}
		if ev.Kind == TurnTerminalFailed {
			detail := host.controlTurnFailureDetail()
			if detail == "" {
				detail = ev.Status
			}
			return TerminalEvidence{}, fmt.Errorf("%w: session %s turn %s: %s",
				ErrTurnControlTurnFailed, sessionID, turnID, detail), true
		}
		return ev, nil, true
	}

	// drain empties whatever is already queued before we honour a competing
	// ready case. Go's select picks uniformly at random among ready cases, so
	// without this a turn that really did terminate loses a coin flip to
	// processDone/ctx/timer — the publisher buffers the evidence and then drives
	// the lifecycle straight into process exit, leaving both ready.
	drain := func() (TerminalEvidence, error, bool) {
		for {
			select {
			case ev := <-evidence:
				if res, err, done := classify(ev); done {
					return res, err, true
				}
			default:
				return TerminalEvidence{}, nil, false
			}
		}
	}

	processDone := host.controlProcessDone()

	for {
		select {
		case ev := <-evidence:
			if res, err, done := classify(ev); done {
				return res, err
			}

		case <-processDone:
			if res, err, done := drain(); done {
				return res, err
			}
			// The process died instead of the turn terminating. Process exit —
			// including a SIGTERM or SIGKILL that produced it — is NOT evidence
			// that the control action took effect, so it fails closed here
			// rather than being credited as an interrupt that worked.
			err := host.controlProcessErr()
			if err == nil {
				err = errAgentProcessExited
			}
			return TerminalEvidence{}, fmt.Errorf(
				"%w: %s process exited before session %s turn %s produced terminal evidence: %w",
				ErrTurnControlUncorrelated, host.controlProvider(), sessionID, turnID, err)

		case <-ctx.Done():
			if res, err, done := drain(); done {
				return res, err
			}
			return TerminalEvidence{}, fmt.Errorf(
				"%w: waiting for session %s turn %s terminal evidence: %w",
				ErrTurnControlUncorrelated, sessionID, turnID, ctx.Err())

		case <-timer.C:
			if res, err, done := drain(); done {
				return res, err
			}
			return TerminalEvidence{}, fmt.Errorf(
				"%w: no terminal event for session %s turn %s within %s (%d unrelated terminal events seen)",
				ErrTurnControlUncorrelated, sessionID, turnID, timeout, uncorrelated)
		}
	}
}

// awaitSteerEffect implements the STEER half of the effect contract.
//
// A correlated terminal inside the acceptance window is reported exactly as
// awaitTerminalEvidence would: a turn that ended on the steer is not a steer
// that took effect, and a failed turn is still ErrTurnControlTurnFailed.
//
// The window elapsing is the SUCCESS case — but only once "the turn is still
// live" has been CHECKED rather than assumed. The caller's context must not be
// cancelled, the process must not have exited, and the host must still name the
// target turn as live (every terminal path retires it, so a retired id means
// the turn ended and we simply did not see correlated evidence for it). Any of
// those failing keeps the original fail-closed error, which is what preserves
// the guarantee that a bare acknowledgement is never success.
func awaitSteerEffect(
	ctx context.Context,
	host controlHost,
	evidence <-chan TerminalEvidence,
	sessionID, turnID string,
	window time.Duration,
) (TerminalEvidence, error) {
	ev, err := awaitTerminalEvidence(ctx, host, evidence, sessionID, turnID, window)
	if err == nil {
		return ev, nil
	}
	// A correlated failure (ErrTurnControlTurnFailed) is a real verdict about
	// our turn, never "we ran out of time". Only the latter can become success.
	if !errors.Is(err, ErrTurnControlUncorrelated) {
		return TerminalEvidence{}, err
	}
	if ctx.Err() != nil {
		return TerminalEvidence{}, err
	}
	select {
	case <-host.controlProcessDone():
		return TerminalEvidence{}, err
	default:
	}
	if host.controlLiveTurnID() != turnID {
		return TerminalEvidence{}, err
	}
	if lg := host.controlLogger(); lg != nil {
		lg.Info("steer accepted, turn still running",
			"provider", host.controlProvider(),
			"session_id", sessionID,
			"turn_id", turnID,
			"acceptance_window", window.String(),
		)
	}
	return TerminalEvidence{
		ThreadID: sessionID,
		TurnID:   turnID,
		Status:   "running",
		Kind:     TurnStillRunning,
	}, nil
}

// errAgentProcessExited is the provider-neutral fallback when a backend
// reports process exit without its own error. Backends normally supply a
// provider-specific error (errCodexProcessExited); this covers the nil case.
var errAgentProcessExited = errors.New("agent process exited")

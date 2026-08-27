package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Claude Code turn control.
//
// Same contract as Codex — SupportsTurnControl / ControlTurn / SteerTurn /
// InterruptTurn, an acknowledgement is never success, only a correlated
// terminal is — running on the SHARED waiters in turn_control_evidence.go.
// Only the wire differs, and it differs in two ways worth stating up front
// because they are what the shared code had to absorb:
//
//  1. INTERRUPT is a control_request with a request_id, answered by a
//     correlated control_response. That is an acknowledgement, and by the
//     contract it is NOT the answer: the answer is the `result` frame that
//     follows.
//
//  2. STEER has NO acknowledgement at all. It is an ordinary `user` frame
//     written to the same stdin the prompt went down. Nothing replies. So the
//     only available evidence that a steer landed is that the turn is still
//     running afterwards — which is exactly what TurnStillRunning already
//     means, and why awaitSteerEffect checks liveness instead of trusting an
//     ack it will never receive.
//
// Protocol shapes below were captured from a LIVE claude 2.1.247 in
// --input-format stream-json mode, not read off a fixture. See
// docs/design/turn-control-effect-contract.md.

// claudeControlRequestTimeout bounds the interrupt control_request round trip.
// Measured ack latency on a live CLI was ~7ms; this is a fail-closed ceiling,
// not an expectation.
const claudeControlRequestTimeout = 30 * time.Second

// claudeInterruptSubtype is the control_request subtype the CLI accepts to
// cancel an in-flight turn.
const claudeInterruptSubtype = "interrupt"

// claudeTerminalKind maps a Claude `result` frame onto the provider-neutral
// classification.
//
// terminalReason OUTRANKS isError, and that ordering is load-bearing. A
// successful interrupt reports is_error=true with
// terminal_reason="aborted_streaming" — observed live. Classifying on is_error
// alone would report every successful interrupt as a FAILED turn, which is the
// same shape of bug as crediting an unrelated event as success, just inverted.
//
// Everything unrecognized fails CLOSED to TurnTerminalFailed. Only a reason we
// affirmatively recognize as success becomes TurnTerminalCompleted: a spurious
// "failed" costs a caller a retry, whereas a spurious "completed" tells it the
// steer or interrupt took effect when nothing proves that.
func claudeTerminalKind(terminalReason string, isError bool) TurnTerminalKind {
	switch terminalReason {
	case "aborted_streaming", "aborted", "interrupted", "cancelled", "canceled":
		return TurnTerminalAborted
	case "completed":
		if isError {
			// The CLI called it completed but also flagged an error. Trust the
			// error: an errored turn is not proof a control action worked.
			return TurnTerminalFailed
		}
		return TurnTerminalCompleted
	default:
		return TurnTerminalFailed
	}
}

// claudeControlState is the per-execution turn-control state for one Claude
// process. It hangs off the backend for the life of a single Execute, which is
// also the life of exactly one turn: Claude Code runs one prompt to one
// `result` per invocation, so "the live turn" and "this session" coincide.
//
// That is why the session id doubles as the turn id. Codex multiplexes many
// turns on a thread and needs both; Claude does not, and inventing a synthetic
// turn id would add a correlation key the provider never echoes back.
type claudeControlState struct {
	evidence controlEvidenceBus

	// sessionID is set once from the system/init frame. turn id == session id.
	sessionID controlAtomicString
	// liveTurn is the turn a control request may target, retired on terminal.
	liveTurn controlAtomicString

	mu          sync.Mutex
	stdin       interface{ Write([]byte) (int, error) }
	processDone chan struct{}
	processErr  error
	failure     string

	// pending correlates control_response frames back to their request.
	pending map[string]chan claudeControlAck

	logger          *slog.Logger
	evidenceTimeout time.Duration
	nextRequestID   int
}

// claudeControlAck is a decoded control_response.
type claudeControlAck struct {
	Subtype string
	Error   string
}

func newClaudeControlState(logger *slog.Logger) *claudeControlState {
	return &claudeControlState{
		processDone: make(chan struct{}),
		pending:     make(map[string]chan claudeControlAck),
		logger:      logger,
	}
}

// ── controlHost ──

func (s *claudeControlState) controlProcessDone() <-chan struct{} { return s.processDone }

func (s *claudeControlState) controlProcessErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processErr
}

func (s *claudeControlState) controlTurnFailureDetail() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}

func (s *claudeControlState) controlLiveTurnID() string { return s.liveTurn.get() }
func (s *claudeControlState) controlProvider() string   { return "claude" }
func (s *claudeControlState) controlLogger() *slog.Logger {
	return s.logger
}

var _ controlHost = (*claudeControlState)(nil)

// ── lifecycle, driven by the stdout reader ──

func (s *claudeControlState) attachStdin(w interface{ Write([]byte) (int, error) }) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stdin = w
}

// beginTurn records the session id from system/init and arms turn control.
func (s *claudeControlState) beginTurn(sessionID string) {
	if sessionID == "" {
		return
	}
	s.sessionID.set(sessionID)
	s.liveTurn.set(sessionID)
}

// setFailureDetail records the provider's own description of a failure so a
// woken waiter can report it rather than a bare status. MUST be called before
// publishTerminal for the same frame — the publish wakes the waiter, and
// recording afterwards is a race that degrades a real reason to a status
// string.
func (s *claudeControlState) setFailureDetail(detail string) {
	if detail == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure == "" {
		s.failure = detail
	}
}

// publishTerminal retires the live turn and publishes correlated evidence.
//
// ONLY the `result` frame may call this. Claude emits a
// `user: "[Request interrupted by user]"` frame BEFORE the result on an
// interrupt — observed live — and treating that as terminal would preempt the
// real verdict, which is precisely the bug fixed for Codex in #8. A `user`
// frame is turn CONTENT, not a turn verdict.
func (s *claudeControlState) publishTerminal(terminalReason string, isError bool) {
	sessionID := s.sessionID.get()
	turnID := s.liveTurn.get()
	if sessionID == "" || turnID == "" {
		return
	}
	s.liveTurn.set("")
	status := terminalReason
	if status == "" {
		status = "unknown"
	}
	s.evidence.publish(TerminalEvidence{
		ThreadID: sessionID,
		TurnID:   turnID,
		Status:   status,
		Kind:     claudeTerminalKind(terminalReason, isError),
	})
}

func (s *claudeControlState) markProcessExited(err error) {
	s.mu.Lock()
	if s.processErr == nil {
		s.processErr = err
		close(s.processDone)
	}
	pending := s.pending
	s.pending = map[string]chan claudeControlAck{}
	s.mu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- claudeControlAck{Subtype: "error", Error: "process exited"}:
		default:
		}
	}
}

// handleControlResponse routes a control_response frame to whoever is waiting
// on its request_id. Unmatched responses are ignored: Claude also answers our
// inbound tool-permission replies on this channel.
func (s *claudeControlState) handleControlResponse(raw json.RawMessage) {
	var frame struct {
		Response struct {
			Subtype   string `json:"subtype"`
			RequestID string `json:"request_id"`
			Error     string `json:"error"`
		} `json:"response"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return
	}
	id := frame.Response.RequestID
	if id == "" {
		return
	}
	s.mu.Lock()
	ch, ok := s.pending[id]
	if ok {
		delete(s.pending, id)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- claudeControlAck{Subtype: frame.Response.Subtype, Error: frame.Response.Error}:
	default:
	}
}

// ── the control surface ──

// controlTurn performs one turn-control operation against a running Claude
// process. It is the func stored in Session.turnControl for every Claude
// session.
func (s *claudeControlState) controlTurn(ctx context.Context, req TurnControlRequest) (TurnControlResult, error) {
	select {
	case <-s.processDone:
		err := s.controlProcessErr()
		if err == nil {
			err = errAgentProcessExited
		}
		return TurnControlResult{}, fmt.Errorf("claude turn control: %w", err)
	default:
	}

	sessionID, turnID := s.sessionID.get(), s.liveTurn.get()
	if sessionID == "" || turnID == "" || turnID != req.TurnID {
		return TurnControlResult{}, ErrTurnControlInactive
	}

	// Subscribe BEFORE sending. The provider can emit the turn's terminal frame
	// as soon as it has processed the request — measured at 10ms after an
	// interrupt ack — so a subscription taken afterwards could miss the very
	// event it exists to wait for.
	evidence, unsubscribe := s.evidence.subscribe()
	defer unsubscribe()

	switch req.Op {
	case TurnControlInterrupt:
		if err := s.sendInterrupt(ctx, sessionID, turnID); err != nil {
			return TurnControlResult{}, err
		}
	case TurnControlSteer:
		if err := s.sendSteer(req.Input); err != nil {
			return TurnControlResult{}, fmt.Errorf("%w: claude steer: %w", ErrTurnControlRejected, err)
		}
	default:
		// Unreachable through Session.ControlTurn, which validates first.
		return TurnControlResult{}, fmt.Errorf("%w: claude cannot perform %q", ErrTurnControlInvalid, req.Op)
	}

	var (
		terminal TerminalEvidence
		err      error
	)
	if req.Op == TurnControlSteer {
		terminal, err = awaitSteerEffect(ctx, s, evidence, sessionID, turnID, resolveSteerWindow(s.evidenceTimeout))
	} else {
		terminal, err = awaitTerminalEvidence(ctx, s, evidence, sessionID, turnID, resolveEvidenceTimeout(s.evidenceTimeout))
	}
	if err != nil {
		return TurnControlResult{}, err
	}
	if s.logger != nil {
		s.logger.Info("claude turn control correlated",
			"op", string(req.Op),
			"session_id", terminal.ThreadID,
			"turn_id", terminal.TurnID,
			"status", terminal.Status,
			"kind", string(terminal.Kind),
		)
	}
	return TurnControlResult{
		Provider:         "claude",
		SessionID:        sessionID,
		TurnID:           turnID,
		Op:               req.Op,
		TerminalEvidence: terminal,
	}, nil
}

// sendInterrupt writes a control_request and waits for its correlated
// control_response. The ack proves the CLI accepted the request; it does NOT
// prove the turn ended, which is why the caller still waits for terminal
// evidence afterwards.
func (s *claudeControlState) sendInterrupt(ctx context.Context, sessionID, turnID string) error {
	s.mu.Lock()
	s.nextRequestID++
	id := fmt.Sprintf("multica-interrupt-%d", s.nextRequestID)
	ackCh := make(chan claudeControlAck, 1)
	s.pending[id] = ackCh
	w := s.stdin
	s.mu.Unlock()

	if w == nil {
		return fmt.Errorf("%w: claude stdin unavailable", ErrTurnControlRejected)
	}

	frame := map[string]any{
		"type":       "control_request",
		"request_id": id,
		"request":    map[string]any{"subtype": claudeInterruptSubtype},
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return fmt.Errorf("%w: claude interrupt write: %w", ErrTurnControlRejected, err)
	}
	if s.logger != nil {
		s.logger.Info("claude turn control sent",
			"op", "interrupt", "request_id", id, "session_id", sessionID, "turn_id", turnID)
	}

	timeout := claudeControlRequestTimeout
	if s.evidenceTimeout > 0 && s.evidenceTimeout < timeout {
		timeout = s.evidenceTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case ack := <-ackCh:
		if ack.Subtype != "success" {
			detail := ack.Error
			if detail == "" {
				detail = ack.Subtype
			}
			return fmt.Errorf("%w: claude interrupt: %s", ErrTurnControlRejected, detail)
		}
		return nil
	case <-s.processDone:
		err := s.controlProcessErr()
		if err == nil {
			err = errAgentProcessExited
		}
		return fmt.Errorf("claude interrupt: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("%w: claude interrupt: %w", ErrTurnControlRejected, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("%w: claude interrupt: no control_response within %s", ErrTurnControlRejected, timeout)
	}
}

// sendSteer writes an ordinary user frame to the running turn's stdin.
//
// There is no acknowledgement to wait for — the CLI does not answer a user
// frame — so a successful write is the whole of the outbound half. Whether the
// steer took effect is decided by awaitSteerEffect, which requires the turn to
// still be verifiably live.
func (s *claudeControlState) sendSteer(input string) error {
	s.mu.Lock()
	w := s.stdin
	s.mu.Unlock()
	if w == nil {
		return fmt.Errorf("claude stdin unavailable")
	}
	data, err := buildClaudeInput(input)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("claude steer write: %w", err)
	}
	if s.logger != nil {
		s.logger.Info("claude turn control sent", "op", "steer", "turn_id", s.liveTurn.get())
	}
	return nil
}

package agent

import (
	"context"
	"errors"
	"testing"
)

func TestSessionControlRejectsMalformedRequestBeforeProviderIO(t *testing.T) {
	called := false
	session := &Session{control: func(_ context.Context, _ ControlRequest) (ControlResult, error) {
		called = true
		return ControlResult{Accepted: true}, nil
	}}

	_, err := session.Control(context.Background(), ControlRequest{Operation: ControlInterrupt})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if called {
		t.Fatal("provider control invoked for malformed request")
	}
}

func TestSessionControlUnsupportedProviderFailsClosed(t *testing.T) {
	_, err := (&Session{}).Control(context.Background(), ControlRequest{
		RequestID: "request-1",
		Operation: ControlInterrupt,
	})
	if !errors.Is(err, ErrControlUnsupported) {
		t.Fatalf("error = %v, want unsupported", err)
	}
}

func TestSessionControlInactiveProviderFailsClosed(t *testing.T) {
	session := &Session{control: func(_ context.Context, _ ControlRequest) (ControlResult, error) {
		return ControlResult{}, ErrControlInactive
	}}
	result, err := session.Control(context.Background(), ControlRequest{
		RequestID:                 "request-2",
		Operation:                 ControlInterrupt,
		ExpectedProviderSessionID: "session-1",
		ExpectedProviderTurnID:    "turn-1",
	})
	if !errors.Is(err, ErrControlInactive) {
		t.Fatalf("error = %v, want inactive", err)
	}
	if result.Accepted {
		t.Fatalf("inactive provider returned accepted result: %+v", result)
	}
}

func TestSessionControlCachesFailureWithoutResending(t *testing.T) {
	providerErr := errors.New("provider evidence unavailable")
	calls := 0
	session := &Session{control: func(_ context.Context, _ ControlRequest) (ControlResult, error) {
		calls++
		return ControlResult{Provider: "codex"}, providerErr
	}}
	request := ControlRequest{
		RequestID:                 "request-failure-cache",
		Operation:                 ControlInterrupt,
		ExpectedProviderSessionID: "thread-1",
		ExpectedProviderTurnID:    "turn-1",
	}

	first, firstErr := session.Control(context.Background(), request)
	second, secondErr := session.Control(context.Background(), request)
	if !errors.Is(firstErr, providerErr) || !errors.Is(secondErr, providerErr) {
		t.Fatalf("firstErr=%v secondErr=%v", firstErr, secondErr)
	}
	if first != second {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if calls != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
}

func TestSessionControlRejectsRequestIDPayloadConflict(t *testing.T) {
	calls := 0
	session := &Session{control: func(_ context.Context, _ ControlRequest) (ControlResult, error) {
		calls++
		return ControlResult{Provider: "codex", Accepted: true}, nil
	}}
	first := ControlRequest{
		RequestID:                 "request-conflict",
		Operation:                 ControlCheckpoint,
		Instruction:               "checkpoint A",
		ExpectedProviderSessionID: "thread-1",
		ExpectedProviderTurnID:    "turn-1",
	}
	second := first
	second.Instruction = "checkpoint B"

	if _, err := session.Control(context.Background(), first); err != nil {
		t.Fatalf("first Control: %v", err)
	}
	if _, err := session.Control(context.Background(), second); !errors.Is(err, ErrControlRequestConflict) {
		t.Fatalf("second error = %v, want request conflict", err)
	}
	if calls != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
}

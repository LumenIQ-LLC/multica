package agent

import (
	"context"
	"errors"
	"testing"
)

func TestSessionControlRejectsMalformedRequestBeforeProviderIO(t *testing.T) {
	called := false
	s := &Session{control: func(context.Context, ControlRequest) (ControlResult, error) {
		called = true
		return ControlResult{}, nil
	}}
	for _, request := range []ControlRequest{
		{}, {RequestID: "id", Operation: ControlCheckpoint}, {RequestID: "id", Operation: ControlCancelAndRedirect}, {RequestID: "id", Operation: "unknown"},
	} {
		if _, err := s.Control(context.Background(), request); err == nil {
			t.Fatalf("Control(%+v) succeeded", request)
		}
	}
	if called {
		t.Fatal("provider handler was called for malformed request")
	}
}

func TestSessionControlUnsupportedAndInactive(t *testing.T) {
	_, err := (&Session{}).Control(context.Background(), ControlRequest{RequestID: "id", Operation: ControlInterrupt})
	if !errors.Is(err, ErrControlUnsupported) {
		t.Fatalf("error = %v, want unsupported", err)
	}
	inactive := &Session{control: func(context.Context, ControlRequest) (ControlResult, error) {
		return ControlResult{}, ErrControlInactive
	}}
	_, err = inactive.Control(context.Background(), ControlRequest{RequestID: "id", Operation: ControlInterrupt})
	if !errors.Is(err, ErrControlInactive) {
		t.Fatalf("error = %v, want inactive", err)
	}
}

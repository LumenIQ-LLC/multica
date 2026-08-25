package agent

import "encoding/json"

// Wire parameter shapes for the Codex app-server's turn-control RPCs.
//
// The two RPCs name the target turn DIFFERENTLY, and getting it wrong is a
// silent-looking failure rather than a compile error, so each shape is pinned
// here with its own type instead of being hand-built at the call site:
//
//   - turn/interrupt takes `turnId`.
//   - turn/steer takes `expectedTurnId`, and REJECTS `turnId`. The name is the
//     contract: steer is only meaningful against the turn the caller expects to
//     still be running, so the app-server makes the caller state that
//     expectation and refuses the request if the thread has moved on.

// codexTurnInterruptParams is the turn/interrupt wire shape.
type codexTurnInterruptParams struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
}

// codexTurnSteerParams is the turn/steer wire shape. It keeps TurnID as the
// internal field name so every turn-addressed param struct in this package
// reads the same way, and MarshalJSON performs the rename to `expectedTurnId`
// at the boundary — that way no caller has to remember which of the two RPCs
// renames the field, and the rename cannot be forgotten at one call site.
type codexTurnSteerParams struct {
	ThreadID string
	TurnID   string
	Input    []map[string]any
}

func (p codexTurnSteerParams) MarshalJSON() ([]byte, error) {
	type steerWireParams struct {
		ThreadID       string           `json:"threadId"`
		ExpectedTurnID string           `json:"expectedTurnId"`
		Input          []map[string]any `json:"input"`
	}
	return json.Marshal(steerWireParams{
		ThreadID:       p.ThreadID,
		ExpectedTurnID: p.TurnID,
		Input:          p.Input,
	})
}

// codexTurnControlInput wraps steer text in the single text block the
// app-server accepts as turn input, mirroring what codexTurnInput builds for
// turn/start so a steered turn receives its input in the same form as the
// original prompt.
func codexTurnControlInput(text string) []map[string]any {
	return []map[string]any{{"type": "text", "text": text}}
}

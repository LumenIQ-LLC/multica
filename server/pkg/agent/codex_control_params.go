package agent

import "encoding/json"

// MarshalJSON keeps the internal active-turn field name while matching the
// Codex app-server v2 wire contract. turn/steer requires expectedTurnId;
// turnId is the separate turn/interrupt parameter and is rejected for steer.
func (p codexTurnSteerParams) MarshalJSON() ([]byte, error) {
	type steerWireParams struct {
		ThreadID       string                `json:"threadId"`
		ExpectedTurnID string                `json:"expectedTurnId"`
		Input          []codexTurnStartInput `json:"input"`
	}
	return json.Marshal(steerWireParams{
		ThreadID:       p.ThreadID,
		ExpectedTurnID: p.TurnID,
		Input:          p.Input,
	})
}

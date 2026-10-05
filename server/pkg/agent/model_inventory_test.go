package agent

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestClaudeObservedModelInventory(t *testing.T) {
	observed := claudeObservedModelInventory(claudeSDKMessage{ModelUsage: map[string]claudeResultModelUsage{"claude-opus": {InputTokens: 1}, "claude-sonnet": {OutputTokens: 2}}})
	if !observed.Complete || !reflect.DeepEqual(observed.Models, []string{"claude-opus", "claude-sonnet"}) {
		t.Fatalf("lost switch: %+v", observed)
	}
	for _, msg := range []claudeSDKMessage{{Model: "configured-only", Usage: &claudeUsage{InputTokens: 1}}, {ModelUsage: map[string]claudeResultModelUsage{"unknown": {InputTokens: 1}}}} {
		if claudeObservedModelInventory(msg).Complete {
			t.Fatal("unobserved/incomplete result qualified")
		}
	}
}
func TestFailedClaudeAttemptRetainsObservedInventory(t *testing.T) {
	if !claudeObservedModelInventory(claudeSDKMessage{IsError: true, ModelUsage: map[string]claudeResultModelUsage{"claude-opus": {InputTokens: 1}}}).Complete {
		t.Fatal("failed task lost observed author inventory")
	}
}
func TestCodexObservedModelInventory(t *testing.T) {
	cases := []struct {
		name, body string
		complete   bool
		models     []string
	}{
		{"switches", `{"type":"event_msg","payload":{"type":"token_count","info":{"model":"gpt-a","total_token_usage":{"input_tokens":1}}}}
{"type":"event_msg","payload":{"type":"token_count","info":{"model":"gpt-b","total_token_usage":{"input_tokens":2}}}}
`, true, []string{"gpt-a", "gpt-b"}},
		{"no fallback", `{"type":"turn_context","payload":{"model":"gpt-default"}}
{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1}}}}
`, false, []string{}},
		{"partial", `{"type":"event_msg","payload":{"type":"token_count","info":{"model":"gpt-a","total_token_usage":{"input_tokens":1}}}}
{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":2}}}}
`, false, []string{"gpt-a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "events.jsonl")
			if err := os.WriteFile(p, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			got := parseCodexSessionFileSince(p, time.Time{}, false)
			if got == nil || got.inventory == nil || got.inventory.Complete != tc.complete || !reflect.DeepEqual(got.inventory.Models, tc.models) {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestCodexObservedTurnCoverage(t *testing.T) {
	s := &codexSessionUsage{inventory: &ModelInventory{Complete: true, Models: []string{"gpt-observed"}}, observedTurnIDs: map[string]bool{"one": true}}
	if s.qualifiedInventory([]string{"one", "two"}).Complete {
		t.Fatal("one turn hid missing second turn inventory")
	}
	if s.qualifiedInventory(nil).Complete {
		t.Fatal("missing terminal turn proof qualified")
	}
	if !s.qualifiedInventory([]string{"one"}).Complete {
		t.Fatal("exact observed turn did not qualify")
	}
	s.observedTurnIDs = nil
	if s.qualifiedInventory([]string{"one"}).Complete {
		t.Fatal("legacy telemetry without turn identity qualified")
	}
}

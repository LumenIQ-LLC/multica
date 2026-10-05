package agent

import (
	"sort"
	"strings"
)

// ModelInventory is observed provider telemetry, separate from Usage's billing
// fallback model. Nil or Complete=false is never evidence of independence.
// Models includes every observed model, including switches within one execution.
type ModelInventory struct {
	Complete bool
	Models   []string
}

type modelInventoryAccumulator struct {
	models     map[string]bool
	incomplete bool
}

func (a *modelInventoryAccumulator) observe(model string) {
	model = strings.TrimSpace(model)
	switch strings.ToLower(model) {
	case "", "unknown", "auto", "default", "inherit":
		a.incomplete = true
		return
	}
	if a.models == nil {
		a.models = make(map[string]bool)
	}
	a.models[model] = true
}
func (a *modelInventoryAccumulator) result(complete bool) *ModelInventory {
	models := make([]string, 0, len(a.models))
	for model := range a.models {
		models = append(models, model)
	}
	sort.Strings(models)
	return &ModelInventory{Complete: complete && !a.incomplete && len(models) > 0, Models: models}
}

// Only the SDK's per-model result inventory qualifies Claude; the legacy
// fallbackModel and aggregate Usage fields intentionally cannot qualify it.
func claudeObservedModelInventory(msg claudeSDKMessage) *ModelInventory {
	var inventory modelInventoryAccumulator
	for model, usage := range msg.ModelUsage {
		if !claudeUsageHasTokens(usage.InputTokens, usage.OutputTokens, usage.CacheReadInputTokens, usage.CacheCreationInputTokens) {
			continue
		}
		inventory.observe(model)
	}
	return inventory.result(len(msg.ModelUsage) > 0)
}

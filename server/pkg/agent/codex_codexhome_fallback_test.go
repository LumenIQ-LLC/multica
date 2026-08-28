package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A real production failure, reproduced.
//
// An embedder (LumenIQ Mission Control) sets CODEX_HOME with os.Setenv AND via a
// container ENV, so it is unambiguously present in os.Environ(). Its first live
// Cloud Run execution still died with:
//
//	codex: mcp_config is set but CODEX_HOME env var is not configured
//
// because the guard consulted only cfg.Env, which that embedder never populates.
// The value was in the process the whole time. A guard that refuses on the
// absence of a value it never looked for reports a configuration error that does
// not exist.
//
// NOT t.Parallel: these mutate process env via t.Setenv.
func TestCodexHomeFallsBackToProcessEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	codexHome := t.TempDir()
	t.Setenv("CODEX_HOME", codexHome)

	backend, err := New("codex", Config{
		ExecutablePath: writeFakeCodexAppServer(t, "exit 0\n"),
		Logger:         slog.Default(),
		Env:            map[string]string{}, // embedder never populates cfg.Env
	})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = backend.Execute(ctx, "prompt", ExecOptions{
		Timeout:   2 * time.Second,
		McpConfig: json.RawMessage(`{"mcpServers":{"fetch":{"command":"uvx"}}}`),
	})

	// The fake app-server exits immediately, so Execute still fails -- but it
	// must NOT fail on the CODEX_HOME guard, which is the whole point.
	if err != nil && strings.Contains(err.Error(), "CODEX_HOME env var is not configured") {
		t.Fatalf("guard fired despite CODEX_HOME being in the process env: %v", err)
	}

	// And the managed config must actually have been materialised there.
	if _, statErr := os.Stat(filepath.Join(codexHome, "config.toml")); statErr != nil {
		t.Fatalf("managed config.toml not written to the process-env CODEX_HOME: %v", statErr)
	}
}

// cfg.Env must still WIN. The daemon giving each task an isolated CODEX_HOME is
// the point of the per-task config (MUL-4424); an inherited process value must
// never silently override that isolation. The fallback fills a gap, it does not
// take precedence.
func TestCodexHomeConfiguredEnvWinsOverProcessEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	configured := t.TempDir()
	inherited := t.TempDir()
	t.Setenv("CODEX_HOME", inherited)

	backend, err := New("codex", Config{
		ExecutablePath: writeFakeCodexAppServer(t, "exit 0\n"),
		Logger:         slog.Default(),
		Env:            map[string]string{"CODEX_HOME": configured},
	})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = backend.Execute(ctx, "prompt", ExecOptions{
		Timeout:   2 * time.Second,
		McpConfig: json.RawMessage(`{"mcpServers":{"fetch":{"command":"uvx"}}}`),
	})

	if _, statErr := os.Stat(filepath.Join(configured, "config.toml")); statErr != nil {
		t.Fatalf("configured cfg.Env CODEX_HOME must win: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(inherited, "config.toml")); statErr == nil {
		t.Fatal("inherited process CODEX_HOME must NOT be used when cfg.Env sets one")
	}
}

// The fail-closed guard is preserved when the value is genuinely absent
// everywhere. This is the pre-existing contract and it must not regress.
func TestCodexHomeGuardStillFiresWhenTrulyUnset(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	t.Setenv("CODEX_HOME", "")

	backend, err := New("codex", Config{
		ExecutablePath: writeFakeCodexAppServer(t, "exit 0\n"),
		Logger:         slog.Default(),
		Env:            map[string]string{},
	})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = backend.Execute(ctx, "prompt", ExecOptions{
		Timeout:   2 * time.Second,
		McpConfig: json.RawMessage(`{"mcpServers":{"fetch":{"command":"uvx"}}}`),
	})
	if err == nil || !strings.Contains(err.Error(), "CODEX_HOME") {
		t.Fatalf("expected the fail-closed guard to fire when CODEX_HOME is unset everywhere, got %v", err)
	}
}

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent/agenterr"
	"github.com/multica-ai/multica/server/pkg/agent/agentfailure"
	"github.com/multica-ai/multica/server/pkg/i18n"
	"github.com/multica-ai/multica/server/pkg/masking"
	"github.com/multica-ai/multica/server/pkg/security"
	"github.com/multica-ai/multica/server/pkg/types"
)

// Status represents the terminal status of an agent execution.
type Status string

const (
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusTimedOut  Status = "timed_out"
	StatusAborted   Status = "aborted"
)

// Message represents a streaming message from the agent.
type Message struct {
	Type      string         `json:"type"` // system, assistant, user, result
	Subtype   string         `json:"subtype,omitempty"`
	Content   string         `json:"content,omitempty"`
	ToolName  string         `json:"toolName,omitempty"`
	ToolID    string         `json:"toolId,omitempty"`
	Input     map[string]any `json:"input,omitempty"`
	Output    string         `json:"output,omitempty"`
	IsError   bool           `json:"isError,omitempty"`
	Raw       string         `json:"raw,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

// Result represents the final execution result.
type Result struct {
	Status    Status `json:"status"`
	Error     string `json:"error,omitempty"`
	SessionID string `json:"sessionId,omitempty"`

	// Usage aggregates token counts reported by the CLI. Keys include
	// "input_tokens", "output_tokens", "cache_read_input_tokens", etc.
	Usage map[string]int64 `json:"usage,omitempty"`

	// CostUSDTicks stores the CLI-reported USD cost in 1e-10 dollar ticks.
	// This intentionally avoids float64 at the agent↔service boundary.
	CostUSDTicks int64 `json:"costUsdTicks,omitempty"`

	// ResumeRejected is set when a resume was requested but the CLI rejected
	// the session ID (expired, deleted, or invalid). The caller should start a
	// fresh session instead of silently continuing without context.
	ResumeRejected bool `json:"resumeRejected,omitempty"`
}

type ControlOperation string

const (
	ControlCheckpoint        ControlOperation = "checkpoint"
	ControlCancelAndRedirect ControlOperation = "cancel_and_redirect"
	ControlInterrupt         ControlOperation = "interrupt"
)

var (
	ErrControlUnsupported     = errors.New("agent control unsupported")
	ErrControlInactive        = errors.New("agent control session inactive")
	ErrControlRequestConflict = errors.New("agent control request id reused with different payload")
)

// ControlRequest is the provider-neutral cooperative control request. The
// expected provider identity fields are mandatory compare-and-swap guards for
// adapters that expose an active provider turn. Providers without native turn
// identity may leave ExpectedProviderTurnID empty and must document their
// boundary evidence in ControlResult.
type ControlRequest struct {
	RequestID                 string           `json:"requestId"`
	Operation                 ControlOperation `json:"operation"`
	Instruction               string           `json:"instruction,omitempty"`
	ExpectedProviderSessionID string           `json:"expectedProviderSessionId"`
	ExpectedProviderTurnID    string           `json:"expectedProviderTurnId,omitempty"`
}

// ControlResult reports only provider-native evidence. Process exit, local
// signals, and caller-side resume heuristics are not valid acceptance evidence.
type ControlResult struct {
	Provider          string    `json:"provider"`
	ProviderSessionID string    `json:"providerSessionId"`
	ProviderTurnID    string    `json:"providerTurnId,omitempty"`
	Accepted          bool      `json:"accepted"`
	BoundaryKind      string    `json:"boundaryKind,omitempty"`
	TerminalEvidence string    `json:"terminalEvidence,omitempty"`
	ResumableEvidence string   `json:"resumableEvidence,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

func (r ControlRequest) validate() error {
	if strings.TrimSpace(r.RequestID) == "" {
		return fmt.Errorf("control request id is required")
	}
	switch r.Operation {
	case ControlCheckpoint, ControlCancelAndRedirect:
		if strings.TrimSpace(r.Instruction) == "" {
			return fmt.Errorf("control instruction is required for %s", r.Operation)
		}
	case ControlInterrupt:
	default:
		return fmt.Errorf("unsupported control operation %q", r.Operation)
	}
	return nil
}

type sessionControlFunc func(context.Context, ControlRequest) (ControlResult, error)

type sessionControlCacheEntry struct {
	request ControlRequest
	result  ControlResult
	err     error
}

type Session struct {
	Messages <-chan Message
	Result   <-chan Result
	control  sessionControlFunc

	controlMu    sync.Mutex
	controlCache map[string]sessionControlCacheEntry
}

// Control asks the active provider session to cooperatively checkpoint,
// redirect, or interrupt. Unsupported and inactive sessions fail closed.
func (s *Session) Control(ctx context.Context, request ControlRequest) (ControlResult, error) {
	if err := request.validate(); err != nil {
		return ControlResult{}, err
	}
	if s == nil || s.control == nil {
		return ControlResult{}, ErrControlUnsupported
	}

	// A control request ID is a durable idempotency key for this live session.
	// Serialize first execution and cache both success and failure so a caller
	// cannot ambiguously resend provider control after losing the first reply.
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if cached, ok := s.controlCache[request.RequestID]; ok {
		if cached.request != request {
			return ControlResult{}, ErrControlRequestConflict
		}
		return cached.result, cached.err
	}

	result, err := s.control(ctx, request)
	if s.controlCache == nil {
		s.controlCache = make(map[string]sessionControlCacheEntry)
	}
	s.controlCache[request.RequestID] = sessionControlCacheEntry{
		request: request,
		result:  result,
		err:     err,
	}
	return result, err
}

// ExecOptions configures the agent execution.
type ExecOptions struct {
	Provider        types.AgentProvider
	Prompt          string
	RepoRoot        string
	Model           string
	MaxTurns        int
	Timeout         time.Duration
	StallTimeout    time.Duration
	PermissionMode  string
	AllowedTools    []string
	DisallowedTools []string
	SystemPrompt    string

	// AppendSystemPrompt is appended to the provider's default system prompt.
	// Unlike SystemPrompt (which replaces the default), this preserves the
	// provider's built-in instructions while adding task-specific guidance.
	AppendSystemPrompt string

	// SettingSources controls which Claude Code settings are loaded.
	// Values: "user" (~/.claude/settings.json), "project" (.claude/settings.json),
	// "local" (.claude/settings.local.json). When nil, defaults to ["project"].
	// Set empty slice to load no settings.
	SettingSources []string

	// CLAUDE.md source policy. When nil, agent-level defaults apply.
	// Values: "user", "project", "local".
	ClaudeMDSettingSources []string

	// MCP source contract.
	McpMode                types.McpMode
	McpPreset              string
	McpServers             []types.McpServerConfig
	StrictMcpConfig        bool
	AllowRemoteMcp         bool
	AllowUnpinnedRemoteMcp bool
	ClaudeSettingsPath     string
	ClaudeMcpConfigPath    string

	// Plugin directories to load for the execution. Each path is passed to the
	// CLI as --plugin-dir. Paths must exist and be directories.
	PluginDirs []string

	// PersistSession enables provider session storage for later resume.
	PersistSession bool
	// ResumeSessionID is the provider session to continue when non-empty.
	ResumeSessionID string

	// CodexHomeMode controls how CODEX_HOME is selected for Codex executions.
	// Empty defaults to "task", which isolates state per task and prevents
	// credentials/history from leaking across executions. "inherit" is an
	// explicit compatibility escape hatch for trusted environments.
	CodexHomeMode string
	// CodexHomeRoot is the parent directory for task-scoped Codex homes.
	// Empty defaults under MULTICA_HOME, then MULTICA_DATA_DIR, then the OS temp dir.
	CodexHomeRoot string
	// CodexHomeTaskID identifies the task-scoped Codex home.
	// Empty values are auto-generated.
	CodexHomeTaskID string

	// CodexUsesChatGPTAuth controls auth preflight for the Codex CLI.
	// When true, account login state is required (OPENAI_API_KEY-only is rejected).
	// When false, either OPENAI_API_KEY or account login state is accepted.
	CodexUsesChatGPTAuth bool

	// CodexChatGPTAuthFile is a path to a pre-authenticated Codex auth.json.
	// When CodexUsesChatGPTAuth is true, the file is copied into the task-scoped
	// CODEX_HOME before execution. This enables account auth in isolated homes
	// without sharing the entire user CODEX_HOME directory.
	CodexChatGPTAuthFile string

	// CodexRealtimeConversation is required to enable realtime conversation features.
	CodexRealtimeConversation bool
}

// Agent is the interface for executing AI agents.
type Agent interface {
	Execute(ctx context.Context, opts ExecOptions) (*Session, error)
}

// New creates an Agent for the given provider.
func New(provider types.AgentProvider, env map[string]string) (Agent, error) {
	switch provider {
	case types.AgentProviderClaude:
		return NewClaudeAgent(env), nil
	case types.AgentProviderCodex:
		return NewCodexAgent(env), nil
	case types.AgentProviderCursor:
		return NewCursorAgent(env), nil
	case types.AgentProviderGemini:
		return NewGeminiAgent(env), nil
	case types.AgentProviderCopilot:
		return NewCopilotAgent(env), nil
	default:
		return nil, fmt.Errorf("unsupported agent provider: %s", provider)
	}
}

// NewDefault creates the default Agent implementation.
func NewDefault() Agent {
	return NewClaudeAgent(nil)
}

func normalizeOpts(opts *ExecOptions) {
	if opts.MaxTurns <= 0 {
		opts.MaxTurns = 50
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Minute
	}
	if opts.StallTimeout <= 0 {
		opts.StallTimeout = 10 * time.Minute
	}
	if opts.PermissionMode == "" {
		opts.PermissionMode = "bypassPermissions"
	}
	if opts.SettingSources == nil {
		opts.SettingSources = []string{"project"}
	}
	if opts.ClaudeMDSettingSources == nil {
		opts.ClaudeMDSettingSources = []string{"project"}
	}
	if opts.McpMode == "" {
		opts.McpMode = types.McpModeNone
	}
}

func validateExecOptions(opts *ExecOptions) error {
	if opts.RepoRoot == "" {
		return errors.New("repo root is required")
	}
	abs, err := filepath.Abs(opts.RepoRoot)
	if err != nil {
		return fmt.Errorf("resolve repo root: %w", err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("repo root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("repo root is not a directory: %s", abs)
	}
	opts.RepoRoot = abs

	switch opts.McpMode {
	case types.McpModeNone:
		opts.McpPreset = ""
		opts.McpServers = nil
	case types.McpModePreset:
		if opts.McpPreset == "" {
			return errors.New("mcp preset is required when mcpMode is preset")
		}
		opts.McpServers = nil
	case types.McpModeExplicit:
		if len(opts.McpServers) == 0 {
			return errors.New("at least one mcp server is required when mcpMode is explicit")
		}
		opts.McpPreset = ""
	case types.McpModeDisabled:
		opts.McpPreset = ""
		opts.McpServers = nil
	default:
		return fmt.Errorf("invalid mcp mode: %s", opts.McpMode)
	}

	if err := security.ValidateMcpServers(opts.McpServers); err != nil {
		return err
	}
	return nil
}

// prepareMcpConfig resolves env indirection and optionally writes a provider-specific MCP config file.
func prepareMcpConfig(opts ExecOptions) (ExecOptions, func(), error) {
	cleanup := func() {}
	if opts.McpMode != types.McpModeExplicit {
		return opts, cleanup, nil
	}

	servers, err := security.ResolveMcpEnv(opts.McpServers)
	if err != nil {
		return opts, cleanup, err
	}
	opts.McpServers = servers

	switch opts.Provider {
	case types.AgentProviderClaude:
		if opts.ClaudeMcpConfigPath == "" {
			path, err := writeTempMcpConfig(opts.McpServers)
			if err != nil {
				return opts, cleanup, err
			}
			opts.ClaudeMcpConfigPath = path
			cleanup = func() { _ = os.Remove(path) }
		}
	case types.AgentProviderGemini:
		return opts, cleanup, errors.New("explicit MCP mode is not supported by Gemini provider")
	case types.AgentProviderCopilot:
		return opts, cleanup, errors.New("explicit MCP mode is not supported by Copilot provider")
	}

	return opts, cleanup, nil
}

// BaseAgent provides common functionality for agent implementations.
type BaseAgent struct {
	env map[string]string
}

func NewBaseAgent(env map[string]string) *BaseAgent {
	return &BaseAgent{env: env}
}

func (a *BaseAgent) envSlice() []string {
	env := os.Environ()
	for k, v := range a.env {
		env = append(env, k+"="+v)
	}
	return env
}

func (a *BaseAgent) envMap() map[string]string {
	env := make(map[string]string)
	for _, item := range os.Environ() {
		if idx := strings.IndexByte(item, '='); idx >= 0 {
			env[item[:idx]] = item[idx+1:]
		}
	}
	for k, v := range a.env {
		env[k] = v
	}
	return env
}

func sendMessage(ch chan<- Message, msg Message) bool {
	select {
	case ch <- msg:
		return true
	default:
		return false
	}
}

// putMessage delivers a message without dropping it, unless the execution context
// is canceled. This applies backpressure to the provider reader so API callers do
// not silently lose events when channels are temporarily full.
func putMessage(ctx context.Context, ch chan<- Message, msg Message) bool {
	select {
	case ch <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func putResult(ch chan<- Result, result Result) {
	ch <- result
}

func safeCloseMessages(ch chan Message) {
	defer func() { _ = recover() }()
	close(ch)
}

func safeCloseResult(ch chan Result) {
	defer func() { _ = recover() }()
	close(ch)
}

// parseJSONLine attempts to parse a JSON line.
func parseJSONLine(line string, v any) error {
	dec := jsonDecoder(strings.NewReader(line))
	dec.UseNumber()
	return dec.Decode(v)
}

// jsonDecoder is a small seam for tests.
var jsonDecoder = func(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}

// redactForLogs applies shared masking to stderr or provider output that may
// include secrets. Keep this in the agent package so every provider path can
// consistently protect logs.
func redactForLogs(raw string) string {
	return masking.MaskSecrets(raw)
}

var numericValuePattern = regexp.MustCompile(`[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?`)

func parseExactDecimalTicks(value string, scale int64) (int64, bool) {
	value = strings.TrimSpace(value)
	if value == "" || !numericValuePattern.MatchString(value) || numericValuePattern.FindString(value) != value {
		return 0, false
	}

	negative := false
	if value[0] == '-' || value[0] == '+' {
		negative = value[0] == '-'
		value = value[1:]
	}

	exponent := 0
	if idx := strings.IndexAny(value, "eE"); idx >= 0 {
		parsed, err := strconv.Atoi(value[idx+1:])
		if err != nil {
			return 0, false
		}
		exponent = parsed
		value = value[:idx]
	}

	parts := strings.SplitN(value, ".", 2)
	digits := parts[0]
	fractionDigits := 0
	if len(parts) == 2 {
		digits += parts[1]
		fractionDigits = len(parts[1])
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return 0, true
	}

	scaleDigits := len(strconv.FormatInt(scale, 10)) - 1
	shift := exponent - fractionDigits + scaleDigits
	if shift < 0 {
		cut := len(digits) + shift
		if cut <= 0 {
			return 0, true
		}
		digits = digits[:cut]
	} else if shift > 0 {
		digits += strings.Repeat("0", shift)
	}

	parsed, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	if negative {
		parsed = -parsed
	}
	return parsed, true
}

func parseUsageValue(value any) (int64, bool) {
	switch v := value.(type) {
	case json.Number:
		i, err := v.Int64()
		return i, err == nil
	case float64:
		return int64(v), true
	case int:
		return int64(v), true
	case int64:
		return v, true
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func parseCostTicks(value any) (int64, bool) {
	const costScale = int64(10_000_000_000)
	switch v := value.(type) {
	case json.Number:
		return parseExactDecimalTicks(v.String(), costScale)
	case float64:
		return parseExactDecimalTicks(strconv.FormatFloat(v, 'g', -1, 64), costScale)
	case string:
		return parseExactDecimalTicks(v, costScale)
	default:
		return 0, false
	}
}

func mergeUsage(dst map[string]int64, src map[string]any) {
	for key, value := range src {
		if count, ok := parseUsageValue(value); ok {
			dst[key] += count
		}
	}
}

func failureResult(provider types.AgentProvider, err error) Result {
	normalized := agentfailure.Normalize(err)
	status := StatusFailed
	if errors.Is(normalized, agenterr.ErrStall) {
		status = StatusTimedOut
	}
	return Result{
		Status: status,
		Error:  normalized.Error(),
	}
}

func executeWithLifecycle(ctx context.Context, opts ExecOptions, execute func(context.Context, ExecOptions) (*Session, error)) (*Session, error) {
	normalizeOpts(&opts)
	if err := validateExecOptions(&opts); err != nil {
		return nil, err
	}

	prepared, cleanup, err := prepareMcpConfig(opts)
	if err != nil {
		return nil, err
	}

	session, err := execute(ctx, prepared)
	if err != nil {
		cleanup()
		return nil, err
	}

	messages := make(chan Message, 128)
	results := make(chan Result, 1)
	go func() {
		defer safeCloseMessages(messages)
		defer safeCloseResult(results)
		defer cleanup()

		for msg := range session.Messages {
			if !putMessage(ctx, messages, msg) {
				return
			}
		}

		select {
		case result, ok := <-session.Result:
			if !ok {
				putResult(results, Result{Status: StatusFailed, Error: "agent result channel closed unexpectedly"})
				return
			}
			putResult(results, result)
		case <-ctx.Done():
			putResult(results, Result{Status: StatusAborted, Error: ctx.Err().Error()})
		}
	}()

	return &Session{
		Messages: messages,
		Result:   results,
		control:  session.control,
	}, nil
}

func providerLabel(provider types.AgentProvider) string {
	if provider == "" {
		return "agent"
	}
	return string(provider)
}

func logProviderFailure(provider types.AgentProvider, err error) {
	if err == nil {
		return
	}
	log.Printf("[%s] %s", providerLabel(provider), redactForLogs(err.Error()))
}

func providerError(provider types.AgentProvider, err error) error {
	if err == nil {
		return nil
	}
	logProviderFailure(provider, err)
	return fmt.Errorf("%s execution failed: %w", providerLabel(provider), err)
}

func localizeError(err error) string {
	if err == nil {
		return ""
	}
	return i18n.T("agent.error.execution_failed") + ": " + err.Error()
}

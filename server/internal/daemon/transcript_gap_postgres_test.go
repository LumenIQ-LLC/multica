package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/handler"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/agent"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// This opt-in fixture reaches the real daemon message handler and SQL insert.
// It permits the assignment's isolated local database or the exact existing
// GitHub Actions service fixture. No implicit DATABASE_URL fallback is allowed.
func TestTranscriptGapPersistsThroughActualDaemonMessageHandler(t *testing.T) {
	raw := os.Getenv("R25_DATABASE_URL")
	if raw == "" {
		t.Skip("R25_DATABASE_URL not set; actual SQL persistence not exercised")
	}
	if !allowedR25DatabaseURL(raw, os.Getenv("GITHUB_ACTIONS")) {
		t.Fatal("R25_DATABASE_URL must name an explicitly allowed disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	fx := testutil.New(pool, "", "")
	user := fx.Insert(t, "user", testutil.Cols{"name": "R25 transcript fixture", "email": fmt.Sprintf("r25-gap-%d@multica.test", time.Now().UnixNano())})
	workspace := fx.Insert(t, "workspace", testutil.Cols{"name": "R25 transcript fixture", "slug": "r25-" + user, "description": "synthetic", "issue_prefix": "R25"})
	fx.UserID = user
	fx.WorkspaceID = workspace
	runtimeID := fx.Runtime(t, "R25 fixture runtime")
	agentID := fx.Agent(t, "R25 fixture agent", runtimeID)
	issueID := fx.Issue(t, "R25 transcript fixture")
	taskID := fx.Task(t, agentID, testutil.Cols{"runtime_id": runtimeID, "issue_id": issueID, "status": "running"})
	fx.Cleanup(t, "DELETE FROM task_message WHERE task_id=$1", taskID)
	queries := db.New(pool)
	bus := events.New()
	target := &handler.Handler{Queries: queries, TaskService: service.NewTaskService(queries, pool, nil, bus), Bus: bus}
	router := chi.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(middleware.WithDaemonContext(r.Context(), workspace, "r25-fixture-daemon")))
		})
	})
	router.Post("/api/daemon/tasks/{taskId}/messages", target.ReportTaskMessages)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	d := &Daemon{client: NewClient(server.URL), logger: slog.Default()}
	result, _, err := d.executeAndDrain(ctx, sessionBackend{session: overflowSession()}, "fixture", agent.ExecOptions{}, slog.Default(), taskID, "", new(atomic.Int32))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.DroppedMessages != 344 {
		t.Fatalf("unexpected terminal handoff: %+v", result)
	}
	var kind, content, count string
	var complete bool
	var seq, lastSeq int
	err = pool.QueryRow(ctx, "SELECT type, content, input->>'dropped_messages', (input->>'complete')::boolean, seq FROM task_message WHERE task_id=$1 AND input->>'event_type'='transcript.gap'", taskID).Scan(&kind, &content, &count, &complete, &seq)
	if err != nil {
		t.Fatalf("terminal returned before durable gap: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT max(seq) FROM task_message WHERE task_id=$1", taskID).Scan(&lastSeq); err != nil {
		t.Fatal(err)
	}
	if kind != "error" || count != "344" || complete || seq != lastSeq || content == "" {
		t.Fatalf("unrenderable or unordered persisted gap: kind=%s count=%s complete=%v seq=%d/%d", kind, count, complete, seq, lastSeq)
	}
}

func allowedR25DatabaseURL(raw, githubActions string) bool {
	const local = "postgres://core_fixture@127.0.0.1:55439/multica_core_r25?sslmode=disable"
	// .github/workflows/ci.yml backend-tests service fixture, explicitly passed
	// through R25_DATABASE_URL by CI. Exact matching also refuses redirecting DSN
	// options or alternate hosts hidden in query parameters.
	const ci = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	return raw == local || (githubActions == "true" && raw == ci)
}

func TestTranscriptGapDatabaseDestinationGuard(t *testing.T) {
	const local = "postgres://core_fixture@127.0.0.1:55439/multica_core_r25?sslmode=disable"
	const ci = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	for _, tc := range []struct {
		name, dsn, actions string
		allowed            bool
	}{
		{"explicit local fixture", local, "", true},
		{"explicit CI fixture", ci, "true", true},
		{"CI DSN outside CI", ci, "", false},
		{"non-exact CI marker", ci, "1", false},
		{"other local database", "postgres://core_fixture@127.0.0.1:55439/multica_core_review?sslmode=disable", "true", false},
		{"external host", "postgres://multica:multica@db.example:5432/multica?sslmode=disable", "true", false},
		{"CI host redirect option", ci + "&host=db.example", "true", false},
		{"local host redirect option", local + "&hostaddr=203.0.113.1", "true", false},
		{"unset explicit DSN", "", "true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := allowedR25DatabaseURL(tc.dsn, tc.actions); got != tc.allowed {
				t.Fatalf("allowed=%v want=%v", got, tc.allowed)
			}
		})
	}
}

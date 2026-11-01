package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/middleware"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// authenticateTaskActorForTest supplies the trusted identity that Auth normally
// stamps after validating a mat_ credential. Header-only attack fixtures must
// not call this helper.
func authenticateTaskActorForTest(r *http.Request) {
	taskID := r.Header.Get("X-Task-ID")
	if taskID == "" {
		taskID = "11111111-1111-1111-1111-111111111111"
	}
	workspaceID := r.Header.Get("X-Workspace-ID")
	if workspaceID == "" {
		workspaceID = testWorkspaceID
	}
	r.Header.Set("X-Actor-Source", "task_token")
	*r = *r.WithContext(middleware.SetTaskActorContext(r.Context(), middleware.TaskActor{
		UserID: requestUserID(r), AgentID: r.Header.Get("X-Agent-ID"),
		TaskID: taskID, WorkspaceID: workspaceID,
	}))
}

func TestResolveActor_HeadersCannotPromoteMember(t *testing.T) {
	const user = "11111111-1111-1111-1111-111111111111"
	const agent = "22222222-2222-2222-2222-222222222222"
	const task = "33333333-3333-3333-3333-333333333333"
	const workspace = "44444444-4444-4444-4444-444444444444"
	// No Queries instance: even a syntactically valid or legitimately observed
	// pair must not be looked up as a substitute for authenticated delegation.
	h := &Handler{}
	for _, source := range []string{"", "task_token", "member", "cloud_pat"} {
		t.Run(source, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/agents/"+agent, nil)
			r.Header.Set("X-Agent-ID", agent)
			r.Header.Set("X-Task-ID", task)
			r.Header.Set("X-Actor-Source", source)
			kind, id := h.resolveActor(r, user, workspace)
			if kind != "member" || id != user {
				t.Fatalf("got (%q,%q), want member", kind, id)
			}
		})
	}
	actor := middleware.TaskActor{UserID: user, AgentID: agent, TaskID: task, WorkspaceID: workspace}
	r := httptest.NewRequest(http.MethodGet, "/api/agents/"+agent, nil)
	r = r.WithContext(middleware.SetTaskActorContext(context.Background(), actor))
	// Client headers cannot replace the authenticated agent or task.
	r.Header.Set("X-Agent-ID", "forged")
	r.Header.Set("X-Task-ID", "forged")
	kind, id := h.resolveActor(r, user, workspace)
	if kind != "agent" || id != agent {
		t.Fatalf("validated task actor = (%q,%q)", kind, id)
	}

	for _, mismatch := range []struct{ user, workspace string }{{"other", workspace}, {user, "other"}} {
		kind, id = h.resolveActor(r, mismatch.user, mismatch.workspace)
		if kind != "agent" || id != "" {
			t.Fatalf("mismatched binding acquired identity: (%q,%q)", kind, id)
		}
		if h.canAccessPrivateAgent(r.Context(), db.Agent{}, kind, id, mismatch.workspace) {
			t.Fatal("refused binding bypassed private view")
		}
		if h.invokeAgentDecision(r.Context(), db.Agent{}, kind, id, user, mismatch.workspace) {
			t.Fatal("refused binding acquired invoke authority")
		}
		if _, ok := h.accessibleAgentIDs(r.Context(), mismatch.workspace, kind, id, "admin"); ok {
			t.Fatal("refused binding acquired list authority")
		}
		if _, ok := h.restrictedAgentIDs(r.Context(), mismatch.workspace, kind, id, "admin"); ok {
			t.Fatal("refused binding acquired rollup authority")
		}
	}

}

func TestGetAgent_KnownTaskHeadersCannotBypassPrivateView(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	target, _, member := privateAgentTestFixture(t)
	knownTask := insertHandlerTestTask(t, target)
	caller := createHandlerTestAgent(t, "member-owned-r20-caller", nil)
	if _, err := testPool.Exec(context.Background(), "UPDATE agent SET owner_id=$1 WHERE id=$2", member, caller); err != nil {
		t.Fatal(err)
	}
	callerTask := insertHandlerTestTask(t, caller)
	for _, pair := range []struct{ name, agent, task string }{{"target pair", target, knownTask}, {"own agent pair", caller, callerTask}} {
		t.Run(pair.name, func(t *testing.T) {
			r := newRequestAs(member, http.MethodGet, "/api/agents/"+target, nil)
			r = withURLParam(r, "id", target)
			r.Header.Set("X-Agent-ID", pair.agent)
			r.Header.Set("X-Task-ID", pair.task)
			r.Header.Set("X-Actor-Source", "task_token")
			w := httptest.NewRecorder()
			testHandler.GetAgent(w, r)
			if w.Code != http.StatusForbidden {
				t.Fatalf("header pair bypassed private view: %d %s", w.Code, w.Body.String())
			}
		})
	}
	// Validated task credentials retain the existing workspace A2A view policy.
	r := newRequestAs(member, http.MethodGet, "/api/agents/"+target, nil)
	r = withURLParam(r, "id", target)
	r.Header.Set("X-Agent-ID", caller)
	r.Header.Set("X-Task-ID", callerTask)
	authenticateTaskActorForTest(r)
	w := httptest.NewRecorder()
	testHandler.GetAgent(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("validated task credential lost A2A view: %d %s", w.Code, w.Body.String())
	}
}

func TestTaskLineageUsesAuthenticatedTaskContext(t *testing.T) {
	caller := createHandlerTestAgent(t, "r20-lineage-caller", nil)
	other := createHandlerTestAgent(t, "r20-lineage-other", nil)
	boundTask := createHandlerTestTaskForAgent(t, caller)
	foreignTask := createHandlerTestTaskForAgent(t, other)
	if _, err := testPool.Exec(context.Background(), "UPDATE agent_task_queue SET originator_user_id=$1, accountable_user_id=$1 WHERE id=$2", testUserID, boundTask); err != nil {
		t.Fatal(err)
	}
	r := newRequest(http.MethodPost, "/api/issues/fixture/comments", nil)
	r.Header.Set("X-Agent-ID", caller)
	r.Header.Set("X-Task-ID", boundTask)
	authenticateTaskActorForTest(r)
	// A later advisory-header change must not redirect lineage or permissions.
	r.Header.Set("X-Agent-ID", other)
	r.Header.Set("X-Task-ID", foreignTask)
	task, ok := testHandler.taskFromRequestHeader(r)
	if !ok || uuidToString(task.ID) != boundTask {
		t.Fatalf("task context lost its binding: ok=%v task=%s", ok, uuidToString(task.ID))
	}
	if got := testHandler.invokeOriginatorFromRequest(r, "agent", caller); got != testUserID {
		t.Fatalf("originator=%q, want authenticated task's human", got)
	}
	r.Header.Del("X-Agent-ID")
	r.Header.Del("X-Task-ID")
	if task, ok = testHandler.taskFromRequestHeader(r); !ok || uuidToString(task.ID) != boundTask {
		t.Fatal("stripped advisory headers changed the authenticated task")
	}
	forged := newRequest(http.MethodPost, "/api/issues/fixture/comments", nil)
	forged.Header.Set("X-Agent-ID", caller)
	forged.Header.Set("X-Task-ID", boundTask)
	forged.Header.Set("X-Actor-Source", "task_token")
	if _, ok := testHandler.taskFromRequestHeader(forged); ok {
		t.Fatal("header-only task acquired lineage authority")
	}
	invalid := r.WithContext(middleware.SetTaskActorContext(r.Context(), middleware.TaskActor{
		UserID: testUserID, AgentID: caller, TaskID: foreignTask, WorkspaceID: testWorkspaceID,
	}))
	if _, ok := testHandler.taskFromRequestHeader(invalid); ok {
		t.Fatal("task belonging to another agent acquired lineage authority")
	}
}

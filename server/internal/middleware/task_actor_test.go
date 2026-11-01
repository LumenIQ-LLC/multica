package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type taskActorDB struct {
	db.DBTX
	token db.TaskToken
}
type taskActorRow struct{ token db.TaskToken }

func (d taskActorDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	if len(args) != 1 || args[0] != d.token.TokenHash {
		panic("unexpected task-token lookup")
	}
	return taskActorRow{d.token}
}
func (r taskActorRow) Scan(dest ...any) error {
	*dest[0].(*pgtype.UUID) = r.token.ID
	*dest[1].(*string) = r.token.TokenHash
	*dest[2].(*pgtype.UUID) = r.token.TaskID
	*dest[3].(*pgtype.UUID) = r.token.AgentID
	*dest[4].(*pgtype.UUID) = r.token.WorkspaceID
	*dest[5].(*pgtype.UUID) = r.token.UserID
	*dest[6].(*pgtype.Timestamptz) = r.token.ExpiresAt
	*dest[7].(*pgtype.Timestamptz) = r.token.CreatedAt
	return nil
}

func TestAuth_TaskActorIsBoundToValidatedCredential(t *testing.T) {
	const bearer = "mat_synthetic_test_only"
	row := db.TaskToken{
		TokenHash:   auth.HashToken(bearer),
		UserID:      util.MustParseUUID("11111111-1111-1111-1111-111111111111"),
		AgentID:     util.MustParseUUID("22222222-2222-2222-2222-222222222222"),
		TaskID:      util.MustParseUUID("33333333-3333-3333-3333-333333333333"),
		WorkspaceID: util.MustParseUUID("44444444-4444-4444-4444-444444444444"),
	}
	called := false
	h := Auth(db.New(taskActorDB{token: row}), nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		actor, ok := TaskActorFromContext(r.Context())
		want := TaskActor{UserID: uuidToString(row.UserID), AgentID: uuidToString(row.AgentID), TaskID: uuidToString(row.TaskID), WorkspaceID: uuidToString(row.WorkspaceID)}
		if !ok || actor != want {
			t.Fatalf("actor=%+v ok=%v want=%+v", actor, ok, want)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	r := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	r.Header.Set("Authorization", "Bearer "+bearer)
	r.Header.Set("X-Agent-ID", "forged")
	r.Header.Set("X-Task-ID", "forged")
	r.Header.Set("X-Workspace-ID", "forged")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if !called || w.Code != http.StatusNoContent {
		t.Fatalf("status=%d called=%v", w.Code, called)
	}
}

func TestAuth_HumanCredentialClearsPriorTaskContext(t *testing.T) {
	called := false
	h := Auth(nil, nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if _, ok := TaskActorFromContext(r.Context()); ok {
			t.Fatal("human auth retained task authority")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	r := httptest.NewRequest(http.MethodGet, "/api/me", nil)
	r.Header.Set("Authorization", "Bearer "+generateToken(validClaims(), auth.JWTSecret()))
	r = r.WithContext(SetTaskActorContext(r.Context(), TaskActor{UserID: "old", AgentID: "old", TaskID: "old", WorkspaceID: "old"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if !called || w.Code != http.StatusNoContent {
		t.Fatalf("status=%d called=%v", w.Code, called)
	}
}

package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestBodyLimitRejectsBeforeHandler(t *testing.T) {
	for _, contentType := range []string{"", "application/json", "multipart/form-data; boundary=forged"} {
		t.Run(contentType, func(t *testing.T) {
			for _, knownLength := range []bool{true, false} {
				req := httptest.NewRequest(http.MethodPost, "/auth/send-code", strings.NewReader(`{"email":"user@example.test"}`+strings.Repeat(" ", authBodyLimit)))
				req.Header.Set("Content-Type", contentType)
				if !knownLength {
					req.ContentLength = -1
				}
				called := false
				h := RequestBodyLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusRequestEntityTooLarge || called {
					t.Fatalf("knownLength=%v status=%d handler=%v", knownLength, rec.Code, called)
				}
			}
		})
	}
}

func TestRequestBodyLimitPreservesSignedBytes(t *testing.T) {
	want := "{ \"event\": \"signed\" }\r\n"
	h := RequestBodyLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil || string(got) != want {
			t.Fatalf("body=%q error=%v", got, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/webhooks/github", strings.NewReader(want)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestRequestBodyLimitOrdinaryAPIAndBoundedUploadExceptions(t *testing.T) {
	for _, path := range []string{"/api/issues", "/api/workspaces/fixture/issues"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(strings.Repeat("x", jsonBodyLimit+1)))
		rec := httptest.NewRecorder()
		RequestBodyLimit(okHandler).ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s status=%d", path, rec.Code)
		}
	}
	for _, path := range []string{"/api/upload-file", "/api/skills/import", "/api/workspaces/fixture/plugins/packages"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("multipart fixture"))
		req.Header.Set("Content-Type", "multipart/form-data; boundary=fixture")
		req.ContentLength = 20 << 20
		rec := httptest.NewRecorder()
		RequestBodyLimit(okHandler).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("handler-bounded upload %s status=%d", path, rec.Code)
		}
	}
}

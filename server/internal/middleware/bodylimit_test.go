package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chimw "github.com/go-chi/chi/v5/middleware"
)

type deadlineRecorder struct{ *httptest.ResponseRecorder }

func (*deadlineRecorder) SetReadDeadline(time.Time) error { return nil }

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

func TestRequestBodyLimitPreservesExistingSkillBundleContract(t *testing.T) {
	// Eight 1MiB supporting files plus a 1MiB SKILL.md are supported by
	// the importers. Go's JSON encoder can expand each '<' to six bytes.
	content := strings.Repeat("<", 1<<20)
	files := make([]map[string]string, 8)
	for i := range files {
		files[i] = map[string]string{"path": string(rune('a'+i)) + ".md", "content": content}
	}
	bundle := map[string]any{"name": "fixture", "content": content, "files": files}
	for _, path := range []string{"/api/skills", "/api/skills/", "/api/skills/fixture", "/api/daemon/runtimes/runtime/local-skills/import/request/result"} {
		t.Run(path, func(t *testing.T) {
			body := bundle
			if strings.HasPrefix(path, "/api/daemon/") {
				body = map[string]any{"status": "completed", "skill": bundle}
			}
			payload, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			h := RequestBodyLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n, err := io.Copy(io.Discard, r.Body)
				if err != nil || n != int64(len(payload)) {
					t.Errorf("bundle was changed or truncated: bytes=%d error=%v", n, err)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			rec := httptest.NewRecorder()
			method := http.MethodPost
			if path == "/api/skills/fixture" {
				method = http.MethodPut
			}
			h.ServeHTTP(rec, httptest.NewRequest(method, path, bytes.NewReader(payload)))
			if rec.Code != http.StatusNoContent {
				t.Fatalf("valid existing bundle rejected: bytes=%d status=%d", len(payload), rec.Code)
			}
		})
	}
}

func TestRequestBodyLimitSkillExceptionsStayBoundedAndRouteSpecific(t *testing.T) {
	for _, fixture := range []struct{ method, path string }{
		{http.MethodPost, "/api/skills/search"},
		{http.MethodPost, "/api/skills/fixture"},
		{http.MethodPost, "/api/daemon/runtimes/runtime/local-skills/request/result"},
		{http.MethodPost, "/api/daemon/runtimes/runtime/local-skills/import/request/result/extra"},
		{http.MethodPut, "/api/skills/fixture/files"},
	} {
		req := httptest.NewRequest(fixture.method, fixture.path, strings.NewReader("fixture"))
		req.ContentLength = jsonBodyLimit + 1
		rec := httptest.NewRecorder()
		RequestBodyLimit(okHandler).ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("unrelated %s %s bypassed ordinary cap: %d", fixture.method, fixture.path, rec.Code)
		}
	}
	for _, knownLength := range []bool{true, false} {
		req := httptest.NewRequest(http.MethodPost, "/api/skills", io.LimitReader(repeatedBodyByte{}, skillBundleBodyLimit+1))
		if knownLength {
			req.ContentLength = skillBundleBodyLimit + 1
		}
		rec := httptest.NewRecorder()
		RequestBodyLimit(okHandler).ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("skill cap unbounded, knownLength=%v status=%d", knownLength, rec.Code)
		}
	}
}

type repeatedBodyByte struct{}

func (repeatedBodyByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	return len(p), nil
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
		rec := &deadlineRecorder{httptest.NewRecorder()}
		RequestBodyLimit(okHandler).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("handler-bounded upload %s status=%d", path, rec.Code)
		}
	}
}

func TestRequestBodyLimitMultipartHasFiniteExtendedDeadline(t *testing.T) {
	const ordinaryTimeout = 25 * time.Millisecond
	const uploadTimeout = 250 * time.Millisecond
	const byteCap = 512
	parseUpload := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, byteCap)
		if err := r.ParseMultipartForm(byteCap); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		defer r.MultipartForm.RemoveAll()
		w.WriteHeader(http.StatusNoContent)
	})
	bounded := requestBodyLimit(parseUpload, uploadTimeout)
	// Request logging and metrics both use this Chi writer. Exercise its
	// ResponseController unwrapping rather than testing a bare writer only.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bounded.ServeHTTP(chimw.NewWrapResponseWriter(w, r.ProtoMajor), r)
	})
	server := httptest.NewUnstartedServer(handler)
	server.Config.ReadHeaderTimeout = ordinaryTimeout
	server.Config.ReadTimeout = ordinaryTimeout
	server.Config.IdleTimeout = 100 * time.Millisecond
	server.Start()
	defer server.Close()
	for _, fixture := range []struct {
		name, path string
		delay      time.Duration
		size, want int
	}{
		{"slow file upload", "/api/upload-file", 75 * time.Millisecond, 32, http.StatusNoContent},
		{"slow skill import", "/api/skills/import", 75 * time.Millisecond, 32, http.StatusNoContent},
		{"slow plugin package", "/api/workspaces/fixture/plugins/packages", 75 * time.Millisecond, 32, http.StatusNoContent},
		{"oversize upload", "/api/upload-file", 0, 600, http.StatusRequestEntityTooLarge},
		{"upload past finite deadline", "/api/upload-file", 400 * time.Millisecond, 32, http.StatusRequestEntityTooLarge},
		{"ordinary auth body stays short", "/auth/send-code", 75 * time.Millisecond, 32, http.StatusBadRequest},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			var payload bytes.Buffer
			form := multipart.NewWriter(&payload)
			file, err := form.CreateFormFile("file", "fixture.txt")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.WriteString(file, strings.Repeat("x", fixture.size))
			_ = form.Close()
			reader, writer := io.Pipe()
			defer reader.Close()
			go func() {
				data := payload.Bytes()
				_, _ = writer.Write(data[:len(data)/2])
				time.Sleep(fixture.delay)
				_, _ = writer.Write(data[len(data)/2:])
				_ = writer.Close()
			}()
			request, err := http.NewRequest(http.MethodPost, server.URL+fixture.path, reader)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", form.FormDataContentType())
			client := &http.Client{Timeout: 2 * time.Second}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != fixture.want {
				t.Fatalf("status=%d want=%d", response.StatusCode, fixture.want)
			}
		})
	}
}

func TestRequestBodyLimitRejectsUploadWithoutDeadlineSupport(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/upload-file", strings.NewReader("fixture"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=fixture")
	rec := httptest.NewRecorder()
	RequestBodyLimit(okHandler).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rec.Code)
	}
}

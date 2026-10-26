package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const authBodyLimit = 64 << 10
const jsonBodyLimit = 2 << 20
const webhookBodyLimit = 10 << 20
const multipartReadTimeout = 5 * time.Minute

// RequestBodyLimit bounds ordinary JSON even when Content-Type is missing or
// forged. Buffering before the handler also rejects oversized trailing bytes
// after an otherwise valid first JSON value. No JSON re-encoding is performed,
// preserving signed webhook bytes. Streaming responses and upgrades are untouched.
func RequestBodyLimit(next http.Handler) http.Handler {
	return requestBodyLimit(next, multipartReadTimeout)
}

func requestBodyLimit(next http.Handler, uploadTimeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		path := r.URL.Path
		// These three multipart handlers install their own MaxBytesReader
		// before ParseMultipartForm (100MiB/16MiB/plugin bundle+overhead).
		multipart := strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/form-data")
		if multipart && (path == "/api/upload-file" || path == "/api/skills/import" ||
			(strings.HasPrefix(path, "/api/workspaces/") && strings.HasSuffix(path, "/plugins/packages"))) {
			// A 100MiB upload can legitimately outlast the ordinary 30s body
			// budget. Extend only these handler-capped multipart reads, keeping
			// a finite total upload deadline and the original byte limits.
			if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(uploadTimeout)); err != nil {
				writeBodyError(w, http.StatusServiceUnavailable, "upload read deadline unavailable")
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		limit := int64(jsonBodyLimit)
		if strings.HasPrefix(path, "/auth/") {
			limit = authBodyLimit
		} else if strings.HasPrefix(path, "/api/webhooks/") {
			limit = webhookBodyLimit
		}
		if r.ContentLength > limit {
			writeBodyError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		body := http.MaxBytesReader(w, r.Body, limit)
		payload, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			var sizeErr *http.MaxBytesError
			if errors.As(err, &sizeErr) {
				writeBodyError(w, http.StatusRequestEntityTooLarge, "request body too large")
			} else {
				writeBodyError(w, http.StatusBadRequest, "failed to read request body")
			}
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(payload))
		next.ServeHTTP(w, r)
	})
}

func writeBodyError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

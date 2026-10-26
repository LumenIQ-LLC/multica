package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProviderResponseHardLimit(t *testing.T) {
	var target map[string]string
	if err := decodeProviderJSON(strings.NewReader(`{"ok":"yes"}`), 64, &target); err != nil {
		t.Fatal(err)
	}
	if target["ok"] != "yes" {
		t.Fatal(target)
	}
	// A valid JSON prefix followed by excess whitespace must not bypass the cap.
	if err := decodeProviderJSON(strings.NewReader(`{"ok":"yes"}`+strings.Repeat(" ", 100)), 64, &target); err == nil {
		t.Fatal("oversized body accepted")
	}
	if err := decodeProviderJSON(strings.NewReader(`{"credential":"secret"`), 64, &target); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("unsafe JSON diagnostic")
	}
}

func TestProviderRequestCancellationAndRedirect(t *testing.T) {
	if providerHTTPClient.Timeout != 15*time.Second {
		t.Fatal("missing total request timeout")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/credential-target", http.StatusFound)
			return
		}
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		t.Error("redirect target was reached")
	}))
	defer server.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/redirect?code=private-fixture", nil)
	response, err := doProviderRequest(request)
	if err != nil {
		_, fixtureErr := providerHTTPClient.Do(request)
		t.Fatalf("fixture transport: %v", fixtureErr)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusFound {
		t.Fatal("redirect followed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/slow?code=private-fixture", nil)
	started := time.Now()
	_, err = doProviderRequest(request)
	if err == nil || strings.Contains(err.Error(), "private-fixture") {
		t.Fatalf("unsafe diagnostic: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("context cancellation did not bound upstream")
	}
}

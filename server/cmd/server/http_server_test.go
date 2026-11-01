package main

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/multica-ai/multica/server/internal/middleware"
)

func TestHTTPServerTimeoutDefaultsPreserveStreams(t *testing.T) {
	srv := newHTTPServer(":0", http.NotFoundHandler())
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.IdleTimeout <= 0 || srv.MaxHeaderBytes <= 0 {
		t.Fatal("request/idle bounds missing")
	}
	if srv.WriteTimeout != 0 {
		t.Fatal("blanket write deadline would terminate long streams")
	}
}

func TestHTTPServerSlowHeadersAndIdleConnectionTerminate(t *testing.T) {
	srv := newHTTPServer("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	srv.ReadHeaderTimeout = 50 * time.Millisecond
	srv.IdleTimeout = 50 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() { _ = srv.Close() })
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: fixture\r\nX-Slow:")
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadAll(conn)
	_ = conn.Close()
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("slow header connection was not terminated")
	}

	idle, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	_, _ = io.WriteString(idle, "GET / HTTP/1.1\r\nHost: fixture\r\n\r\n")
	reader := bufio.NewReader(idle)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	_ = idle.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = reader.ReadByte()
	if err == nil {
		t.Fatal("idle connection unexpectedly returned bytes")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("idle connection was not terminated")
	}
}

func TestHTTPServerStreamsAndWebSocketRemainUsable(t *testing.T) {
	upgrader := websocket.Upgrader{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			kind, data, err := conn.ReadMessage()
			if err == nil {
				_ = conn.WriteMessage(kind, data)
			}
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(75 * time.Millisecond)
		_, _ = io.WriteString(w, "data: second\n\n")
	})
	boundedHandler := middleware.RequestBodyLimit(handler)
	server := httptest.NewUnstartedServer(boundedHandler)
	server.Config = newHTTPServer("", boundedHandler)
	server.Config.ReadTimeout = 20 * time.Millisecond
	server.Config.IdleTimeout = 20 * time.Millisecond
	server.Start()
	defer server.Close()
	response, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || !strings.Contains(string(body), "data: second") {
		t.Fatalf("stream body=%q err=%v", body, err)
	}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(75 * time.Millisecond)
	if err := conn.WriteMessage(websocket.TextMessage, []byte("fixture")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil || string(data) != "fixture" {
		t.Fatalf("websocket=%q error=%v", data, err)
	}
}

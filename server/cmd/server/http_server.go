package main

import (
	"net/http"
	"time"
)

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
		// RequestBodyLimit extends only the existing byte-capped multipart
		// upload routes to five minutes through ResponseController.
		ReadTimeout:    30 * time.Second,
		IdleTimeout:    90 * time.Second,
		MaxHeaderBytes: 1 << 20,
		// SSE and WebSocket lifetimes are intentionally independent of request
		// upload/idle deadlines. Do not add a blanket response WriteTimeout.
	}
}

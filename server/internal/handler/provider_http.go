package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

// Provider calls are finite, including body reads. Refuse redirects so fixed
// OAuth/API endpoints cannot forward credentials to another destination.
var providerHTTPClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		MaxConnsPerHost:       32,
		ForceAttemptHTTP2:     true,
	},
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
}

func doProviderRequest(req *http.Request) (*http.Response, error) {
	response, err := providerHTTPClient.Do(req)
	if err != nil {
		// URL errors may contain authorization codes or other query credentials.
		return nil, errors.New("provider request failed")
	}
	return response, nil
}

func decodeProviderJSON(body io.Reader, limit int64, target any) error {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return errors.New("provider response read failed")
	}
	if int64(len(data)) > limit {
		return errors.New("provider response exceeds limit")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return errors.New("invalid provider JSON response")
	}
	return nil
}

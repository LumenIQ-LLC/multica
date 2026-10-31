package middleware

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func rateLimitCall(h http.Handler, ip string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/auth/send-code", nil)
	req.RemoteAddr = ip + ":1234"
	req.Header.Set("X-Forwarded-For", "198.51.100.99")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRateLimitMissingRedisHasAtomicFiniteBudget(t *testing.T) {
	h := RateLimit(nil, 5, time.Minute, nil)(okHandler)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := rateLimitCall(h, "203.0.113.1")
			if res.Code == http.StatusOK {
				successes.Add(1)
			}
			if res.Header().Get("X-RateLimit-Mode") != "local_per_instance" {
				t.Error("degradation mode missing")
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 5 {
		t.Fatalf("allowed=%d want5", successes.Load())
	}
	// Hitting one source's ceiling must not trip a global emergency lockout.
	if res := rateLimitCall(h, "203.0.113.2"); res.Code != http.StatusOK {
		t.Fatalf("different source status=%d", res.Code)
	}
}

func TestRateLimitRedisFailureRetainsBudgetAndReportsMode(t *testing.T) {
	// A closed disposable client deterministically rejects the Redis operation
	// without requiring an external service or making a network connection.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	_ = rdb.Close()
	health := NewRateLimitHealth(true)
	h := RateLimit(rdb, 2, time.Minute, nil, health)(okHandler)
	for i := range 4 {
		res := rateLimitCall(h, "203.0.113.1")
		want := http.StatusOK
		if i >= 2 {
			want = http.StatusTooManyRequests
		}
		if res.Code != want {
			t.Fatalf("request%d status=%d want=%d", i, res.Code, want)
		}
	}
	if health.Mode() != "local_per_instance" {
		t.Fatalf("mode=%s", health.Mode())
	}
}

func TestLocalRateWindowsIsolateSourcesExpireAndStayBounded(t *testing.T) {
	l := &localRateLimiter{}
	now := time.Unix(1000, 0)
	if !l.allow("fixture", 2, time.Minute, now) {
		t.Fatal("local allowance failed")
	}
	for i := range 100000 {
		l.allow(fmt.Sprint(i), 1, time.Minute, now)
	}
	if !l.allow("fixture", 2, time.Minute, now) {
		t.Fatal("rotating other keys consumed a live source's budget")
	}
	if l.allow("fixture", 2, time.Minute, now) {
		t.Fatal("hot budget reset after key pressure")
	}
	if len(l.windows) != localRateCapacity {
		t.Fatal("local storage grew")
	}
	if l.allow("new-source-at-capacity", 1, time.Minute, now) {
		t.Fatal("new source bypassed finite capacity")
	}
	if !l.allow("new-source-after-expiry", 1, time.Minute, now.Add(time.Minute)) || len(l.windows) != 1 {
		t.Fatal("expired windows were not reclaimed")
	}
}

func TestRateLimitLocalFallbackPreservesExplicitProxyTrust(t *testing.T) {
	_, proxy, err := net.ParseCIDR("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	for _, trusted := range []bool{false, true} {
		var proxies []*net.IPNet
		if trusted {
			proxies = []*net.IPNet{proxy}
		}
		h := RateLimit(nil, 1, time.Minute, proxies)(okHandler)
		for i, forwarded := range []string{"203.0.113.1", "203.0.113.2"} {
			req := httptest.NewRequest(http.MethodPost, "/auth/send-code", nil)
			req.RemoteAddr = "10.0.0.1:1234"
			req.Header.Set("X-Forwarded-For", forwarded)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			want := http.StatusOK
			if i > 0 && !trusted {
				want = http.StatusTooManyRequests
			}
			if rec.Code != want {
				t.Fatalf("trusted=%v client=%d status=%d want=%d", trusted, i, rec.Code, want)
			}
		}
	}
}

func TestLocalRateCapacityIsAtomicForNewSources(t *testing.T) {
	l := &localRateLimiter{}
	now := time.Unix(1000, 0)
	for i := range localRateCapacity - 1 {
		if !l.allow(fmt.Sprint(i), 1, time.Minute, now) {
			t.Fatal("capacity exhausted prematurely")
		}
	}
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := range 128 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.allow(fmt.Sprintf("new-%d", i), 1, time.Minute, now) {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 1 || len(l.windows) != localRateCapacity {
		t.Fatalf("capacity raced: admitted=%d windows=%d", admitted.Load(), len(l.windows))
	}
}

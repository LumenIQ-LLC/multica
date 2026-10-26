package middleware

import (
	"fmt"
	"hash/maphash"
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

func TestLocalRateSlotsExpireWithoutEvictionAndStayBounded(t *testing.T) {
	l := &localRateLimiter{seed: maphash.MakeSeed()}
	now := time.Unix(1000, 0)
	if !l.allow("fixture", 1, time.Minute, now) || l.allow("fixture", 1, time.Minute, now) {
		t.Fatal("local allowance failed")
	}
	for i := range 100000 {
		l.allow(fmt.Sprint(i), 1, time.Minute, now)
	}
	if l.allow("fixture", 1, time.Minute, now) {
		t.Fatal("hot budget reset after key pressure")
	}
	if len(l.slots) != 65536 {
		t.Fatal("local storage grew")
	}
	if !l.allow("fixture", 1, time.Minute, now.Add(time.Minute)) {
		t.Fatal("expired budget did not reset")
	}
}

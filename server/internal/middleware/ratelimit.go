package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// rateLimitScript atomically increments the counter and sets the TTL on
// first access. Using a Lua script ensures INCR and EXPIRE cannot be
// split by a network failure — if INCR succeeds the TTL is guaranteed
// to be set, preventing a stuck key that acts as a permanent ban.
var rateLimitScript = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
`)

// ParseTrustedProxies parses a comma-separated list of CIDRs into a
// slice of *net.IPNet. Invalid entries are warned and skipped.
// Returns nil if raw is empty (default: never trust X-Forwarded-For).
func ParseTrustedProxies(raw string) []*net.IPNet {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	var nets []*net.IPNet
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		_, cidr, err := net.ParseCIDR(p)
		if err != nil {
			slog.Warn("ratelimit: invalid trusted proxy CIDR, skipping", "cidr", p, "error", err)
			continue
		}
		nets = append(nets, cidr)
	}
	return nets
}

// RateLimitHealth exposes degradation without making local-only deployments
// unready. The emergency limit is per process; Redis enforces the shared limit.
type RateLimitHealth struct {
	redisConfigured bool
	degradedUntil   atomic.Int64
}

func NewRateLimitHealth(redisConfigured bool) *RateLimitHealth {
	return &RateLimitHealth{redisConfigured: redisConfigured}
}

func (h *RateLimitHealth) Mode() string {
	if !h.redisConfigured || time.Now().UnixNano() < h.degradedUntil.Load() {
		return "local_per_instance"
	}
	return "redis_with_local_guard"
}

type localRateWindow struct {
	count   int
	expires time.Time
}

const localRateCapacity = 65536

// Exact keys keep different sources from sharing counters. Live windows are
// never evicted to admit new keys: at capacity only new sources fail closed.
// Expired windows are reclaimed at most once per second, bounding sweep work.
type localRateLimiter struct {
	mu        sync.Mutex
	windows   map[string]localRateWindow
	nextSweep time.Time
}

func (l *localRateLimiter) allow(key string, limit int, window time.Duration, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.windows == nil {
		l.windows = make(map[string]localRateWindow)
	}
	if !now.Before(l.nextSweep) {
		for source, entry := range l.windows {
			if !now.Before(entry.expires) {
				delete(l.windows, source)
			}
		}
		l.nextSweep = now.Add(time.Second)
	}
	entry, found := l.windows[key]
	if !found || !now.Before(entry.expires) {
		if !found && len(l.windows) >= localRateCapacity {
			return false
		}
		entry = localRateWindow{expires: now.Add(window)}
	}
	if entry.count >= limit {
		return false
	}
	entry.count++
	l.windows[key] = entry
	return true
}

// RateLimit returns a per-IP fixed-window limiter. A bounded local guard is
// always reserved first, so Redis failures, recovery and missing configuration
// cannot repeatedly reset the process's allowance.
//
// trustedProxies controls X-Forwarded-For handling: when the direct
// connection (RemoteAddr) originates from a CIDR in the list, the
// rightmost non-trusted IP in the XFF chain is used as the client IP.
// When the list is empty (default), XFF is never consulted — only
// RemoteAddr is used. This matches the project's conservative trust
// model (see health_realtime.go).
func RateLimit(rdb *redis.Client, limit int, window time.Duration, trustedProxies []*net.IPNet, health ...*RateLimitHealth) func(http.Handler) http.Handler {
	local := &localRateLimiter{}
	status := NewRateLimitHealth(rdb != nil)
	if len(health) > 0 && health[0] != nil {
		status = health[0]
	}
	if limit < 1 {
		limit = 1
	}
	if window < time.Second {
		window = time.Second
	}
	// Bound socket reads/writes as well as the operation context: go-redis
	// clients need not enable ContextTimeoutEnabled. The clone shares the pool.
	redisLimiter := rdb
	if rdb != nil {
		redisLimiter = rdb.WithTimeout(time.Second)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := extractIP(r, trustedProxies)
			key := rateLimitKey(r.URL.Path, ip)
			w.Header().Set("X-RateLimit-Mode", status.Mode())
			if !local.allow(key, limit, window, time.Now()) {
				writeRateLimit(w, window)
				return
			}
			if redisLimiter != nil {
				ctx, cancel := context.WithTimeout(r.Context(), time.Second)
				count, err := rateLimitScript.Run(ctx, redisLimiter, []string{key}, int(window.Seconds())).Int64()
				cancel()
				if err != nil {
					status.degradedUntil.Store(time.Now().Add(time.Minute).UnixNano())
					w.Header().Set("X-RateLimit-Mode", status.Mode())
				} else if count > int64(limit) {
					writeRateLimit(w, window)
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}

func writeRateLimit(w http.ResponseWriter, window time.Duration) {
	w.Header().Set("Retry-After", fmt.Sprintf("%d", int(window.Seconds())))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "too many requests"})
}

// extractIP determines the client IP for rate limiting purposes.
// It only honors X-Forwarded-For when RemoteAddr is from a trusted proxy.
func extractIP(r *http.Request, trustedProxies []*net.IPNet) string {
	remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteHost = r.RemoteAddr
	}

	if len(trustedProxies) > 0 {
		remoteIP := net.ParseIP(remoteHost)
		if remoteIP != nil && isTrustedProxy(remoteIP, trustedProxies) {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				// Walk right-to-left: the rightmost non-trusted entry is
				// the last hop before the trusted proxy chain.
				parts := strings.Split(xff, ",")
				for i := len(parts) - 1; i >= 0; i-- {
					candidate := net.ParseIP(strings.TrimSpace(parts[i]))
					if candidate != nil && !isTrustedProxy(candidate, trustedProxies) {
						return candidate.String()
					}
				}
			}
		}
	}

	// Default: use RemoteAddr in canonical form.
	if ip := net.ParseIP(remoteHost); ip != nil {
		return ip.String()
	}
	return remoteHost
}

func isTrustedProxy(ip net.IP, cidrs []*net.IPNet) bool {
	for _, cidr := range cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func rateLimitKey(path, ip string) string {
	sanitized := strings.TrimPrefix(path, "/")
	sanitized = strings.ReplaceAll(sanitized, "/", ":")
	return fmt.Sprintf("mul:ratelimit:%s:%s", sanitized, ip)
}

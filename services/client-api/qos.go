package main

import (
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
)

const (
	defaultQoSMaxConcurrent      = 100
	defaultQoSMediaShedThreshold = 70
)

// QoSLimiter enforces priority-aware load shedding:
// when concurrency reaches mediaShedThreshold, Media (hazards:read:summary) requests
// are shed with HTTP 429 while Field Team (hazards:read:all) traffic continues to be served.
type QoSLimiter struct {
	maxConcurrent      int64
	mediaShedThreshold int64
	inFlight           atomic.Int64
	fieldAccepted      atomic.Int64
	mediaAccepted      atomic.Int64
	mediaShed          atomic.Int64
}

func newQoSLimiterFromEnv() *QoSLimiter {
	maxConc := envInt64("QOS_MAX_CONCURRENT", defaultQoSMaxConcurrent)
	if maxConc <= 0 {
		maxConc = defaultQoSMaxConcurrent
	}
	shedAt := envInt64("QOS_MEDIA_SHED_THRESHOLD", defaultQoSMediaShedThreshold)
	if shedAt <= 0 || shedAt > maxConc {
		shedAt = (maxConc * 70) / 100
		if shedAt <= 0 {
			shedAt = 1
		}
	}
	return NewQoSLimiter(maxConc, shedAt)
}

func NewQoSLimiter(maxConcurrent, mediaShedThreshold int64) *QoSLimiter {
	return &QoSLimiter{
		maxConcurrent:      maxConcurrent,
		mediaShedThreshold: mediaShedThreshold,
	}
}

func (q *QoSLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if q == nil {
			next.ServeHTTP(w, r)
			return
		}
		claims := claimsFromContext(r.Context())
		cur := q.inFlight.Add(1)

		if claims != nil && claims.Scope == scopeSummary {
			if cur > q.mediaShedThreshold {
				q.inFlight.Add(-1)
				q.mediaShed.Add(1)
				w.Header().Set("Retry-After", "1")
				writeJSON(w, http.StatusTooManyRequests, errorResponse{
					Error:   "TOO_MANY_REQUESTS",
					Message: "System capacity exceeded; priority allocated to Field Team traffic",
				})
				return
			}
			q.mediaAccepted.Add(1)
		} else {
			if cur > q.maxConcurrent {
				q.inFlight.Add(-1)
				writeJSON(w, http.StatusServiceUnavailable, errorResponse{
					Error:   "SERVICE_UNAVAILABLE",
					Message: "Gateway at maximum capacity; please retry",
				})
				return
			}
			q.fieldAccepted.Add(1)
		}

		defer q.inFlight.Add(-1)
		next.ServeHTTP(w, r)
	})
}

func (q *QoSLimiter) Stats() map[string]int64 {
	if q == nil {
		return map[string]int64{}
	}
	return map[string]int64{
		"in_flight":            q.inFlight.Load(),
		"max_concurrent":       q.maxConcurrent,
		"media_shed_threshold": q.mediaShedThreshold,
		"field_accepted":       q.fieldAccepted.Load(),
		"media_accepted":       q.mediaAccepted.Load(),
		"media_shed":           q.mediaShed.Load(),
	}
}

func envInt64(name string, fallback int64) int64 {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

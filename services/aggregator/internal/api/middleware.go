package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"bnpb/aggregator/internal/logger"
)

// responseWriterInterceptor captures the HTTP response status code and size.
type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
	bytesWritten int64
}

func (w *responseWriterInterceptor) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterInterceptor) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

// GenerateCorrelationID produces a pseudo-unique correlation ID if none is supplied.
func GenerateCorrelationID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("corr-%d-%08x", time.Now().UnixNano(), time.Now().UnixMilli()%100000000)
	}
	return fmt.Sprintf("corr-%d-%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}

// CorrelationIDMiddleware extracts or injects the X-Correlation-ID header into request context and response headers.
func CorrelationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = GenerateCorrelationID()
		}

		// Inject into context
		ctx := logger.WithCorrelationID(r.Context(), corrID)

		// Set on response headers
		w.Header().Set("X-Correlation-ID", corrID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LoggingMiddleware logs every HTTP request with structured fields, including latency and status code.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseWriterInterceptor{
			ResponseWriter: w,
			statusCode:     http.StatusOK, // Default to 200 if WriteHeader isn't called explicitly
		}

		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		log := logger.FromContext(r.Context())

		log.Info("http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("query", r.URL.RawQuery),
			slog.Int("status", rec.statusCode),
			slog.Int64("duration_ms", duration.Milliseconds()),
			slog.String("remote_addr", r.RemoteAddr),
		)
	})
}

// RecoveryMiddleware catches unhandled panics, logs the stack trace, and returns HTTP 500 JSON.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log := logger.FromContext(r.Context())
				log.Error("panic recovered in http handler",
					slog.Any("error", rec),
					slog.String("stack", string(debug.Stack())),
				)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":   "INTERNAL_ERROR",
					"message": "An unexpected internal server error occurred",
				})
			}
		}()

		next.ServeHTTP(w, r)
	})
}

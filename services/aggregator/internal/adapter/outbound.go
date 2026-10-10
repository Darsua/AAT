package adapter

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"bnpb/aggregator/internal/logger"
)

// doLogged executes an outgoing HTTP request and records one structured log line
// with the upstream name, endpoint, status and latency, tagged with the correlation ID.
func doLogged(ctx context.Context, client *http.Client, req *http.Request, upstream string) (*http.Response, error) {
	started := time.Now()
	resp, err := client.Do(req)
	attrs := []any{
		slog.String("event", "outbound_call"),
		slog.String("upstream", upstream),
		slog.String("method", req.Method),
		slog.String("path", req.URL.Path),
		slog.Int64("duration_ms", time.Since(started).Milliseconds()),
	}
	if corrID := logger.CorrelationIDFromContext(ctx); corrID != "" {
		attrs = append(attrs, slog.String("correlation_id", corrID))
	}
	if err != nil {
		slog.Warn("outbound call failed", append(attrs, slog.String("error", err.Error()))...)
		return nil, err
	}
	slog.Info("outbound call completed", append(attrs, slog.Int("status", resp.StatusCode))...)
	return resp, nil
}

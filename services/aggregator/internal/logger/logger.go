package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type contextKey string

const (
	correlationIDKey contextKey = "correlation_id"
)

var defaultLogger *slog.Logger

func init() {
	// Default to standard JSON logger
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}).WithAttrs([]slog.Attr{
		slog.String("service", "aggregator"),
	})
	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
}

// Setup initializes the global structured logger with the specified log level.
func Setup(level string) *slog.Logger {
	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn", "warning":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slogLevel,
	}).WithAttrs([]slog.Attr{
		slog.String("service", "aggregator"),
	})

	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
	return defaultLogger
}

// WithCorrelationID returns a new context with the correlation ID attached.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

// CorrelationIDFromContext retrieves the correlation ID from the context if present.
func CorrelationIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(correlationIDKey).(string); ok {
		return val
	}
	return ""
}

// FromContext returns a logger pre-populated with context attributes (like correlation_id).
func FromContext(ctx context.Context) *slog.Logger {
	corrID := CorrelationIDFromContext(ctx)
	if corrID != "" {
		return defaultLogger.With(slog.String("correlation_id", corrID))
	}
	return defaultLogger
}

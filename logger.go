package smartdb

import (
	"context"
)

// Logger defines the interface for logging operations used by smartdb.
// Implementations can provide custom logging behavior for debug and error messages.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, err error, args ...any)
}

// LogArgs is a map type used to pass structured logging arguments to Logger methods.
type LogArgs map[string]any

// NopLogger is a no-operation logger that discards all log messages.
// It is used as the default logger in SmartDB when no logger is provided.
type NopLogger struct{}

// NewNopLogger creates a new NopLogger instance.
func NewNopLogger() *NopLogger {
	return &NopLogger{}
}

// Debug logs a debug message (no-op).
func (*NopLogger) Debug(context.Context, string, ...any) {}

// Error logs an error message (no-op).
func (*NopLogger) Error(context.Context, error, ...any) {}

var _ Logger = (*NopLogger)(nil)

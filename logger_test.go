package smartdb_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/yuppyweb/smartdb"
)

type mockDebugLog struct {
	ctx  context.Context
	msg  string
	args []any
}

type mockErrorLog struct {
	ctx  context.Context
	err  error
	args []any
}

type mockLogger struct {
	mu       sync.Mutex
	debugLog []mockDebugLog
	errorLog []mockErrorLog
}

func (m *mockLogger) Debug(ctx context.Context, msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.debugLog = append(m.debugLog, mockDebugLog{
		ctx:  ctx,
		msg:  msg,
		args: args,
	})
}

func (m *mockLogger) Error(ctx context.Context, err error, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errorLog = append(m.errorLog, mockErrorLog{
		ctx:  ctx,
		err:  err,
		args: args,
	})
}

var _ smartdb.Logger = (*mockLogger)(nil)

func TestNopLogger(t *testing.T) {
	t.Parallel()

	log := smartdb.NewNopLogger()
	ctx := context.Background()

	// Test that NopLogger implements the Logger interface without panicking
	log.Debug(ctx, "this is a debug message", smartdb.LogArgs{"key": "value"})
	log.Error(ctx, errors.New("error message"), smartdb.LogArgs{"key": "value"})
}

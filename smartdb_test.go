package smartdb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/yuppyweb/smartdb"
)

type mockSQLResult struct{}

func (m *mockSQLResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (m *mockSQLResult) RowsAffected() (int64, error) {
	return 0, nil
}

type mockContextDatabaser struct {
	ctx    context.Context
	opts   *sql.TxOptions
	query  string
	args   []any
	sqlTx  *sql.Tx
	result sql.Result
	stmt   *sql.Stmt
	rows   *sql.Rows
	row    *sql.Row
	err    error
}

func (m *mockContextDatabaser) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	m.ctx = ctx
	m.opts = opts

	return m.sqlTx, m.err
}

func (m *mockContextDatabaser) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	m.ctx = ctx
	m.query = query
	m.args = args

	return m.result, m.err
}

func (m *mockContextDatabaser) PingContext(ctx context.Context) error {
	m.ctx = ctx

	return m.err
}

func (m *mockContextDatabaser) PrepareContext(
	ctx context.Context,
	query string,
) (*sql.Stmt, error) {
	m.ctx = ctx
	m.query = query

	return m.stmt, m.err
}

func (m *mockContextDatabaser) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	m.ctx = ctx
	m.query = query
	m.args = args

	return m.rows, m.err
}

func (m *mockContextDatabaser) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	m.ctx = ctx
	m.query = query
	m.args = args

	return m.row
}

var _ smartdb.ContextDatabaser = (*mockContextDatabaser)(nil)

func TestNewSmartDB_WithoutDB(t *testing.T) {
	t.Parallel()

	_, err := smartdb.New(nil)
	if err == nil {
		t.Error("expected error when creating SmartDB with nil DB, got nil")
	}

	if !errors.Is(err, smartdb.ErrDatabaseNil) {
		t.Errorf("expected error %v, got %v", smartdb.ErrDatabaseNil, err)
	}
}

func TestNewSmartDB_WithOptions(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	mockGen := new(mockGenerator)
	mockLog := new(mockLogger)
	txOpts := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	}

	smartDB, err := smartdb.New(
		mockDB,
		smartdb.WithGenerator(mockGen),
		smartdb.WithLogger(mockLog),
		smartdb.WithTxOptions(txOpts),
	)
	if err != nil {
		t.Errorf("unexpected error creating SmartDB with options: %v", err)
	}

	if smartDB == nil {
		t.Error("expected non-nil SmartDB instance, got nil")
	}
}

func TestNewSmartDB_WithoutLoger(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)

	smartDB, err := smartdb.New(mockDB)
	if err != nil {
		t.Errorf("unexpected error creating SmartDB without logger: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	_, _ = smartDB.BeginTx(context.Background(), nil)
	_, _ = smartDB.ExecContext(context.Background(), "SELECT 1")
	_, _ = smartDB.PrepareContext(context.Background(), "SELECT 1")
	_, _ = smartDB.QueryContext(context.Background(), "SELECT 1")
	_, _ = smartDB.BeginContext(context.Background())
	_ = smartDB.QueryRowContext(context.Background(), "SELECT 1")
	_ = smartDB.PingContext(context.Background())
	_ = smartDB.CommitContext(context.Background())
	_ = smartDB.RollbackContext(context.Background())
}

func TestNewSmartDB_WithNilOption(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)

	_, err := smartdb.New(mockDB, nil)
	if err == nil {
		t.Error("expected error when creating SmartDB with nil option, got nil")
	}

	if !errors.Is(err, smartdb.ErrInvalidOption) {
		t.Errorf("expected error %v, got %v", smartdb.ErrInvalidOption, err)
	}
}

func TestSmartDB_BeginTx(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		opts *sql.TxOptions
	}{
		{
			name: "default options",
			opts: nil,
		},
		{
			name: "custom options",
			opts: &sql.TxOptions{
				Isolation: sql.LevelSerializable,
				ReadOnly:  true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockDB := new(mockContextDatabaser)
			log := new(mockLogger)
			ctx := context.WithValue(
				context.Background(),
				&mockTxContextKey{},
				"test-smartdb-begin-tx",
			)

			smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
			if err != nil {
				t.Errorf("unexpected error creating SmartDB: %v", err)
			}

			if smartDB == nil {
				t.Fatal("expected non-nil SmartDB instance, got nil")
			}

			mockDB.sqlTx = new(sql.Tx)

			tx, err := smartDB.BeginTx(ctx, tc.opts)
			if err != nil {
				t.Errorf("unexpected error beginning transaction: %v", err)
			}

			if tx == nil {
				t.Error("expected non-nil transaction, got nil")
			}

			if mockDB.ctx != ctx {
				t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
			}

			if mockDB.opts != tc.opts {
				t.Errorf("expected TxOptions %v, got %v", tc.opts, mockDB.opts)
			}

			if len(log.debugLog) != 1 {
				t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
			}

			if log.debugLog[0].ctx != ctx {
				t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
			}

			if log.debugLog[0].msg != "smartdb: beginning sql transaction" {
				t.Errorf(
					"expected log message 'smartdb: beginning sql transaction', got '%s'",
					log.debugLog[0].msg,
				)
			}

			if len(log.debugLog[0].args) != 1 {
				t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
			}

			arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
			if !ok {
				t.Errorf(
					"expected log argument of type smartdb.LogArgs, got %T",
					log.debugLog[0].args[0],
				)
			}

			if len(arg) != 1 {
				t.Fatalf("expected 1 log argument key-value pair, got %d", len(arg))
			}

			if (arg)["options"] != tc.opts {
				t.Errorf(
					"expected log argument 'options' to be %v, got %v",
					tc.opts,
					(arg)["options"],
				)
			}
		})
	}
}

func TestSmartDB_BeginTx_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-begin-tx-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.err = expectedErr

	_, err = smartDB.BeginTx(ctx, nil)
	if err == nil {
		t.Error("expected error beginning transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrBeginSQLTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrBeginSQLTx, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: beginning sql transaction" {
		t.Errorf(
			"expected log message 'smartdb: beginning sql transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 log argument key-value pair, got %d", len(arg))
	}

	if (arg)["options"] != (*sql.TxOptions)(nil) {
		t.Errorf("expected log argument 'options' to be nil, got %v", (arg)["options"])
	}
}

func TestSmartDB_ExecContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-smartdb-exec-context")

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.result = new(mockSQLResult)

	result, err := smartDB.ExecContext(ctx, "SELECT 1", 5, "arg")
	if err != nil {
		t.Errorf("unexpected error executing query: %v", err)
	}

	if result != mockDB.result {
		t.Errorf("expected result %v, got %v", mockDB.result, result)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if mockDB.query != "SELECT 1" {
		t.Errorf("expected query 'SELECT 1', got '%s'", mockDB.query)
	}

	if len(mockDB.args) != 2 {
		t.Fatalf("expected 2 query arguments, got %d", len(mockDB.args))
	}

	if mockDB.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockDB.args[0])
	}

	if mockDB.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockDB.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: executing query in database" {
		t.Errorf(
			"expected log message 'smartdb: executing query in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Error("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_ExecContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-exec-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.err = expectedErr

	_, err = smartDB.ExecContext(ctx, "SELECT 1", 5, "arg")
	if err == nil {
		t.Error("expected error executing query, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrExecInDB) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrExecInDB, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: executing query in database" {
		t.Errorf(
			"expected log message 'smartdb: executing query in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Error("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_ExecContext_WithTx(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-exec-context-with-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockResult := new(mockSQLResult)
	mockTx := new(mockTx)
	mockTx.result = mockResult

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	result, err := smartDB.ExecContext(ctxWithTx, "SELECT 1", 5, "arg")
	if err != nil {
		t.Errorf("unexpected error executing query with transaction: %v", err)
	}

	if result != mockResult {
		t.Errorf("expected result to be %v, got %v", mockResult, result)
	}

	if mockTx.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, mockTx.ctx)
	}

	if mockTx.query != "SELECT 1" {
		t.Errorf("expected transaction to receive query 'SELECT 1', got '%s'", mockTx.query)
	}

	if len(mockTx.args) != 2 {
		t.Fatalf("expected transaction to receive 2 query arguments, got %d", len(mockTx.args))
	}

	if mockTx.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockTx.args[0])
	}

	if mockTx.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockTx.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: executing query in transaction" {
		t.Errorf(
			"expected log message 'smartdb: executing query in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_ExecContext_WithTxError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-exec-context-with-tx-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockTx := new(mockTx)
	mockTx.err = expectedErr

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	_, err = smartDB.ExecContext(ctxWithTx, "SELECT 1", 5, "arg")
	if err == nil {
		t.Error("expected error executing query with transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrExecInTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrExecInTx, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: executing query in transaction" {
		t.Errorf(
			"expected log message 'smartdb: executing query in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_PingContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-smartdb-ping-context")

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	err = smartDB.PingContext(ctx)
	if err != nil {
		t.Errorf("unexpected error pinging database: %v", err)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: pinging database connection" {
		t.Errorf(
			"expected log message 'smartdb: pinging database connection', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_PingContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-ping-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.err = expectedErr

	err = smartDB.PingContext(ctx)
	if err == nil {
		t.Error("expected error pinging database, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrPingDB) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrPingDB, err)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: pinging database connection" {
		t.Errorf(
			"expected log message 'smartdb: pinging database connection', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_PrepareContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-prepare-context",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockStmt := new(sql.Stmt)
	mockDB.stmt = mockStmt

	stmt, err := smartDB.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Errorf("unexpected error preparing statement: %v", err)
	}

	if stmt != mockStmt {
		t.Errorf("expected statement to be %v, got %v", mockStmt, stmt)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if mockDB.query != "SELECT 1" {
		t.Errorf("expected query 'SELECT 1', got '%s'", mockDB.query)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: preparing statement in database" {
		t.Errorf(
			"expected log message 'smartdb: preparing statement in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 log argument key-value pair, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSmartDB_PrepareContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-prepare-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.err = expectedErr

	_, err = smartDB.PrepareContext(ctx, "SELECT 1")
	if err == nil {
		t.Error("expected error preparing statement, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrPrepareInDB) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrPrepareInDB, err)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if mockDB.query != "SELECT 1" {
		t.Errorf("expected query 'SELECT 1', got '%s'", mockDB.query)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: preparing statement in database" {
		t.Errorf(
			"expected log message 'smartdb: preparing statement in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 log argument key-value pair, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSmartDB_PrepareContext_WithTx(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-prepare-context-with-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockStmt := new(sql.Stmt)
	mockTx := new(mockTx)
	mockTx.stmt = mockStmt

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	stmt, err := smartDB.PrepareContext(ctxWithTx, "SELECT 1")
	if err != nil {
		t.Errorf("unexpected error preparing statement with transaction: %v", err)
	}

	if stmt != mockStmt {
		t.Errorf("expected statement to be %v, got %v", mockStmt, stmt)
	}

	if mockTx.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, mockTx.ctx)
	}

	if mockTx.query != "SELECT 1" {
		t.Errorf("expected transaction to receive query 'SELECT 1', got '%s'", mockTx.query)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: preparing statement in transaction" {
		t.Errorf(
			"expected log message 'smartdb: preparing statement in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 log argument key-value pair, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSmartDB_PrepareContext_WithTxError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-prepare-context-with-tx-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockTx := new(mockTx)
	mockTx.err = expectedErr

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	_, err = smartDB.PrepareContext(ctxWithTx, "SELECT 1")
	if err == nil {
		t.Error("expected error preparing statement with transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrPrepareInTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrPrepareInTx, err)
	}

	if mockTx.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, mockTx.ctx)
	}

	if mockTx.query != "SELECT 1" {
		t.Errorf("expected transaction to receive query 'SELECT 1', got '%s'", mockTx.query)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: preparing statement in transaction" {
		t.Errorf(
			"expected log message 'smartdb: preparing statement in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 log argument key-value pair, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSmartDB_QueryContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-query-context",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockRows := new(sql.Rows)
	mockDB.rows = mockRows

	rows, err := smartDB.QueryContext(ctx, "SELECT 1", 5, "arg")
	if err != nil {
		t.Errorf("unexpected error executing query: %v", err)
	}

	if rows != mockDB.rows {
		t.Errorf("expected rows to be %v, got %v", mockDB.rows, rows)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if mockDB.query != "SELECT 1" {
		t.Errorf("expected query 'SELECT 1', got '%s'", mockDB.query)
	}

	if len(mockDB.args) != 2 {
		t.Fatalf("expected 2 query arguments, got %d", len(mockDB.args))
	}

	if mockDB.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockDB.args[0])
	}

	if mockDB.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockDB.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: querying rows in database" {
		t.Errorf(
			"expected log message 'smartdb: querying rows in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Error("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_QueryContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-query-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.err = expectedErr

	_, err = smartDB.QueryContext(ctx, "SELECT 1", 5, "arg")
	if err == nil {
		t.Error("expected error executing query, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrQueryInDB) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrQueryInDB, err)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if mockDB.query != "SELECT 1" {
		t.Errorf("expected query 'SELECT 1', got '%s'", mockDB.query)
	}

	if len(mockDB.args) != 2 {
		t.Fatalf("expected 2 query arguments, got %d", len(mockDB.args))
	}

	if mockDB.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockDB.args[0])
	}

	if mockDB.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockDB.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: querying rows in database" {
		t.Errorf(
			"expected log message 'smartdb: querying rows in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Error("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_QueryContext_WithTx(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-query-context-with-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockRows := new(sql.Rows)
	mockTx := new(mockTx)
	mockTx.rows = mockRows

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	rows, err := smartDB.QueryContext(ctxWithTx, "SELECT 1", 5, "arg")
	if err != nil {
		t.Errorf("unexpected error executing query with transaction: %v", err)
	}

	if rows != mockTx.rows {
		t.Errorf("expected rows to be %v, got %v", mockTx.rows, rows)
	}

	if mockTx.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, mockTx.ctx)
	}

	if mockTx.query != "SELECT 1" {
		t.Errorf("expected transaction to receive query 'SELECT 1', got '%s'", mockTx.query)
	}

	if len(mockTx.args) != 2 {
		t.Fatalf("expected transaction to receive 2 query arguments, got %d", len(mockTx.args))
	}

	if mockTx.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockTx.args[0])
	}

	if mockTx.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockTx.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: querying rows in transaction" {
		t.Errorf(
			"expected log message 'smartdb: querying rows in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_QueryContext_WithTxError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-query-context-with-tx-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockTx := new(mockTx)
	mockTx.err = expectedErr

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	_, err = smartDB.QueryContext(ctxWithTx, "SELECT 1", 5, "arg")
	if err == nil {
		t.Error("expected error executing query with transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrQueryInTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrQueryInTx, err)
	}

	if mockTx.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, mockTx.ctx)
	}

	if mockTx.query != "SELECT 1" {
		t.Errorf("expected transaction to receive query 'SELECT 1', got '%s'", mockTx.query)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: querying rows in transaction" {
		t.Errorf(
			"expected log message 'smartdb: querying rows in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_QueryRowContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-queryrow-context",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockRow := new(sql.Row)
	mockDB.row = mockRow

	row := smartDB.QueryRowContext(ctx, "SELECT 1", 5, "arg")
	if row != mockDB.row {
		t.Errorf("expected row to be %v, got %v", mockDB.row, row)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if mockDB.query != "SELECT 1" {
		t.Errorf("expected query 'SELECT 1', got '%s'", mockDB.query)
	}

	if len(mockDB.args) != 2 {
		t.Fatalf("expected 2 query arguments, got %d", len(mockDB.args))
	}

	if mockDB.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockDB.args[0])
	}

	if mockDB.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockDB.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: querying single row in database" {
		t.Errorf(
			"expected log message 'smartdb: querying single row in database', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_QueryRowContext_WithTx(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-queryrow-context-with-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockRow := new(sql.Row)
	mockTx := new(mockTx)
	mockTx.row = mockRow

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(nil, mockTx))

	row := smartDB.QueryRowContext(ctxWithTx, "SELECT 1", 5, "arg")
	if row != mockTx.row {
		t.Errorf("expected row to be %v, got %v", mockTx.row, row)
	}

	if mockTx.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, mockTx.ctx)
	}

	if mockTx.query != "SELECT 1" {
		t.Errorf("expected transaction to receive query 'SELECT 1', got '%s'", mockTx.query)
	}

	if len(mockTx.args) != 2 {
		t.Fatalf("expected transaction to receive 2 query arguments, got %d", len(mockTx.args))
	}

	if mockTx.args[0] != 5 {
		t.Errorf("expected first query argument to be 5, got %v", mockTx.args[0])
	}

	if mockTx.args[1] != "arg" {
		t.Errorf("expected second query argument to be 'arg', got %v", mockTx.args[1])
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: querying single row in transaction" {
		t.Errorf(
			"expected log message 'smartdb: querying single row in transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Errorf("expected log argument of type smartdb.LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 log argument key-value pairs, got %d", len(arg))
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}

	args, ok := (arg)["args"].([]any)
	if !ok {
		t.Errorf("expected log argument 'args' to be of type []any, got %T", (arg)["args"])
	}

	if len(args) != 2 {
		t.Fatalf("expected log argument 'args' to have 2 elements, got %d", len(args))
	}

	if args[0] != 5 {
		t.Errorf("expected first element of log argument 'args' to be 5, got %v", args[0])
	}

	if args[1] != "arg" {
		t.Errorf("expected second element of log argument 'args' to be 'arg', got %v", args[1])
	}
}

func TestSmartDB_BeginContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-begintx-context",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx := new(sql.Tx)
	mockDB.sqlTx = sqlTx

	beginCtx, err := smartDB.BeginContext(ctx)
	if err != nil {
		t.Errorf("unexpected error beginning transaction: %v", err)
	}

	if beginCtx == nil {
		t.Fatal("expected non-nil transaction context, got nil")
	}

	txCtx, ok := smartdb.TxFromContext(beginCtx)
	if !ok {
		t.Error("expected to retrieve transaction context from context, got nil")
	}

	if txCtx == nil {
		t.Fatal("expected non-nil transaction context, got nil")
	}

	txContext := smartdb.NewTxContext(sqlTx, sqlTx)

	if *txCtx != *txContext {
		t.Errorf("expected transaction context to be %v, got %v", txContext, txCtx)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: beginning top-level transaction" {
		t.Errorf(
			"expected log message 'smartdb: beginning top-level transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_BeginContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-begintx-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	mockDB.err = expectedErr

	_, err = smartDB.BeginContext(ctx)
	if err == nil {
		t.Error("expected error beginning transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrBeginCtxTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrBeginCtxTx, err)
	}

	if mockDB.ctx != ctx {
		t.Errorf("expected context %v, got %v", ctx, mockDB.ctx)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: beginning top-level transaction" {
		t.Errorf(
			"expected log message 'smartdb: beginning top-level transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_BeginContext_WithTx(t *testing.T) {
	t.Parallel()

	mockDB, conn := openMockDBConn(t)
	gen := new(mockGenerator)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-begintx-context-with-existing-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log), smartdb.WithGenerator(gen))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx, err := mockDB.BeginTx(ctx, nil)
	if err != nil {
		t.Errorf("unexpected error beginning transaction: %v", err)
	}

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, new(mockTx)))

	savepointName := "test_savepoint1"
	gen.savepointName = savepointName

	beginCtx, err := smartDB.BeginContext(ctxWithTx)
	if err != nil {
		t.Errorf("unexpected error beginning transaction with existing transaction: %v", err)
	}

	if beginCtx == nil {
		t.Fatal("expected non-nil transaction context, got nil")
	}

	txCtx, ok := smartdb.TxFromContext(beginCtx)
	if !ok {
		t.Error("expected to retrieve transaction context from context, got nil")
	}

	if txCtx == nil {
		t.Fatal("expected non-nil transaction context, got nil")
	}

	if conn.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, conn.ctx)
	}

	if conn.query != "SAVEPOINT "+savepointName {
		t.Errorf("expected query to be 'SAVEPOINT %s', got '%s'", savepointName, conn.query)
	}

	if len(conn.args) != 0 {
		t.Fatalf("expected 0 query arguments, got %d", len(conn.args))
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: beginning nested transaction" {
		t.Errorf(
			"expected log message 'smartdb: beginning nested transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_BeginContext_WithTxSavepointNameError(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	gen := new(mockGenerator)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-begintx-context-with-existing-tx-name-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log), smartdb.WithGenerator(gen))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx := new(sql.Tx)
	mockTx := new(mockTx)
	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, mockTx))

	expectedErr := errors.New("test error")
	gen.err = expectedErr

	_, err = smartDB.BeginContext(ctxWithTx)
	if err == nil {
		t.Error(
			"expected error beginning transaction with existing transaction and name generation error, got nil",
		)
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointName) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrSavepointName, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: beginning nested transaction" {
		t.Errorf(
			"expected log message 'smartdb: beginning nested transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_BeginContext_WithTxNewSavepointError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB, conn := openMockDBConn(t)
	gen := new(mockGenerator)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-begintx-context-with-existing-tx_newsavepoint-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log), smartdb.WithGenerator(gen))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	conn.err = expectedErr

	sqlTx, err := mockDB.BeginTx(ctx, nil)
	if err != nil {
		t.Errorf("unexpected error beginning transaction: %v", err)
	}

	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, new(mockTx)))

	savepointName := "test_savepoint2"
	gen.savepointName = savepointName

	beginCtx, err := smartDB.BeginContext(ctxWithTx)
	if err == nil {
		t.Error(
			"expected error beginning transaction with existing transaction and new savepoint error, got nil",
		)
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrCreateSavepoint) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrCreateSavepoint, err)
	}

	if beginCtx != nil {
		t.Fatal("expected nil transaction context, got non-nil")
	}

	if conn.ctx != ctxWithTx {
		t.Errorf("expected transaction to receive context %v, got %v", ctxWithTx, conn.ctx)
	}

	if conn.query != "SAVEPOINT "+savepointName {
		t.Errorf("expected query to be 'SAVEPOINT %s', got '%s'", savepointName, conn.query)
	}

	if len(conn.args) != 0 {
		t.Fatalf("expected 0 query arguments, got %d", len(conn.args))
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: beginning nested transaction" {
		t.Errorf(
			"expected log message 'smartdb: beginning nested transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_CommitContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-commit-context",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx := new(sql.Tx)
	mockTx := new(mockTx)
	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, mockTx))

	err = smartDB.CommitContext(ctxWithTx)
	if err != nil {
		t.Errorf("unexpected error committing transaction: %v", err)
	}

	if !mockTx.committed {
		t.Error("expected transaction to be committed, but it was not")
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: committing transaction" {
		t.Errorf(
			"expected log message 'smartdb: committing transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_CommitContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-commit-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx := new(sql.Tx)
	mockTx := new(mockTx)
	mockTx.err = expectedErr
	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, mockTx))

	err = smartDB.CommitContext(ctxWithTx)
	if err == nil {
		t.Error("expected error committing transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrCommitTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrCommitTx, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: committing transaction" {
		t.Errorf(
			"expected log message 'smartdb: committing transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_CommitContext_NoTx(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-commit-context-no-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	err = smartDB.CommitContext(ctx)
	if err == nil {
		t.Error("expected error committing transaction with no transaction in context, got nil")
	}

	if !errors.Is(err, smartdb.ErrNoTxInContext) {
		t.Errorf("expected error to be %v, got %v", smartdb.ErrNoTxInContext, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: committing transaction" {
		t.Errorf(
			"expected log message 'smartdb: committing transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_RollbackContext(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-rollback-context",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx := new(sql.Tx)
	mockTx := new(mockTx)
	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, mockTx))

	err = smartDB.RollbackContext(ctxWithTx)
	if err != nil {
		t.Errorf("unexpected error rolling back transaction: %v", err)
	}

	if !mockTx.rolledBack {
		t.Error("expected transaction to be rolled back, but it was not")
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: rolling back transaction" {
		t.Errorf(
			"expected log message 'smartdb: rolling back transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_RollbackContext_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-rollback-context-error",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	sqlTx := new(sql.Tx)
	mockTx := new(mockTx)
	mockTx.err = expectedErr
	ctxWithTx := smartdb.ContextWithTx(ctx, smartdb.NewTxContext(sqlTx, mockTx))

	err = smartDB.RollbackContext(ctxWithTx)
	if err == nil {
		t.Error("expected error rolling back transaction, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrRollbackTx) {
		t.Errorf("expected error to wrap %v, got %v", smartdb.ErrRollbackTx, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctxWithTx {
		t.Errorf("expected log context %v, got %v", ctxWithTx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: rolling back transaction" {
		t.Errorf(
			"expected log message 'smartdb: rolling back transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

func TestSmartDB_RollbackContext_NoTx(t *testing.T) {
	t.Parallel()

	mockDB := new(mockContextDatabaser)
	log := new(mockLogger)
	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-smartdb-rollback-context-no-tx",
	)

	smartDB, err := smartdb.New(mockDB, smartdb.WithLogger(log))
	if err != nil {
		t.Errorf("unexpected error creating SmartDB: %v", err)
	}

	if smartDB == nil {
		t.Fatal("expected non-nil SmartDB instance, got nil")
	}

	err = smartDB.RollbackContext(ctx)
	if err == nil {
		t.Error("expected error rolling back transaction with no transaction in context, got nil")
	}

	if !errors.Is(err, smartdb.ErrNoTxInContext) {
		t.Errorf("expected error to be %v, got %v", smartdb.ErrNoTxInContext, err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context %v, got %v", ctx, log.debugLog[0].ctx)
	}

	if log.debugLog[0].msg != "smartdb: rolling back transaction" {
		t.Errorf(
			"expected log message 'smartdb: rolling back transaction', got '%s'",
			log.debugLog[0].msg,
		)
	}

	if len(log.debugLog[0].args) != 0 {
		t.Fatalf("expected 0 log arguments, got %d", len(log.debugLog[0].args))
	}
}

type mockDriver struct {
	pull map[string]driver.Conn
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	mockDriverMu.Lock()
	defer mockDriverMu.Unlock()

	conn, ok := d.pull[name]
	if !ok {
		return nil, fmt.Errorf("mockDriver: no connection found for name '%s'", name)
	}

	return conn, nil
}

type mockDriverConn struct {
	ctx    context.Context
	query  string
	args   []any
	result driver.Result
	err    error
}

func (c *mockDriverConn) Prepare(string) (driver.Stmt, error) {
	return nil, nil
}

func (c *mockDriverConn) Close() error {
	return nil
}

func (c *mockDriverConn) Begin() (driver.Tx, error) {
	return nil, nil
}

func (c *mockDriverConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	c.ctx = ctx
	c.query = query
	c.args = make([]any, len(args))

	for i, arg := range args {
		c.args[i] = arg.Value
	}

	return c.result, c.err
}

var (
	mockDriverOnce     sync.Once
	mockDriverConnPull map[string]driver.Conn
	mockDriverMu       sync.Mutex
)

func registerMockDriver() {
	mockDriverOnce.Do(func() {
		mockDriverConnPull = make(map[string]driver.Conn)

		mockDriver := &mockDriver{
			pull: mockDriverConnPull,
		}

		sql.Register("mock", mockDriver)
	})
}

func openMockDBConn(t *testing.T) (*sql.DB, *mockDriverConn) {
	t.Helper()

	registerMockDriver()

	mockDriverMu.Lock()
	defer mockDriverMu.Unlock()

	connName := fmt.Sprintf("conn-%d", len(mockDriverConnPull))
	driverConn := new(mockDriverConn)

	mockDriverConnPull[connName] = driverConn

	db, err := sql.Open("mock", connName)
	if err != nil {
		t.Fatalf("failed to open mock database: %v", err)
	}

	return db, driverConn
}

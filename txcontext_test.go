package smartdb_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/yuppyweb/smartdb"
)

type mockTxContextKey struct{}

type mockTx struct {
	ctx        context.Context
	query      string
	args       []any
	result     sql.Result
	stmt       *sql.Stmt
	rows       *sql.Rows
	row        *sql.Row
	committed  bool
	rolledBack bool
	err        error
}

func (m *mockTx) Commit() error {
	m.committed = true

	return m.err
}

func (m *mockTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.ctx = ctx
	m.query = query
	m.args = args

	return m.result, m.err
}

func (m *mockTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	m.ctx = ctx
	m.query = query

	return m.stmt, m.err
}

func (m *mockTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	m.ctx = ctx
	m.query = query
	m.args = args

	return m.rows, m.err
}

func (m *mockTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	m.ctx = ctx
	m.query = query
	m.args = args

	return m.row
}

func (m *mockTx) Rollback() error {
	m.rolledBack = true

	return m.err
}

var _ smartdb.Tx = (*mockTx)(nil)

func TestTxContext(t *testing.T) {
	t.Parallel()

	mockTx := new(mockTx)
	txContext := smartdb.NewTxContext(nil, mockTx)

	ctx := smartdb.ContextWithTx(context.Background(), txContext)

	txCtx, ok := smartdb.TxFromContext(ctx)
	if !ok {
		t.Fatal("expected to retrieve TxContext from context, but got none")
	}

	if txCtx != txContext {
		t.Fatal("expected to retrieve the same TxContext instance from context")
	}
}

func TestTxContext_EmptyContext(t *testing.T) {
	t.Parallel()

	txCtx, ok := smartdb.TxFromContext(context.Background())
	if ok {
		t.Fatal("expected no TxContext in context, but found one")
	}

	if txCtx != nil {
		t.Fatal("expected TxContext to be nil when not found in context")
	}
}

package smartdb_test

import (
	"context"
	"database/sql"

	"github.com/yuppyweb/smartdb"
)

type mockTxContextKey struct{}

type mockTx struct {
	ctx    context.Context
	query  string
	args   []any
	result sql.Result
	stmt   *sql.Stmt
	rows   *sql.Rows
	row    *sql.Row
	err    error
}

func (m *mockTx) Commit() error {
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
	return m.err
}

var _ smartdb.Tx = (*mockTx)(nil)

package smartdb

import (
	"context"
	"database/sql"
)

// Tx defines the interface for transaction operations. It extends the core SQL transaction
// methods and is implemented by both sql.Tx and Savepoint to provide a unified interface
// for transaction and savepoint operations.
type Tx interface {
	Commit() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Rollback() error
}

type txContextKey struct{}

// txContext wraps a transaction and holds references to both the base transaction and the
// actual transaction to execute on (which may be a Savepoint for nested transactions).
// baseTx persists across nesting levels to maintain access to the root transaction,
// while execTx points to the current execution target (sql.Tx for top-level, Savepoint for nested).
type txContext struct {
	baseTx *sql.Tx // Root transaction maintained across all nesting levels
	execTx Tx      // Current execution target (top-level Tx or Savepoint)
}

// contextWithTx returns a new context with the provided TxContext value attached.
// This is used to make a transaction available to SmartDB methods via context.
func contextWithTx(ctx context.Context, tx *txContext) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// txFromContext retrieves a TxContext from the provided context if one exists.
// It returns the TxContext and a boolean indicating whether a transaction was found.
func txFromContext(ctx context.Context) (*txContext, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*txContext)

	return tx, ok
}

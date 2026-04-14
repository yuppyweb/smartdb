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

// TxContext wraps a transaction and holds references to both the base transaction and the
// actual transaction to execute on (which may be a Savepoint for nested transactions).
// baseTx persists across nesting levels to maintain access to the root transaction,
// while execTx points to the current execution target (sql.Tx for top-level, Savepoint for nested).
type TxContext struct {
	baseTx *sql.Tx // Root transaction maintained across all nesting levels
	execTx Tx      // Current execution target (top-level Tx or Savepoint)
}

// NewTxContext creates a new TxContext with the provided base and execution transactions.
// baseTx is the root transaction maintained across all nesting levels,
// while execTx is the current execution target (sql.Tx for top-level, Savepoint for nested).
func NewTxContext(baseTx *sql.Tx, execTx Tx) *TxContext {
	return &TxContext{
		baseTx: baseTx,
		execTx: execTx,
	}
}

// ContextWithTx returns a new context with the provided TxContext value attached.
// This is used to make a transaction available to SmartDB methods via context.
func ContextWithTx(ctx context.Context, tx *TxContext) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// TxFromContext retrieves a TxContext from the provided context if one exists.
// It returns the TxContext and a boolean indicating whether a transaction was found.
func TxFromContext(ctx context.Context) (*TxContext, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*TxContext)

	return tx, ok
}

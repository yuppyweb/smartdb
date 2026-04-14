// Package smartdb provides smart database transaction management with support for
// nested transactions using database savepoints. It implements context-aware transaction
// handling and allows for fine-grained control over transaction execution with automatic
// savepoint management for nested transactions.
package smartdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const packageName = "smartdb"

// Error variables for smartdb operations. These represent various error conditions
// that can occur during database operations, transaction management, and savepoint handling.
var (
	ErrDatabaseNil     = errors.New(packageName + ": db cannot be nil")
	ErrBeginSQLTx      = errors.New(packageName + ": failed to begin SQL transaction")
	ErrBeginCtxTx      = errors.New(packageName + ": failed to begin transaction in context")
	ErrExecInTx        = errors.New(packageName + ": failed to exec in transaction")
	ErrExecInDB        = errors.New(packageName + ": failed to exec in database")
	ErrPingDB          = errors.New(packageName + ": failed to ping database")
	ErrPrepareInTx     = errors.New(packageName + ": failed to prepare statement in transaction")
	ErrPrepareInDB     = errors.New(packageName + ": failed to prepare statement in database")
	ErrQueryInTx       = errors.New(packageName + ": failed to query in transaction")
	ErrQueryInDB       = errors.New(packageName + ": failed to query in database")
	ErrSavepointName   = errors.New(packageName + ": failed to generate savepoint name")
	ErrCreateSavepoint = errors.New(packageName + ": failed to create savepoint")
	ErrCommitTx        = errors.New(packageName + ": failed to commit transaction")
	ErrRollbackTx      = errors.New(packageName + ": failed to rollback transaction")
	ErrNoTxInContext   = errors.New(packageName + ": no transaction found in context")
)

// ContextDatabaser defines the interface for database operations that accept a context.
// It mirrors the context-aware methods of database/sql.DB and is used to provide
// flexible database abstraction throughout smartdb.
type ContextDatabaser interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ContextTransactor defines the interface for transaction management operations that accept a context.
// It provides methods for beginning, committing, and rolling back transactions within a context.
type ContextTransactor interface {
	BeginContext(ctx context.Context) (context.Context, error)
	CommitContext(ctx context.Context) error
	RollbackContext(ctx context.Context) error
}

// Option is a functional option type for configuring SmartDB instances.
// Options are passed to the New function to customize SmartDB behavior.
type Option func(*SmartDB)

// SmartDB is the main struct that provides smart database transaction management.
// It implements both ContextDatabaser and ContextTransactor interfaces and handles
// transaction routing and nested transaction support through savepoints.
type SmartDB struct {
	db     ContextDatabaser
	txOpts *sql.TxOptions
	gen    *Generator
	log    Logger
}

// New creates a new SmartDB instance with the provided database connection and options.
// It initializes SmartDB with a default no-op logger and a default savepoint namer.
// Returns an error if the provided database is nil.
func New(db ContextDatabaser, opts ...Option) (*SmartDB, error) {
	if db == nil {
		return nil, ErrDatabaseNil
	}

	smartDB := &SmartDB{
		db:     db,
		txOpts: nil,
		gen:    NewGenerator(nil),
		log:    NewNopLogger(),
	}

	for _, opt := range opts {
		opt(smartDB)
	}

	return smartDB, nil
}

// BeginTx begins a new SQL transaction with the specified options.
// It delegates to the underlying database's BeginTx method.
func (s *SmartDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	s.log.Debug(ctx, packageName+": beginning sql transaction", LogArgs{
		"options": opts,
	})

	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBeginSQLTx, err)
	}

	return tx, nil
}

// ExecContext executes a query without returning any rows.
// If a transaction is present in the context, the query is executed within that transaction;
// otherwise, it is executed directly on the database.
func (s *SmartDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx, ok := TxFromContext(ctx); ok {
		s.log.Debug(ctx, packageName+": executing query in transaction", LogArgs{
			"query": query,
			"args":  args,
		})

		result, err := tx.execTx.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrExecInTx, err)
		}

		return result, nil
	}

	s.log.Debug(ctx, packageName+": executing query in database", LogArgs{
		"query": query,
		"args":  args,
	})

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrExecInDB, err)
	}

	return result, nil
}

// PingContext verifies a connection to the database is still alive.
// It wraps the underlying database's PingContext call.
func (s *SmartDB) PingContext(ctx context.Context) error {
	s.log.Debug(ctx, packageName+": pinging database connection")

	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrPingDB, err)
	}

	return nil
}

// PrepareContext prepares a statement for later execution.
// If a transaction is present in the context, the statement is prepared within that transaction;
// otherwise, it is prepared on the database.
func (s *SmartDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if tx, ok := TxFromContext(ctx); ok {
		s.log.Debug(ctx, packageName+": preparing statement in transaction", LogArgs{
			"query": query,
		})

		stmt, err := tx.execTx.PrepareContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPrepareInTx, err)
		}

		return stmt, nil
	}

	s.log.Debug(ctx, packageName+": preparing statement in database", LogArgs{
		"query": query,
	})

	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPrepareInDB, err)
	}

	return stmt, nil
}

// QueryContext executes a query and returns rows that can be iterated over.
// If a transaction is present in the context, the query is executed within that transaction;
// otherwise, it is executed directly on the database.
func (s *SmartDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx, ok := TxFromContext(ctx); ok {
		s.log.Debug(ctx, packageName+": querying rows in transaction", LogArgs{
			"query": query,
			"args":  args,
		})

		rows, err := tx.execTx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrQueryInTx, err)
		}

		return rows, nil
	}

	s.log.Debug(ctx, packageName+": querying rows in database", LogArgs{
		"query": query,
		"args":  args,
	})

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrQueryInDB, err)
	}

	return rows, nil
}

// QueryRowContext executes a query that is expected to return at most one row.
// If a transaction is present in the context, the query is executed within that transaction;
// otherwise, it is executed directly on the database.
func (s *SmartDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if tx, ok := TxFromContext(ctx); ok {
		s.log.Debug(ctx, packageName+": querying single row in transaction", LogArgs{
			"query": query,
			"args":  args,
		})

		return tx.execTx.QueryRowContext(ctx, query, args...)
	}

	s.log.Debug(ctx, packageName+": querying single row in database", LogArgs{
		"query": query,
		"args":  args,
	})

	return s.db.QueryRowContext(ctx, query, args...)
}

// BeginContext begins a new transaction or savepoint and returns a new context that contains
// the transaction. If a transaction already exists in the context, a nested transaction (savepoint)
// is created; otherwise, a top-level transaction is started.
//
// For nested transactions, baseTx always references the root transaction while execTx points
// to the savepoint, ensuring all operations ultimately execute against the underlying database
// transaction while preserving the savepoint hierarchy.
func (s *SmartDB) BeginContext(ctx context.Context) (context.Context, error) {
	if tx, ok := TxFromContext(ctx); ok {
		s.log.Debug(ctx, packageName+": beginning nested transaction")

		name, err := s.gen.SavepointName()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrSavepointName, err)
		}

		sp, err := NewSavepoint(ctx, name, tx.baseTx, s.log)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCreateSavepoint, err)
		}

		return ContextWithTx(ctx, NewTxContext(tx.baseTx, sp)), nil
	}

	s.log.Debug(ctx, packageName+": beginning top-level transaction")

	tx, err := s.BeginTx(ctx, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBeginCtxTx, err)
	}

	return ContextWithTx(ctx, NewTxContext(tx, tx)), nil
}

// CommitContext commits the transaction or savepoint associated with the provided context.
// It returns an error if no transaction is found in the context.
func (s *SmartDB) CommitContext(ctx context.Context) error {
	s.log.Debug(ctx, packageName+": committing transaction")

	tx, ok := TxFromContext(ctx)
	if !ok {
		return ErrNoTxInContext
	}

	if err := tx.execTx.Commit(); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitTx, err)
	}

	return nil
}

// RollbackContext rolls back the transaction or savepoint associated with the provided context.
// It returns an error if no transaction is found in the context.
func (s *SmartDB) RollbackContext(ctx context.Context) error {
	s.log.Debug(ctx, packageName+": rolling back transaction")

	tx, ok := TxFromContext(ctx)
	if !ok {
		return ErrNoTxInContext
	}

	if err := tx.execTx.Rollback(); err != nil {
		return fmt.Errorf("%w: %w", ErrRollbackTx, err)
	}

	return nil
}

var (
	_ ContextDatabaser  = (*SmartDB)(nil)
	_ ContextTransactor = (*SmartDB)(nil)
)

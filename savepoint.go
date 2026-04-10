package smartdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
)

const (
	maxSavepointNameLen = 32
)

// Error variables for savepoint operations. These represent various error conditions
// that can occur when creating, managing, or executing operations within savepoints.
var (
	ErrSavepointNameTooLong = fmt.Errorf(
		"%s: savepoint name exceeds maximum length of %d characters",
		packageName,
		maxSavepointNameLen,
	)
	ErrSavepointEmptyName   = errors.New(packageName + ": savepoint name cannot be empty")
	ErrSavepointInvalidName = errors.New(
		packageName + ": savepoint name contains invalid characters",
	)
	ErrSavepointNilTx    = errors.New(packageName + ": transaction cannot be nil")
	ErrSavepointIsDone   = errors.New(packageName + ": savepoint is already done")
	ErrSavepointCreate   = errors.New(packageName + ": failed to create savepoint")
	ErrSavepointRelease  = errors.New(packageName + ": failed to release savepoint")
	ErrSavepointRollback = errors.New(packageName + ": failed to rollback to savepoint")
	ErrSavepointExec     = errors.New(packageName + ": failed to exec in savepoint")
	ErrSavepointPrepare  = errors.New(packageName + ": failed to prepare statement in savepoint")
	ErrSavepointQuery    = errors.New(packageName + ": failed to query in savepoint")

	// savepointNamePattern enforces SQL identifier rules to ensure names are valid across
	// all major relational databases without escaping requirements.
	// Pattern: starts with letter or underscore, followed by letters, digits, or underscores.
	savepointNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// Savepoint represents a database savepoint within a transaction.
// It implements the Tx interface and provides nested transaction capabilities through
// the database savepoint mechanism. A savepoint stores its context for use in Commit
// and Rollback methods which do not receive a context parameter.
type Savepoint struct {
	// ctx is stored for Commit and Rollback methods which don't receive context parameter.
	// This is the same pattern used by sql.Tx.
	ctx  context.Context //nolint:containedctx
	name string
	tx   Tx
	done atomic.Bool
	mu   sync.RWMutex
	log  Logger
}

// NewSavepoint creates a new savepoint with the specified name within the given transaction.
// It validates the savepoint name and creates the savepoint in the database using the
// SAVEPOINT SQL command. Returns an error if the name is invalid, too long, or if the SQL
// command fails.
func NewSavepoint(ctx context.Context, name string, tx Tx, log Logger) (*Savepoint, error) {
	if log == nil {
		log = NewNopLogger()
	}

	log.Debug(ctx, packageName+": creating savepoint", LogArgs{
		"savepoint": name,
	})

	if name == "" {
		return nil, ErrSavepointEmptyName
	}

	if len(name) > maxSavepointNameLen {
		return nil, ErrSavepointNameTooLong
	}

	if !savepointNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: %s", ErrSavepointInvalidName, name)
	}

	if tx == nil {
		return nil, ErrSavepointNilTx
	}

	query := "SAVEPOINT " + name

	if _, err := tx.ExecContext(ctx, query); err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrSavepointCreate, name, err)
	}

	return &Savepoint{
		ctx:  ctx,
		name: name,
		tx:   tx,
		done: atomic.Bool{},
		mu:   sync.RWMutex{},
		log:  log,
	}, nil
}

// Commit releases the savepoint using the RELEASE SAVEPOINT command.
// After commitment, further operations on this savepoint return an error.
func (sp *Savepoint) Commit() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.log.Debug(sp.ctx, packageName+": releasing savepoint", LogArgs{
		"savepoint": sp.name,
	})

	if sp.done.Load() {
		return sp.errSavepointIsDone()
	}

	query := "RELEASE SAVEPOINT " + sp.name

	if _, err := sp.tx.ExecContext(sp.ctx, query); err != nil {
		return fmt.Errorf("%w %s: %w", ErrSavepointRelease, sp.name, err)
	}

	sp.done.Store(true)

	return nil
}

// ExecContext executes a query within the savepoint without returning any rows.
// Returns an error if the savepoint is already committed or rolled back.
func (sp *Savepoint) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	sp.log.Debug(ctx, packageName+": executing query in savepoint", LogArgs{
		"savepoint": sp.name,
		"query":     query,
		"args":      args,
	})

	if sp.done.Load() {
		return nil, sp.errSavepointIsDone()
	}

	result, err := sp.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrSavepointExec, sp.name, err)
	}

	return result, nil
}

// PrepareContext prepares a statement within the savepoint for later execution.
// Returns an error if the savepoint is already committed or rolled back.
func (sp *Savepoint) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	sp.log.Debug(ctx, packageName+": preparing statement in savepoint", LogArgs{
		"savepoint": sp.name,
		"query":     query,
	})

	if sp.done.Load() {
		return nil, sp.errSavepointIsDone()
	}

	// statement is returned to caller, this method doesn't close it
	stmt, err := sp.tx.PrepareContext(ctx, query) //nolint:sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrSavepointPrepare, sp.name, err)
	}

	return stmt, nil
}

// QueryContext executes a query within the savepoint and returns rows that can be iterated over.
// Returns an error if the savepoint is already committed or rolled back.
func (sp *Savepoint) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	sp.log.Debug(ctx, packageName+": querying rows in savepoint", LogArgs{
		"savepoint": sp.name,
		"query":     query,
		"args":      args,
	})

	if sp.done.Load() {
		return nil, sp.errSavepointIsDone()
	}

	// rows are returned to caller, this method doesn't close them
	rows, err := sp.tx.QueryContext(ctx, query, args...) //nolint:sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrSavepointQuery, sp.name, err)
	}

	return rows, nil
}

// QueryRowContext executes a query within the savepoint that is expected to return at most one row.
// Returns an error if the savepoint is already committed or rolled back.
func (sp *Savepoint) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	sp.log.Debug(ctx, packageName+": querying single row in savepoint", LogArgs{
		"savepoint": sp.name,
		"query":     query,
		"args":      args,
	})

	if sp.done.Load() {
		sp.log.Error(ctx, sp.errSavepointIsDone())

		var cancel context.CancelFunc

		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}

	return sp.tx.QueryRowContext(ctx, query, args...)
}

// Rollback rolls back to the savepoint using the ROLLBACK TO SAVEPOINT command.
// After rollback, the savepoint cannot be used again. The base transaction remains active.
func (sp *Savepoint) Rollback() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.log.Debug(sp.ctx, packageName+": rolling back to savepoint", LogArgs{
		"savepoint": sp.name,
	})

	if sp.done.Load() {
		return sp.errSavepointIsDone()
	}

	query := "ROLLBACK TO SAVEPOINT " + sp.name

	if _, err := sp.tx.ExecContext(sp.ctx, query); err != nil {
		return fmt.Errorf("%w %s: %w", ErrSavepointRollback, sp.name, err)
	}

	sp.done.Store(true)

	return nil
}

func (sp *Savepoint) errSavepointIsDone() error {
	return fmt.Errorf("%w: %s", ErrSavepointIsDone, sp.name)
}

var _ Tx = (*Savepoint)(nil)

# 🧬 SmartDB

[![Go Version](https://img.shields.io/github/go-mod/go-version/yuppyweb/smartdb)](https://github.com/yuppyweb/smartdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/yuppyweb/smartdb)](https://goreportcard.com/report/github.com/yuppyweb/smartdb)
[![Downloads](https://img.shields.io/github/downloads/yuppyweb/smartdb/total.svg)](https://github.com/yuppyweb/smartdb/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

SmartDB is a Go library for context-based transaction management. It's a decorator for the standard database/sql.DB package that implements context-aware database methods and provides seamless transaction management through Go's context, handling nested transactions automatically using savepoints.

## 📦 Installation

```bash
go get github.com/yuppyweb/smartdb
```

## 🚀 Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "log"
    
    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/yuppyweb/smartdb"
)

func main() {
    // Open database connection
    db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Initialize SmartDB
    sdb, err := smartdb.New(db)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Begin a transaction
    txCtx, err := sdb.BeginContext(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // Use the transaction context for database operations
    _, err = sdb.ExecContext(txCtx, "INSERT INTO users (name) VALUES ($1)", "Alice")
    if err != nil {
        sdb.RollbackContext(txCtx)
        log.Fatal(err)
    }

    // Commit the transaction
    if err := sdb.CommitContext(txCtx); err != nil {
        log.Fatal(err)
    }
}
```

## 🎯 Core Features

### Nested Transaction Support 🪆

SmartDB automatically handles nested transactions using database savepoints, eliminating the need for manual savepoint management:

```go
// Outer transaction
txCtx, _ := sdb.BeginContext(ctx)

// Insert first record
sdb.ExecContext(txCtx, "INSERT INTO logs (msg) VALUES ($1)", "Step 1")

// Inner transaction (automatically creates a savepoint)
innerCtx, _ := sdb.BeginContext(txCtx)

// If this fails, only the inner transaction is rolled back
sdb.ExecContext(innerCtx, "INSERT INTO logs (msg) VALUES ($1)", "Step 2")

// Rollback inner transaction automatically rolls back to savepoint
sdb.RollbackContext(innerCtx)

// Outer transaction still active and can continue
sdb.ExecContext(txCtx, "INSERT INTO logs (msg) VALUES ($1)", "Step 3")

sdb.CommitContext(txCtx)
```

### Transaction State Management 💾

SmartDB provides a clean interface for transaction operations:

```go
// Unified transaction interface for both direct transactions and savepoints
var tx smartdb.ContextTransactor = sdb

txCtx, err := tx.BeginContext(ctx)      // Start transaction/savepoint
err = tx.CommitContext(txCtx)            // Commit transaction/savepoint
err = tx.RollbackContext(txCtx)          // Rollback transaction/savepoint
```

### Custom Logging 📝

Integrate your own logging solution with the pluggable Logger interface:

```go
type MyLogger struct{}

func (ml *MyLogger) Debug(ctx context.Context, msg string, args ...any) {
    // Your debug logging implementation
}

func (ml *MyLogger) Error(ctx context.Context, err error, args ...any) {
    // Your error logging implementation
}

// Use custom logger
sdb, _ := smartdb.New(db, smartdb.WithLogger(&MyLogger{}))
```

### Automatic Savepoint Naming 🎲

SmartDB generates unique, SQL-compliant savepoint names automatically:

```go
// No need to manage savepoint names manually
// Format: sp<timestamp_hex><random_hex>
// Example: sp65b3f4a3d2e1c9f5
```

## 🔧 API Reference

### SmartDB Types & Interfaces 🏗️

#### `ContextDatabaser` Interface
Abstracts database operations that accept context:

```go
type ContextDatabaser interface {
    BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    PingContext(ctx context.Context) error
    PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
```

#### `ContextTransactor` Interface
Defines transaction management operations:

```go
type ContextTransactor interface {
    BeginContext(ctx context.Context) (context.Context, error)
    CommitContext(ctx context.Context) error
    RollbackContext(ctx context.Context) error
}
```

#### `Logger` Interface
Customizable logging interface:

```go
type Logger interface {
    Debug(ctx context.Context, msg string, args ...any)
    Error(ctx context.Context, err error, args ...any)
}
```

### Main Functions 🛠️

#### `New(db ContextDatabaser, opts ...Option) (*SmartDB, error)`
Creates a new SmartDB instance:

```go
sdb, err := smartdb.New(sqlDB, smartdb.WithTraceLevel())
```

#### `WithLogger(logger Logger) Option`
Sets a custom logger for SmartDB:

```go
sdb, _ := smartdb.New(db, smartdb.WithLogger(myLogger))
```

#### `WithTxOptions(opts *sql.TxOptions) Option`
Configures transaction options:

```go
sdb, _ := smartdb.New(db, smartdb.WithTxOptions(&sql.TxOptions{
    Isolation: sql.LevelSerializable,
}))
```

#### `WithGenerator(generator *Generator) Option`
Sets a custom savepoint name generator:

```go
sdb, _ := smartdb.New(db, smartdb.WithGenerator(smartdb.NewGenerator(nil)))
```

## ⚠️ Error Handling

SmartDB provides comprehensive error definitions for better error handling:

```go
var (
    ErrDatabaseNil        // Database connection is nil
    ErrBeginSQLTx         // Failed to begin SQL transaction
    ErrBeginCtxTx         // Failed to begin transaction in context
    ErrExecInTx           // Failed to exec in transaction
    ErrExecInDB           // Failed to exec in database
    ErrPingDB             // Failed to ping database
    ErrPrepareInTx        // Failed to prepare statement in transaction
    ErrPrepareInDB        // Failed to prepare statement in database
    ErrQueryInTx          // Failed to query in transaction
    ErrQueryInDB          // Failed to query in database
    ErrSavepointName      // Failed to generate savepoint name
    ErrCreateSavepoint    // Failed to create savepoint
    ErrCommitTx           // Failed to commit transaction
    ErrRollbackTx         // Failed to rollback transaction
    ErrNoTxInContext      // No transaction found in context
)
```

Handle errors gracefully:

```go
if err := sdb.CommitContext(txCtx); err != nil {
    if errors.Is(err, smartdb.ErrCommitTx) {
        log.Printf("Transaction commit failed: %v", err)
    }
    // Handle other error types...
}
```

## 🎓 Examples

### Multi-Step Business Transaction 💳

```go
// Complete example with rollback on error
func ProcessOrder(ctx context.Context, sdb *smartdb.SmartDB, orderID int) error {
    txCtx, err := sdb.BeginContext(ctx)
    if err != nil {
        return fmt.Errorf("begin transaction: %w", err)
    }

    // Deduct inventory
    if _, err := sdb.ExecContext(txCtx, 
        "UPDATE inventory SET qty = qty - 1 WHERE product_id = $1", 
        orderID); err != nil {
        sdb.RollbackContext(txCtx)
        return fmt.Errorf("update inventory: %w", err)
    }

    // Record transaction
    if _, err := sdb.ExecContext(txCtx, 
        "INSERT INTO transactions (order_id) VALUES ($1)", 
        orderID); err != nil {
        sdb.RollbackContext(txCtx)
        return fmt.Errorf("record transaction: %w", err)
    }

    // Commit all changes atomically
    if err := sdb.CommitContext(txCtx); err != nil {
        return fmt.Errorf("commit transaction: %w", err)
    }

    return nil
}
```

### Recovering from Inner Transaction Failure 🔀

```go
func ProcessWithRecovery(ctx context.Context, sdb *smartdb.SmartDB) error {
    txCtx, _ := sdb.BeginContext(ctx)

    // Main operation succeeds
    sdb.ExecContext(txCtx, "INSERT INTO main_log (msg) VALUES ($1)", "Main operation")

    // Try secondary operation in nested transaction
    innerCtx, _ := sdb.BeginContext(txCtx)
    _, err := sdb.ExecContext(innerCtx, "INSERT INTO secondary VALUES (NULL)") // Fails
    
    if err != nil {
        // Rollback only the inner transaction
        sdb.RollbackContext(innerCtx)
        // Main operation is still intact!
    }

    // Continue with main transaction
    sdb.CommitContext(txCtx) // Only main operation is committed
}
```

## ⚙️ Configuration

SmartDB supports flexible configuration through functional options:

```go
sdb, err := smartdb.New(db,
    smartdb.WithLogger(customLogger),
    smartdb.WithTxOptions(&sql.TxOptions{
        Isolation: sql.LevelReadCommitted,
        ReadOnly:  false,
    }),
    smartdb.WithGenerator(smartdb.NewGenerator(customReader)),
)
```

## 🛡️ Thread Safety

SmartDB is fully thread-safe:

✓ Uses `sync.RWMutex` for transaction state protection  
✓ Atomic operations for flags and counters  
✓ Safe concurrent access from multiple goroutines  
✓ Proper context passing for cancellation signals  

```go
// Safe to use from multiple goroutines
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Go(func() {
        ctx := context.Background()
        txCtx, err := sdb.BeginContext(ctx)
        if err != nil {
            log.Printf("Error starting transaction: %v", err)
            return
        }
        // Your operations...
        if err := sdb.CommitContext(txCtx); err != nil {
            log.Printf("Error committing transaction: %v", err)
            sdb.RollbackContext(txCtx)
        }
    })
}
wg.Wait()
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

For more information about the MIT License, visit [opensource.org/licenses/MIT](https://opensource.org/licenses/MIT).

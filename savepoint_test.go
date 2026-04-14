package smartdb_test

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/yuppyweb/smartdb"
)

func TestNewSavepoint_EmptyName(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "new-test-empty-name")
	log := new(mockLogger)

	sp, err := smartdb.NewSavepoint(ctx, "", nil, log)
	if err == nil {
		t.Errorf("expected error for empty savepoint name, got nil")
	}

	if sp != nil {
		t.Errorf("expected nil savepoint on error, got non-nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointEmptyName) {
		t.Errorf("expected ErrSavepointEmptyName, got %v", err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[0].msg != "smartdb: creating savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[0].msg)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "" {
		t.Errorf("expected log argument 'savepoint' to be empty string, got %v", (arg)["savepoint"])
	}
}

func TestNewSavepoint_LongName(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		spName string
	}{
		{
			name:   "name 33 characters long",
			spName: "sp_" + strings.Repeat("_", 30),
		},
		{
			name:   "name 100 characters long",
			spName: "sp_" + strings.Repeat("a", 97),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.WithValue(
				context.Background(),
				&mockTxContextKey{},
				"new-test-long-name",
			)
			log := new(mockLogger)

			sp, err := smartdb.NewSavepoint(ctx, tc.spName, nil, log)
			if err == nil {
				t.Errorf("expected error for long savepoint name, got nil")
			}

			if sp != nil {
				t.Errorf("expected nil savepoint on error, got non-nil")
			}

			if !errors.Is(err, smartdb.ErrSavepointNameTooLong) {
				t.Errorf("expected ErrSavepointNameTooLong, got %v", err)
			}

			if len(log.debugLog) != 1 {
				t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
			}

			if log.debugLog[0].ctx != ctx {
				t.Errorf(
					"expected log context to be the same as input context, got different context",
				)
			}

			if log.debugLog[0].msg != "smartdb: creating savepoint" {
				t.Errorf("unexpected debug log message: %s", log.debugLog[0].msg)
			}

			if len(log.debugLog[0].args) != 1 {
				t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[0].args))
			}

			arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
			if !ok {
				t.Fatalf(
					"expected log argument to be of type LogArgs, got %T",
					log.debugLog[0].args[0],
				)
			}

			if len(arg) != 1 {
				t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
			}

			if (arg)["savepoint"] != tc.spName {
				t.Errorf(
					"expected log argument 'savepoint' to be the long name, got %v",
					(arg)["savepoint"],
				)
			}
		})
	}
}

func TestNewSavepoint_NotMatchName(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		spName string
	}{
		{
			name:   "name with space",
			spName: "invalid name",
		},
		{
			name:   "name starting with digit",
			spName: "1invalid",
		},
		{
			name:   "name with special character",
			spName: "invalid-name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.WithValue(
				context.Background(),
				&mockTxContextKey{},
				"new-test-invalid-name",
			)
			log := new(mockLogger)

			sp, err := smartdb.NewSavepoint(ctx, tc.spName, nil, log)
			if err == nil {
				t.Errorf("expected error for invalid savepoint name, got nil")
			}

			if sp != nil {
				t.Errorf("expected nil savepoint on error, got non-nil")
			}

			if !errors.Is(err, smartdb.ErrSavepointInvalidName) {
				t.Errorf("expected ErrSavepointInvalidName, got %v", err)
			}

			if len(log.debugLog) != 1 {
				t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
			}

			if log.debugLog[0].ctx != ctx {
				t.Errorf(
					"expected log context to be the same as input context, got different context",
				)
			}

			if log.debugLog[0].msg != "smartdb: creating savepoint" {
				t.Errorf("unexpected debug log message: %s", log.debugLog[0].msg)
			}

			if len(log.debugLog[0].args) != 1 {
				t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[0].args))
			}

			arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
			if !ok {
				t.Fatalf(
					"expected log argument to be of type LogArgs, got %T",
					log.debugLog[0].args[0],
				)
			}

			if len(arg) != 1 {
				t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
			}

			if (arg)["savepoint"] != tc.spName {
				t.Errorf(
					"expected log argument 'savepoint' to be the invalid name, got %v",
					(arg)["savepoint"],
				)
			}
		})
	}
}

func TestNewSavepoint_NilTx(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "new-test-nil-tx")
	log := new(mockLogger)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name1", nil, log)
	if err == nil {
		t.Errorf("expected error for nil transaction, got nil")
	}

	if sp != nil {
		t.Errorf("expected nil savepoint on error, got non-nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointNilTx) {
		t.Errorf("expected ErrSavepointNilTx, got %v", err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[0].msg != "smartdb: creating savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[0].msg)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name1" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name1', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestNewSavepoint_WithLogger(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "new-test-with-logger")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name2", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[0].msg != "smartdb: creating savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[0].msg)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name2" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name2', got %v",
			(arg)["savepoint"],
		)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SAVEPOINT valid_name2" {
		t.Errorf("expected transaction to execute 'SAVEPOINT valid_name2', got '%s'", tx.query)
	}
}

func TestNewSavepoint_WithoutLogger(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "new-test-without-logger")
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name3", tx, nil)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SAVEPOINT valid_name3" {
		t.Errorf("expected transaction to execute 'SAVEPOINT valid_name3', got '%s'", tx.query)
	}
}

func TestNewSavepoint_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "new-test-exec-error")
	log := new(mockLogger)
	tx := new(mockTx)
	expectedErr := errors.New("execution error")
	tx.err = expectedErr

	sp, err := smartdb.NewSavepoint(ctx, "valid_name4", tx, log)
	if err == nil {
		t.Fatalf("expected error creating savepoint, got nil")
	}

	if sp != nil {
		t.Fatalf("expected nil savepoint on error, got non-nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to be %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointCreate) {
		t.Errorf("expected error to be wrapped in ErrSavepointCreate, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name4") == false {
		t.Errorf("expected error message to contain 'valid_name4', got %v", err)
	}

	if len(log.debugLog) != 1 {
		t.Fatalf("expected 1 debug log entry, got %d", len(log.debugLog))
	}

	if log.debugLog[0].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[0].msg != "smartdb: creating savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[0].msg)
	}

	if len(log.debugLog[0].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[0].args))
	}

	arg, ok := log.debugLog[0].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[0].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name4" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name4', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Commit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-commit")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name5", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "RELEASE SAVEPOINT valid_name5" {
		t.Errorf(
			"expected transaction to execute 'RELEASE SAVEPOINT valid_name5', got '%s'",
			tx.query,
		)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: releasing savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[1].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name5" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name5', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Commit_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-commit-with-error")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name6", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	expectedErr := errors.New("execution error on commit")
	tx.err = expectedErr

	err = sp.Commit()
	if err == nil {
		t.Fatalf("expected error committing savepoint, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to be %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointRelease) {
		t.Errorf("expected error to be wrapped in ErrSavepointRelease, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name6") == false {
		t.Errorf("expected error message to contain 'valid_name6', got %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "RELEASE SAVEPOINT valid_name6" {
		t.Errorf(
			"expected transaction to execute 'RELEASE SAVEPOINT valid_name6', got '%s'",
			tx.query,
		)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: releasing savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[1].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name6" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name6', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Commit_AfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-commit-after-commit")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name7", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "RELEASE SAVEPOINT valid_name7" {
		t.Errorf(
			"expected transaction to execute 'RELEASE SAVEPOINT valid_name7', got '%s'",
			tx.query,
		)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: releasing savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[1].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name7" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name7', got %v",
			(arg)["savepoint"],
		)
	}

	err = sp.Commit()
	if err == nil {
		t.Fatalf("expected error committing already committed savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name7") == false {
		t.Errorf("expected error message to contain 'valid_name7', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: releasing savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok = log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[2].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name7" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name7', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Commit_AfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-commit-after-rollback",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name8", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "ROLLBACK TO SAVEPOINT valid_name8" {
		t.Errorf(
			"expected transaction to execute 'ROLLBACK TO SAVEPOINT valid_name8', got '%s'",
			tx.query,
		)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: rolling back to savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[1].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name8" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name8', got %v",
			(arg)["savepoint"],
		)
	}

	err = sp.Commit()
	if err == nil {
		t.Fatalf("expected error committing rolled back savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name8") == false {
		t.Errorf("expected error message to contain 'valid_name8', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: releasing savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok = log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf("expected log argument to be of type LogArgs, got %T", log.debugLog[2].args[0])
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name8" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name8', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Commit_ConcurrentCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-concurrent-commit")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name9", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	countGoroutines := 100
	errCh := make(chan error, countGoroutines)

	for range countGoroutines {
		go func() {
			errCh <- sp.Commit()
		}()
	}

	var (
		commitCount int
		doneCount   int
	)

	for range countGoroutines {
		err := <-errCh

		switch {
		case err == nil:
			commitCount++
		case errors.Is(err, smartdb.ErrSavepointIsDone):
			doneCount++
		default:
			t.Errorf("unexpected error committing savepoint: %v", err)
		}
	}

	if commitCount != 1 {
		t.Errorf("expected 1 successful commit, got %d", commitCount)
	}

	if doneCount != countGoroutines-1 {
		t.Errorf(
			"expected %d errors for already done savepoint, got %d",
			countGoroutines-1,
			doneCount,
		)
	}
}

func TestSavepoint_ExecContext(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-exec-context",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name10", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	mockResult := new(mockSQLResult)
	tx.result = mockResult

	result, err := sp.ExecContext(ctx, "SELECT 1", 5, "arg")
	if err != nil {
		t.Fatalf("unexpected error executing query on savepoint: %v", err)
	}

	if result != mockResult {
		t.Errorf("expected result to be the mock result, got %v", result)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to execute 'SELECT 1', got '%s'", tx.query)
	}

	if len(tx.args) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(tx.args))
	}

	if tx.args[0] != 5 {
		t.Errorf("expected first argument to be 5, got %v", tx.args[0])
	}

	if tx.args[1] != "arg" {
		t.Errorf("expected second argument to be 'arg', got %v", tx.args[1])
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: executing query in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name10" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name10', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_ExecContext_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-savepoint-exec-error")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name11", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	expectedErr := errors.New("execution error on query")
	tx.err = expectedErr

	_, err = sp.ExecContext(ctx, "SELECT 1", 5, "arg")
	if err == nil {
		t.Fatalf("expected error executing query on savepoint, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to be %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointExec) {
		t.Errorf("expected error to be wrapped in ErrSavepointExec, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name11") == false {
		t.Errorf("expected error message to contain 'valid_name11', got %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to execute 'SELECT 1', got '%s'", tx.query)
	}

	if len(tx.args) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(tx.args))
	}

	if tx.args[0] != 5 {
		t.Errorf("expected first argument to be 5, got %v", tx.args[0])
	}

	if tx.args[1] != "arg" {
		t.Errorf("expected second argument to be 'arg', got %v", tx.args[1])
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: executing query in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name11" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name11', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_ExecContext_AfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-exec-after-commit",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name12", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	_, err = sp.ExecContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error executing query on committed savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name12") == false {
		t.Errorf("expected error message to contain 'valid_name12', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: executing query in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name12" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name12', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_ExecContext_AfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-exec-after-rollback",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name13", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	_, err = sp.ExecContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error executing query on rolled back savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name13") == false {
		t.Errorf("expected error message to contain 'valid_name13', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: executing query in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name13" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name13', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_ExecContext_ConcurrentExec(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-concurrent-exec")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name14", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	countGoroutines := 100
	breakCount := 55
	errCh := make(chan error, countGoroutines)
	wg := sync.WaitGroup{}

	for idx := range countGoroutines {
		if idx == breakCount {
			wg.Wait()

			err := sp.Commit()
			if err != nil {
				t.Errorf("unexpected error committing savepoint in concurrent exec test: %v", err)
			}
		}

		wg.Go(func() {
			_, err := sp.ExecContext(ctx, "SELECT 1")
			errCh <- err
		})
	}

	wg.Wait()

	var (
		execCount int
		doneCount int
	)

	for range countGoroutines {
		err := <-errCh

		switch {
		case err == nil:
			execCount++
		case errors.Is(err, smartdb.ErrSavepointIsDone):
			doneCount++
		default:
			t.Errorf("unexpected error executing query in concurrent exec test: %v", err)
		}
	}

	if execCount != breakCount {
		t.Errorf("expected %d successful execs before commit, got %d", breakCount-1, execCount)
	}

	if doneCount != countGoroutines-breakCount {
		t.Errorf(
			"expected %d done execs after commit, got %d",
			countGoroutines-breakCount+1,
			doneCount,
		)
	}
}

func TestSavepoint_PrepareContext(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-prepare-context",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name15", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	mockStmt := new(sql.Stmt)
	tx.stmt = mockStmt

	stmt, err := sp.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error preparing statement on savepoint: %v", err)
	}

	if stmt != mockStmt {
		t.Errorf("expected statement to be the mock statement, got %v", stmt)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to prepare 'SELECT 1', got '%s'", tx.query)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: preparing statement in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name15" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name15', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSavepoint_PrepareContext_PrepareError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-prepare-error",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name16", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	expectedErr := errors.New("prepare error on statement")
	tx.err = expectedErr

	_, err = sp.PrepareContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error preparing statement on savepoint, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointPrepare) {
		t.Errorf("expected error to be wrapped in ErrSavepointPrepare, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name16") == false {
		t.Errorf("expected error message to contain 'valid_name16', got %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to prepare 'SELECT 1', got '%s'", tx.query)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: preparing statement in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name16" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name16', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSavepoint_PrepareContext_AfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-prepare-after-commit",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name17", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	_, err = sp.PrepareContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error preparing statement on committed savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name17") == false {
		t.Errorf("expected error message to contain 'valid_name17', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: preparing statement in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name17" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name17', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSavepoint_PrepareContext_AfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-prepare-after-rollback",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name18", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	_, err = sp.PrepareContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error preparing statement on rolled back savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name18") == false {
		t.Errorf("expected error message to contain 'valid_name18', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: preparing statement in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 2 {
		t.Fatalf("expected 2 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name18" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name18', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}
}

func TestSavepoint_PrepareContext_ConcurrentPrepare(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-concurrent-prepare")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name19", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	countGoroutines := 100
	breakCount := 55
	errCh := make(chan error, countGoroutines)
	wg := sync.WaitGroup{}

	for idx := range countGoroutines {
		if idx == breakCount {
			wg.Wait()

			err := sp.Commit()
			if err != nil {
				t.Errorf(
					"unexpected error committing savepoint in concurrent prepare test: %v",
					err,
				)
			}
		}

		wg.Go(func() {
			_, err := sp.PrepareContext(ctx, "SELECT 1")
			errCh <- err
		})
	}

	wg.Wait()

	var (
		prepareCount int
		doneCount    int
	)

	for range countGoroutines {
		err := <-errCh

		switch {
		case err == nil:
			prepareCount++
		case errors.Is(err, smartdb.ErrSavepointIsDone):
			doneCount++
		default:
			t.Errorf("unexpected error preparing statement in concurrent prepare test: %v", err)
		}
	}

	if prepareCount != breakCount {
		t.Errorf(
			"expected %d successful prepares before commit, got %d",
			breakCount-1,
			prepareCount,
		)
	}

	if doneCount != countGoroutines-breakCount {
		t.Errorf(
			"expected %d done prepares after commit, got %d",
			countGoroutines-breakCount+1,
			doneCount,
		)
	}
}

func TestSavepoint_QueryContext(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-context",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name20", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	mockRows := new(sql.Rows)
	tx.rows = mockRows

	rows, err := sp.QueryContext(ctx, "SELECT 1", 5, "arg")
	if err != nil {
		t.Fatalf("unexpected error querying on savepoint: %v", err)
	}

	if rows != mockRows {
		t.Errorf("expected rows to be the mock rows, got %v", rows)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to execute 'SELECT 1', got '%s'", tx.query)
	}

	if len(tx.args) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(tx.args))
	}

	if tx.args[0] != 5 {
		t.Errorf("expected first argument to be 5, got %v", tx.args[0])
	}

	if tx.args[1] != "arg" {
		t.Errorf("expected second argument to be 'arg', got %v", tx.args[1])
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: querying rows in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name20" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name20', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryContext_QueryError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-error",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name21", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	expectedErr := errors.New("query error on rows")
	tx.err = expectedErr

	_, err = sp.QueryContext(ctx, "SELECT 1", 5, "arg")
	if err == nil {
		t.Fatalf("expected error querying on savepoint, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointQuery) {
		t.Errorf("expected error to be wrapped in ErrSavepointQuery, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name21") == false {
		t.Errorf("expected error message to contain 'valid_name21', got %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to execute 'SELECT 1', got '%s'", tx.query)
	}

	if len(tx.args) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(tx.args))
	}

	if tx.args[0] != 5 {
		t.Errorf("expected first argument to be 5, got %v", tx.args[0])
	}

	if tx.args[1] != "arg" {
		t.Errorf("expected second argument to be 'arg', got %v", tx.args[1])
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: querying rows in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name21" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name21', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryContext_AfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-after-commit",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name22", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	_, err = sp.QueryContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error querying on committed savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name22") == false {
		t.Errorf("expected error message to contain 'valid_name22', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: querying rows in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name22" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name22', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryContext_AfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-after-rollback",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name23", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	_, err = sp.QueryContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("expected error querying on rolled back savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name23") == false {
		t.Errorf("expected error message to contain 'valid_name23', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: querying rows in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name23" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name23', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryContext_ConcurrentQuery(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-concurrent-query")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name24", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	countGoroutines := 100
	breakCount := 55
	errCh := make(chan error, countGoroutines)
	wg := sync.WaitGroup{}

	for idx := range countGoroutines {
		if idx == breakCount {
			wg.Wait()

			err := sp.Commit()
			if err != nil {
				t.Errorf("unexpected error committing savepoint in concurrent query test: %v", err)
			}
		}

		wg.Go(func() {
			_, err := sp.QueryContext(ctx, "SELECT 1")
			errCh <- err
		})
	}

	wg.Wait()

	var (
		queryCount int
		doneCount  int
	)

	for range countGoroutines {
		err := <-errCh

		switch {
		case err == nil:
			queryCount++
		case errors.Is(err, smartdb.ErrSavepointIsDone):
			doneCount++
		default:
			t.Errorf("unexpected error querying in concurrent query test: %v", err)
		}
	}

	if queryCount != breakCount {
		t.Errorf("expected %d successful queries before commit, got %d", breakCount-1, queryCount)
	}

	if doneCount != countGoroutines-breakCount {
		t.Errorf(
			"expected %d done queries after commit, got %d",
			countGoroutines-breakCount+1,
			doneCount,
		)
	}
}

func TestSavepoint_QueryRowContext(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-row-context",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name25", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	mockRow := new(sql.Row)
	tx.row = mockRow

	row := sp.QueryRowContext(ctx, "SELECT 1", 5, "arg")
	if row != mockRow {
		t.Errorf("expected row to be the mock row, got %v", row)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if tx.query != "SELECT 1" {
		t.Errorf("expected transaction to execute 'SELECT 1', got '%s'", tx.query)
	}

	if len(tx.args) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(tx.args))
	}

	if tx.args[0] != 5 {
		t.Errorf("expected first argument to be 5, got %v", tx.args[0])
	}

	if tx.args[1] != "arg" {
		t.Errorf("expected second argument to be 'arg', got %v", tx.args[1])
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: querying single row in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name25" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name25', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryRowContext_AfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-row-after-commit",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name26", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	_ = sp.QueryRowContext(ctx, "SELECT 1")

	select {
	case <-tx.ctx.Done():
	default:
		t.Errorf("expected transaction context to be canceled after commit, but it is not done")
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: querying single row in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name26" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name26', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryRowContext_AfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-query-row-after-rollback",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name27", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	_ = sp.QueryRowContext(ctx, "SELECT 1")

	select {
	case <-tx.ctx.Done():
	default:
		t.Errorf("expected transaction context to be canceled after rollback, but it is not done")
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: querying single row in savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 3 {
		t.Fatalf("expected 3 keys in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name27" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name27', got %v",
			(arg)["savepoint"],
		)
	}

	if (arg)["query"] != "SELECT 1" {
		t.Errorf("expected log argument 'query' to be 'SELECT 1', got %v", (arg)["query"])
	}

	if (arg)["args"] == nil {
		t.Errorf("expected log argument 'args' to be non-nil, got nil")
	}
}

func TestSavepoint_QueryRowContext_ConcurrentQuery(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-concurrent-query-row")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name28", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	countGoroutines := 100
	breakCount := 55
	ctxCh := make(chan context.Context, countGoroutines)
	wg := sync.WaitGroup{}

	for idx := range countGoroutines {
		if idx == breakCount {
			wg.Wait()

			err := sp.Commit()
			if err != nil {
				t.Errorf(
					"unexpected error committing savepoint in concurrent query row test: %v",
					err,
				)
			}
		}

		wg.Go(func() {
			queryCtx := context.WithValue(
				ctx,
				&mockTxContextKey{},
				"concurrent-query-row-"+strconv.Itoa(idx),
			)

			_ = sp.QueryRowContext(queryCtx, "SELECT 1")

			ctxCh <- tx.ctx
		})
	}

	wg.Wait()

	var (
		queryCount int
		doneCount  int
	)

	for range countGoroutines {
		queryCtx := <-ctxCh

		select {
		case <-queryCtx.Done():
			doneCount++
		default:
			queryCount++
		}
	}

	if queryCount != breakCount {
		t.Errorf("expected %d successful query rows before commit, got %d", breakCount, queryCount)
	}

	if doneCount != countGoroutines-breakCount {
		t.Errorf(
			"expected %d done query rows after commit, got %d",
			countGoroutines-breakCount,
			doneCount,
		)
	}
}

func TestSavepoint_Rollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-savepoint-rollback")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name29", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: rolling back to savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name29" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name29', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Rollback_ExecError(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-rollback-exec-error",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name30", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	expectedErr := errors.New("exec error on rollback")
	tx.err = expectedErr

	err = sp.Rollback()
	if err == nil {
		t.Fatalf("expected error rolling back savepoint, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}

	if !errors.Is(err, smartdb.ErrSavepointRollback) {
		t.Errorf("expected error to be wrapped in ErrSavepointRollback, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name30") == false {
		t.Errorf("expected error message to contain 'valid_name30', got %v", err)
	}

	if tx.ctx != ctx {
		t.Errorf(
			"expected transaction context to be the same as input context, got different context",
		)
	}

	if len(log.debugLog) != 2 {
		t.Fatalf("expected 2 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[1].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[1].msg != "smartdb: rolling back to savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[1].msg)
	}

	if len(log.debugLog[1].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[1].args))
	}

	arg, ok := log.debugLog[1].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[1].args[0],
		)
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name30" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name30', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Rollback_AfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-rollback-after-commit",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name31", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Commit()
	if err != nil {
		t.Fatalf("unexpected error committing savepoint: %v", err)
	}

	err = sp.Rollback()
	if err == nil {
		t.Fatalf("expected error rolling back committed savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name31") == false {
		t.Errorf("expected error message to contain 'valid_name31', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: rolling back to savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name31" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name31', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Rollback_AfterRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		&mockTxContextKey{},
		"test-savepoint-rollback-after-rollback",
	)
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name32", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	err = sp.Rollback()
	if err != nil {
		t.Fatalf("unexpected error rolling back savepoint: %v", err)
	}

	err = sp.Rollback()
	if err == nil {
		t.Fatalf("expected error rolling back already rolled back savepoint, got nil")
	}

	if !errors.Is(err, smartdb.ErrSavepointIsDone) {
		t.Errorf("expected error to be ErrSavepointIsDone, got %v", err)
	}

	if strings.Contains(err.Error(), "valid_name32") == false {
		t.Errorf("expected error message to contain 'valid_name32', got %v", err)
	}

	if len(log.debugLog) != 3 {
		t.Fatalf("expected 3 debug log entries, got %d", len(log.debugLog))
	}

	if log.debugLog[2].ctx != ctx {
		t.Errorf("expected log context to be the same as input context, got different context")
	}

	if log.debugLog[2].msg != "smartdb: rolling back to savepoint" {
		t.Errorf("unexpected debug log message: %s", log.debugLog[2].msg)
	}

	if len(log.debugLog[2].args) != 1 {
		t.Fatalf("expected 1 debug log argument, got %d", len(log.debugLog[2].args))
	}

	arg, ok := log.debugLog[2].args[0].(smartdb.LogArgs)
	if !ok {
		t.Fatalf(
			"expected first log argument to be of type LogArgs, got %T",
			log.debugLog[2].args[0],
		)
	}

	if len(arg) != 1 {
		t.Fatalf("expected 1 key in LogArgs, got %d", len(arg))
	}

	if (arg)["savepoint"] != "valid_name32" {
		t.Errorf(
			"expected log argument 'savepoint' to be 'valid_name32', got %v",
			(arg)["savepoint"],
		)
	}
}

func TestSavepoint_Rollback_ConcurrentRollback(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), &mockTxContextKey{}, "test-concurrent-rollback")
	log := new(mockLogger)
	tx := new(mockTx)

	sp, err := smartdb.NewSavepoint(ctx, "valid_name33", tx, log)
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if sp == nil {
		t.Fatalf("expected non-nil savepoint, got nil")
	}

	countGoroutines := 100
	errCh := make(chan error, countGoroutines)

	for range countGoroutines {
		go func() {
			errCh <- sp.Rollback()
		}()
	}

	var (
		rollbackCount int
		doneCount     int
	)

	for range countGoroutines {
		err := <-errCh

		switch {
		case err == nil:
			rollbackCount++
		case errors.Is(err, smartdb.ErrSavepointIsDone):
			doneCount++
		default:
			t.Errorf("unexpected error rolling back in concurrent rollback test: %v", err)
		}
	}

	if rollbackCount != 1 {
		t.Errorf("expected exactly 1 successful rollback, got %d", rollbackCount)
	}

	if doneCount != countGoroutines-1 {
		t.Errorf(
			"expected %d done rollbacks after first rollback, got %d",
			countGoroutines-1,
			doneCount,
		)
	}
}

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/yuppyweb/smartdb"
	"github.com/yuppyweb/smartdb/tests/helper"
)

func TestNestedTxAndSavepoints(t *testing.T) {
	ctx := context.Background()
	log := helper.NewLogger()

	t.Cleanup(func() {
		log.PrintLogs(t)
	})

	log.Infof("Starting PostgreSQL container.")

	ctr, err := postgres.Run(
		ctx,
		helper.PostgresImage,
		postgres.WithDatabase(helper.PostgresDatabase),
		postgres.WithUsername(helper.PostgresUser),
		postgres.WithPassword(helper.PostgresPassword),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver(helper.PostgresDriver),
		testcontainers.WithLogger(log),
	)

	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		log.Errorf("failed to start PostgreSQL container: %v", err)

		t.FailNow()
	}

	log.Infof("Taking snapshot of PostgreSQL container.")

	if err := ctr.Snapshot(ctx); err != nil {
		log.Errorf("failed to take snapshot of PostgreSQL container: %v", err)

		t.FailNow()
	}

	log.Infof("Getting connection string for PostgreSQL container.")

	dsn, err := ctr.ConnectionString(ctx)
	if err != nil {
		log.Errorf("failed to get connection string: %v", err)

		t.FailNow()
	}

	t.Run("Nested transaction commit both", func(t *testing.T) {
		log.Infof("Starting test: %s", t.Name())

		t.Cleanup(func() {
			log.Infof("Restoring PostgreSQL container.")

			if err := ctr.Restore(ctx); err != nil {
				log.Errorf("failed to restore PostgreSQL container: %v", err)
			}
		})

		log.Infof("Creating test table.")

		if err := helper.ExecPostgres(ctx, ctr, "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT);"); err != nil {
			log.Errorf("failed to execute command in PostgreSQL container: %v", err)

			t.FailNow()
		}

		log.Infof("Connecting to database.")

		db, err := helper.ConnectPostgres(dsn)
		if err != nil {
			log.Errorf("failed to connect to database: %v", err)

			t.FailNow()
		}

		defer db.Close()

		log.Infof("Creating smartdb instance.")

		sdb, err := smartdb.New(db)
		if err != nil {
			log.Errorf("failed to create smartdb instance: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning context transaction.")

		outerCtx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in outer context transaction.")

		_, err = sdb.ExecContext(outerCtx, "INSERT INTO test_table (id, name) VALUES (1, 'Alice');")
		if err != nil {
			log.Errorf("failed to execute command in outer context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning nested context transaction.")

		innerCtx, err := sdb.BeginContext(outerCtx)
		if err != nil {
			log.Errorf("failed to begin nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in nested context transaction.")

		_, err = sdb.ExecContext(innerCtx, "INSERT INTO test_table (id, name) VALUES (2, 'Bob');")
		if err != nil {
			log.Errorf("failed to execute command in nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Committing nested context transaction.")

		if err := sdb.CommitContext(innerCtx); err != nil {
			log.Errorf("failed to commit nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Committing outer context transaction.")

		if err := sdb.CommitContext(outerCtx); err != nil {
			log.Errorf("failed to commit outer context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Querying test table.")

		result, err := helper.QueryPostgres(ctx, ctr, "SELECT * FROM test_table ORDER BY id;")
		if err != nil {
			log.Errorf("failed to query test table: %v", err)

			t.FailNow()
		}

		log.Infof("Query result: %q", result)

		expectedResult := "\x01\x00\x00\x00\x00\x00\x00: id | name  \n----+-------\n  1 | Alice\n  2 | Bob\n(2 rows)\n\n"

		if result != expectedResult {
			log.Errorf("unexpected query result: got %q, want %q", result, expectedResult)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})

	t.Run("Nested transaction rollback inner", func(t *testing.T) {
		log.Infof("Starting test: %s", t.Name())

		t.Cleanup(func() {
			log.Infof("Restoring PostgreSQL container.")

			if err := ctr.Restore(ctx); err != nil {
				log.Errorf("failed to restore PostgreSQL container: %v", err)
			}
		})

		log.Infof("Creating test table.")

		if err := helper.ExecPostgres(ctx, ctr, "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT);"); err != nil {
			log.Errorf("failed to execute command in PostgreSQL container: %v", err)

			t.FailNow()
		}

		log.Infof("Connecting to database.")

		db, err := helper.ConnectPostgres(dsn)
		if err != nil {
			log.Errorf("failed to connect to database: %v", err)

			t.FailNow()
		}

		defer db.Close()

		log.Infof("Creating smartdb instance.")

		sdb, err := smartdb.New(db)
		if err != nil {
			log.Errorf("failed to create smartdb instance: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning context transaction.")

		outerCtx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in outer context transaction.")

		_, err = sdb.ExecContext(outerCtx, "INSERT INTO test_table (id, name) VALUES (1, 'Alice');")
		if err != nil {
			log.Errorf("failed to execute command in outer context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning nested context transaction.")

		innerCtx, err := sdb.BeginContext(outerCtx)
		if err != nil {
			log.Errorf("failed to begin nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in nested context transaction.")

		_, err = sdb.ExecContext(innerCtx, "INSERT INTO test_table (id, name) VALUES (2, 'Bob');")
		if err != nil {
			log.Errorf("failed to execute command in nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Rolling back nested context transaction.")

		if err := sdb.RollbackContext(innerCtx); err != nil {
			log.Errorf("failed to rollback nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Committing outer context transaction.")

		if err := sdb.CommitContext(outerCtx); err != nil {
			log.Errorf("failed to commit outer context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Querying test table.")

		result, err := helper.QueryPostgres(ctx, ctr, "SELECT * FROM test_table ORDER BY id;")
		if err != nil {
			log.Errorf("failed to query test table: %v", err)

			t.FailNow()
		}

		log.Infof("Query result: %q", result)

		expectedResult := "\x01\x00\x00\x00\x00\x00\x00/ id | name  \n----+-------\n  1 | Alice\n(1 row)\n\n"

		if result != expectedResult {
			log.Errorf("unexpected query result: got %q, want %q", result, expectedResult)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})

	t.Run("Nested transaction rollback outer", func(t *testing.T) {
		log.Infof("Starting test: %s", t.Name())

		t.Cleanup(func() {
			log.Infof("Restoring PostgreSQL container.")

			if err := ctr.Restore(ctx); err != nil {
				log.Errorf("failed to restore PostgreSQL container: %v", err)
			}
		})

		log.Infof("Creating test table.")

		if err := helper.ExecPostgres(ctx, ctr, "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT);"); err != nil {
			log.Errorf("failed to execute command in PostgreSQL container: %v", err)

			t.FailNow()
		}

		log.Infof("Connecting to database.")

		db, err := helper.ConnectPostgres(dsn)
		if err != nil {
			log.Errorf("failed to connect to database: %v", err)

			t.FailNow()
		}

		defer db.Close()

		log.Infof("Creating smartdb instance.")

		sdb, err := smartdb.New(db)
		if err != nil {
			log.Errorf("failed to create smartdb instance: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning context transaction.")

		outerCtx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in outer context transaction.")

		_, err = sdb.ExecContext(outerCtx, "INSERT INTO test_table (id, name) VALUES (1, 'Alice');")
		if err != nil {
			log.Errorf("failed to execute command in outer context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning nested context transaction.")

		innerCtx, err := sdb.BeginContext(outerCtx)
		if err != nil {
			log.Errorf("failed to begin nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in nested context transaction.")

		_, err = sdb.ExecContext(innerCtx, "INSERT INTO test_table (id, name) VALUES (2, 'Bob');")
		if err != nil {
			log.Errorf("failed to execute command in nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Committing nested context transaction.")

		if err := sdb.CommitContext(innerCtx); err != nil {
			log.Errorf("failed to commit nested context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Rolling back outer context transaction.")

		if err := sdb.RollbackContext(outerCtx); err != nil {
			log.Errorf("failed to rollback outer context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Querying test table.")

		result, err := helper.QueryPostgres(ctx, ctr, "SELECT * FROM test_table ORDER BY id;")
		if err != nil {
			log.Errorf("failed to query test table: %v", err)

			t.FailNow()
		}

		log.Infof("Query result: %q", result)

		expectedResult := "\x01\x00\x00\x00\x00\x00\x00\" id | name \n----+------\n(0 rows)\n\n"

		if result != expectedResult {
			log.Errorf("unexpected query result: got %q, want %q", result, expectedResult)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})

	t.Run("Nested transaction multiple nested levels", func(t *testing.T) {
		log.Infof("Starting test: %s", t.Name())

		t.Cleanup(func() {
			log.Infof("Restoring PostgreSQL container.")

			if err := ctr.Restore(ctx); err != nil {
				log.Errorf("failed to restore PostgreSQL container: %v", err)
			}
		})

		log.Infof("Creating test table.")

		if err := helper.ExecPostgres(ctx, ctr, "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT);"); err != nil {
			log.Errorf("failed to execute command in PostgreSQL container: %v", err)

			t.FailNow()
		}

		log.Infof("Connecting to database.")

		db, err := helper.ConnectPostgres(dsn)
		if err != nil {
			log.Errorf("failed to connect to database: %v", err)

			t.FailNow()
		}

		defer db.Close()

		log.Infof("Creating smartdb instance.")

		sdb, err := smartdb.New(db)
		if err != nil {
			log.Errorf("failed to create smartdb instance: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning 1 level context transaction.")

		level1Ctx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin 1 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in 1 level context transaction.")

		_, err = sdb.ExecContext(level1Ctx, "INSERT INTO test_table (id, name) VALUES (1, 'Alice');")
		if err != nil {
			log.Errorf("failed to execute command in 1 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning 2 level context transaction.")

		level2Ctx, err := sdb.BeginContext(level1Ctx)
		if err != nil {
			log.Errorf("failed to begin 2 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in 2 level context transaction.")

		_, err = sdb.ExecContext(level2Ctx, "INSERT INTO test_table (id, name) VALUES (2, 'Bob');")
		if err != nil {
			log.Errorf("failed to execute command in 2 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Beginning 3 level context transaction.")

		level3Ctx, err := sdb.BeginContext(level2Ctx)
		if err != nil {
			log.Errorf("failed to begin 3 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Inserting record in 3 level context transaction.")

		_, err = sdb.ExecContext(level3Ctx, "INSERT INTO test_table (id, name) VALUES (3, 'Charlie');")
		if err != nil {
			log.Errorf("failed to execute command in 3 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Rolling back 2 level context transaction.")

		if err := sdb.RollbackContext(level2Ctx); err != nil {
			log.Errorf("failed to rollback 2 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Committing 1 level context transaction.")

		if err := sdb.CommitContext(level1Ctx); err != nil {
			log.Errorf("failed to commit 1 level context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Querying test table.")

		result, err := helper.QueryPostgres(ctx, ctr, "SELECT * FROM test_table ORDER BY id;")
		if err != nil {
			log.Errorf("failed to query test table: %v", err)

			t.FailNow()
		}

		log.Infof("Query result: %q", result)

		expectedResult := "\x01\x00\x00\x00\x00\x00\x00/ id | name  \n----+-------\n  1 | Alice\n(1 row)\n\n"

		if result != expectedResult {
			log.Errorf("unexpected query result: got %q, want %q", result, expectedResult)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})
}

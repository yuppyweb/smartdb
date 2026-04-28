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

func TestBasicTxOperations(t *testing.T) {
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

	t.Run("BeginContext success", func(t *testing.T) {
		log.Infof("Starting test: %s", t.Name())

		t.Cleanup(func() {
			log.Infof("Restoring PostgreSQL container.")

			if err := ctr.Restore(ctx); err != nil {
				log.Errorf("failed to restore PostgreSQL container: %v", err)
			}
		})

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

		ctx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Getting transaction from context.")

		tx, ok := smartdb.TxFromContext(ctx)
		if !ok {
			log.Errorf("failed to get transaction from context")

			t.FailNow()
		}

		if tx == nil {
			log.Errorf("transaction is nil")

			t.FailNow()
		}

		log.Infof("Rolling back context transaction.")

		if err := sdb.RollbackContext(ctx); err != nil {
			log.Errorf("failed to rollback context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})

	t.Run("CommitContext success", func(t *testing.T) {
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

		ctx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin context transaction: %v", err)

			t.FailNow()
		}

		_, err = sdb.ExecContext(ctx, "INSERT INTO test_table (id, name) VALUES (1, 'Alice');")
		if err != nil {
			log.Errorf("failed to execute command in context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Committing context transaction.")

		if err := sdb.CommitContext(ctx); err != nil {
			log.Errorf("failed to commit context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Querying test table.")

		result, err := helper.QueryPostgres(ctx, ctr, "SELECT name FROM test_table WHERE id = 1;")
		if err != nil {
			log.Errorf("failed to execute query in PostgreSQL container: %v", err)

			t.FailNow()
		}

		log.Infof("Query result: %q", result)

		expectedResult := "\x01\x00\x00\x00\x00\x00\x00  name  \n-------\n Alice\n(1 row)\n\n"
		if result != expectedResult {
			log.Errorf("unexpected query result: got %q, want %q", result, expectedResult)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})

	t.Run("RollbackContext success", func(t *testing.T) {
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

		ctx, err := sdb.BeginContext(context.Background())
		if err != nil {
			log.Errorf("failed to begin context transaction: %v", err)

			t.FailNow()
		}

		_, err = sdb.ExecContext(ctx, "INSERT INTO test_table (id, name) VALUES (1, 'Alice');")
		if err != nil {
			log.Errorf("failed to execute command in context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Rolling back context transaction.")

		if err := sdb.RollbackContext(ctx); err != nil {
			log.Errorf("failed to rollback context transaction: %v", err)

			t.FailNow()
		}

		log.Infof("Querying test table.")

		result, err := helper.QueryPostgres(ctx, ctr, "SELECT name FROM test_table WHERE id = 1;")
		if err != nil {
			log.Errorf("failed to execute query in PostgreSQL container: %v", err)

			t.FailNow()
		}

		log.Infof("Query result: %q", result)

		expectedResult := "\x01\x00\x00\x00\x00\x00\x00\x18 name \n------\n(0 rows)\n\n"
		if result != expectedResult {
			log.Errorf("unexpected query result: got %q, want %q", result, expectedResult)

			t.FailNow()
		}

		log.Infof("Test %s completed successfully", t.Name())
	})
}

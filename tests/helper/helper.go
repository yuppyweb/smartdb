//go:build integration

package helper

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/testcontainers/testcontainers-go"
)

func ConnectPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open(PostgresDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	return db, nil
}

func ExecPostgres(ctx context.Context, ctr testcontainers.Container, query string) error {
	cmd := []string{"psql", "-U", PostgresUser, "-d", PostgresDatabase, "-c", query}

	if _, _, err := ctr.Exec(ctx, cmd); err != nil {
		return err
	}

	return nil
}

func QueryPostgres(ctx context.Context, ctr testcontainers.Container, query string) (string, error) {
	cmd := []string{"psql", "-U", PostgresUser, "-d", PostgresDatabase, "-c", query}

	_, reader, err := ctr.Exec(ctx, cmd)
	if err != nil {
		return "", err
	}

	bytes, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

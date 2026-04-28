//go:build integration

package helper

import (
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

const (
	Debug            = false
	PostgresDriver   = "pgx"
	PostgresImage    = "postgres:18.3"
	PostgresDatabase = "testdb"
	PostgresUser     = "testuser"
	PostgresPassword = "testpass"
)

package db

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/dbpgx"
	"strings"
)

type Db struct {
	Url      string
	Pool     *pgxpool.Pool
	Database string
}

func NewDbDatasource(url string, database string) (*Db, error) {
	pool, err := dbpgx.NewPoolFromUrl(url)
	if err != nil {
		return nil, err
	}
	_, err = pool.Exec(context.Background(), "SET application_name = '$ crdb-schema-analyzer'")
	_, err = pool.Exec(context.Background(), "SET default_transaction_quality_of_service=background")
	_, err = pool.Exec(context.Background(), "SET default_transaction_use_follower_reads=on")
	if err != nil {
		return nil, err
	}
	return &Db{
		Url:      url,
		Pool:     pool,
		Database: database,
	}, nil
}

// SQLStringListToSlice converts a SQL-style string list like "{a,b,c}" to a Go string slice []string{"a", "b", "c"}
func SQLStringListToSlice(input string) []string {
	// Trim leading and trailing braces
	trimmed := strings.Trim(input, "{}")

	// Handle empty input case
	if trimmed == "" {
		return []string{}
	}

	// Split by comma
	parts := strings.Split(trimmed, ",")

	// Optionally trim spaces (if needed)
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}

	return parts
}

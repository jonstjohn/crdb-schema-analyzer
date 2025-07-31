package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/dbpgx"
	"strings"
)

type Db struct {
	Url      string
	Pool     *pgxpool.Pool
	Database string
}

func NewDbDatasource(url string, database string, readOnly bool) (*Db, error) {
	pool, err := dbpgx.NewPoolFromUrl(url)
	if err != nil {
		return nil, err
	}
	if readOnly {
		_, err = pool.Exec(context.Background(), "SET application_name = '$ crdb-schema-analyzer'")
		_, err = pool.Exec(context.Background(), "SET default_transaction_quality_of_service=background")
		_, err = pool.Exec(context.Background(), "SET default_transaction_use_follower_reads=on")
	}
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

func DeleteByColumnValuesSql(table string, columns []string, values []any) (string, error) {
	if len(columns) == 0 || len(columns) != len(values) {
		return "", fmt.Errorf("columns and values must be non-empty and of equal length")
	}

	var conditions []string
	for i, col := range columns {
		// Use numbered placeholders ($1, $2, ...) if this will be used with pgx Query / Exec
		conditions = append(conditions, fmt.Sprintf("\"%s\" = '%s'", col, values[i]))
	}

	whereClause := strings.Join(conditions, " AND ")
	sql := fmt.Sprintf("DELETE FROM \"%s\" WHERE %s", table, whereClause)
	return sql, nil
}

func DeleteByColumnValuesWithExistsCheckSql(table string, columns []string, values []any,
	relatedTable string, relatedColumns []string, relatedValues []any) (string, error) {

	del, err := DeleteByColumnValuesSql(table, columns, values)
	if err != nil {
		return "", err
	}
	sel, err := SelectByColumnValuesSql(relatedTable, relatedColumns, relatedValues)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s AND NOT EXISTS (%s)", del, sel), nil
}

func SelectByColumnValuesSql(table string, columns []string, values []any) (string, error) {
	if len(columns) == 0 || len(columns) != len(values) {
		return "", fmt.Errorf("columns and values must be non-empty and of equal length")
	}
	var conditions []string
	for i, col := range columns {
		conditions = append(conditions, fmt.Sprintf("\"%s\" = '%s'", col, values[i]))
	}
	sql := fmt.Sprintf("SELECT \"%s\" FROM \"%s\" WHERE %s", columns[0], table, strings.Join(conditions, " AND "))
	return sql, nil
}

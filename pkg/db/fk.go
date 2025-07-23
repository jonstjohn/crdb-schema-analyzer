package db

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
)

type FkRow struct {
	ConstraintName    string
	TableName         string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	UpdateRule        string
	DeleteRule        string
}

type FkOrphanedRow struct {
	TableName         string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	PrimaryKeyColumns []string
	PrimaryKeyValues  []any
}

const AllSql = `
-- Look for FK constraints that have a CASCADE DELETE rule
-- First get all constraints
WITH fks AS (
	SELECT constraint_name, table_name
	FROM information_schema.table_constraints
	WHERE constraint_type = 'FOREIGN KEY'
),
-- Next get actions
actions AS (
	SELECT constraint_name, update_rule, delete_rule
	FROM information_schema.referential_constraints
),
-- Next get key columns (primary table)
keys AS (
	SELECT constraint_name, array_agg(column_name) as keycols
	FROM information_schema.key_column_usage
	GROUP BY constraint_name
),
-- Finally get constraint columns (related table)
constraints AS (
	SELECT constraint_name, table_name, array_agg(column_name) as constraintcols
	FROM information_schema.constraint_column_usage
	GROUP BY constraint_name, table_name
),
-- NOW combined them all
fk_constraints AS (
SELECT fks.constraint_name,
  fks.table_name,
  keys.keycols as columns,
  constraints.table_name as referenced_table,
  constraints.constraintcols as referenced_columns,
  actions.update_rule, actions.delete_rule
FROM
  fks INNER JOIN actions ON fks.constraint_name = actions.constraint_name
    INNER JOIN keys ON fks.constraint_name = keys.constraint_name
    INNER JOIN constraints ON fks.constraint_name = constraints.constraint_name
WHERE delete_rule = 'CASCADE'
ORDER BY table_name, constraint_name
)
SELECT
	constraint_name, table_name, columns,
	referenced_table, referenced_columns,
	update_rule, delete_rule
FROM fk_constraints
ORDER BY table_name, constraint_name;
`

const OrphanSql = `
-- Get all rows from the main table
WITH main AS (
  SELECT %s FROM %s -- columns, table_name
  WHERE %s -- both columns are not null
)
-- join with referenced table 
SELECT %s -- referenced_columns
FROM main
LEFT JOIN %s -- $referenced_table 
  ON %s -- $columns joined with $referenced_columns
WHERE %s -- $referenced_columns IS NULL;
`

func (db *Db) OrphanSql(table string, columns []string, referencedTable string,
	referencedColumns []string, countOnly bool) string {
	var columnNotNulls []string
	for _, column := range columns {
		columnNotNulls = append(columnNotNulls, fmt.Sprintf("\"%s\" IS NOT NULL", column))
	}

	var joins []string
	for i, column := range columns {
		joins = append(joins, fmt.Sprintf("main.\"%s\" = \"%s\".\"%s\"", column, referencedTable, referencedColumns[i]))
	}

	var referencedColumnNulls []string
	for _, ref := range referencedColumns {
		referencedColumnNulls = append(referencedColumnNulls, fmt.Sprintf("\"%s\".\"%s\" IS NULL", referencedTable, ref))
	}

	selectColumnStr := quoteAndJoin(referencedColumns, ",")
	if countOnly {
		selectColumnStr = "COUNT(*)"
	}

	sql := fmt.Sprintf(OrphanSql,
		quoteAndJoin(columns, ","),
		fmt.Sprintf("\"%s\"", table),
		strings.Join(columnNotNulls, " AND "),
		//quoteAndJoin(referencedColumns, ","),
		selectColumnStr,
		fmt.Sprintf("\"%s\"", referencedTable),
		strings.Join(joins, " AND "),
		strings.Join(referencedColumnNulls, " AND "),
	)
	return sql
}

func (db *Db) OrphanedCount(
	table string, columns []string, referencedTable string, referencedColumns []string) (int, error) {
	sql := db.OrphanSql(table, columns, referencedTable, referencedColumns, true)
	logrus.Debugln(sql)

	var count int
	err := db.Pool.QueryRow(context.Background(), sql).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (db *Db) OrphanedRows(
	table string, columns []string, referencedTable string, referencedColumns []string) ([]FkOrphanedRow, error) {
	sql := db.OrphanSql(table, columns, referencedTable, referencedColumns, true)
	logrus.Debugln(sql)

	var orphanedRows []FkOrphanedRow
	rows, err := db.Pool.Query(context.Background(), sql)
	if err != nil {
		return orphanedRows, err
	}
	fields := rows.FieldDescriptions()
	var pkColumns []string
	for _, field := range fields {
		pkColumns = append(pkColumns, field.Name)
	}
	for rows.Next() {
		values, err := rows.Values()
		orphanedRows = append(orphanedRows, FkOrphanedRow{
			TableName:         table,
			Columns:           columns,
			ReferencedTable:   referencedTable,
			ReferencedColumns: referencedColumns,
			PrimaryKeyColumns: pkColumns,
			PrimaryKeyValues:  values,
		})
		if err != nil {
			return orphanedRows, err
		}

	}
	return orphanedRows, nil
}

func (db *Db) Fks() ([]FkRow, error) {
	var fkRows []FkRow

	rows, err := db.Pool.Query(context.Background(), AllSql)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var constraintName string
		var tableName string
		var columns []string
		var referencedTable string
		var referencedColumns []string
		var updateRule string
		var deleteRule string

		err := rows.Scan(&constraintName, &tableName,
			&columns, &referencedTable, &referencedColumns,
			&updateRule, &deleteRule)

		if err != nil {
			return nil, err
		}

		fkRows = append(fkRows, FkRow{
			ConstraintName: constraintName, TableName: tableName,
			Columns:           columns, // SQLStringListToSlice(columnsStr),
			ReferencedTable:   referencedTable,
			ReferencedColumns: referencedColumns, // SQLStringListToSlice(referencedColumnsStr),
			UpdateRule:        updateRule,
			DeleteRule:        deleteRule,
		})

	}

	return fkRows, nil
}

func quoteAndJoin(columns []string, separator string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = fmt.Sprintf(`"%s"`, col)
	}
	return strings.Join(quoted, separator)
}

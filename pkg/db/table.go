package db

import (
	"context"
	"fmt"
)

type TableSizeRow struct {
	Database     string
	Name         string
	LogicalBytes uint64
}

const tableSizeSql = `
SELECT t.database_name,
  t.name as table_name,
  sum((crdb_internal.range_stats(r.start_key) ->> 'key_bytes')::INT
    + (crdb_internal.range_stats(r.start_key) ->> 'val_bytes')::INT
    + coalesce((crdb_internal.range_stats(r.start_key) ->> 'range_key_bytes')::INT, 0)
    + coalesce((crdb_internal.range_stats(r.start_key) ->> 'range_val_bytes')::INT, 0)) AS logical_size_bytes
FROM crdb_internal.ranges_no_leases r
  LEFT OUTER JOIN "".crdb_internal.index_spans s ON s.start_key < r.end_key AND s.end_key > r.start_key
  LEFT OUTER JOIN "".crdb_internal.tables t ON s.descriptor_id = t.table_id
WHERE t.database_name = $1 
GROUP BY t.database_name, t.name
`

type ShowTablesRow struct {
	Schema            string
	Name              string
	Type              string
	Owner             string
	EstimatedRowCount int
	Locality          string
}

const showTableSql = `
SHOW TABLES FROM %s
`

// TableSize gets the logical table sizes for all tables/databases
func (db *Db) TableSize(database string) ([]TableSizeRow, error) {
	var rows []TableSizeRow

	rs, err := db.Pool.Query(context.Background(), tableSizeSql, database)

	if err != nil {
		return rows, err
	}

	var dbase string
	var name string
	var logicalBytes uint64

	for rs.Next() {
		err := rs.Scan(&dbase, &name, &logicalBytes)
		if err != nil {
			return rows, err
		}
		rows = append(rows, TableSizeRow{Database: dbase, Name: name, LogicalBytes: logicalBytes})
	}
	return rows, nil
}

// ShowTables returns the output from SHOW TABLES FROM [database]
func (db *Db) ShowTables(database string) ([]ShowTablesRow, error) {
	var rows []ShowTablesRow

	rs, err := db.Pool.Query(context.Background(), fmt.Sprintf(showTableSql, database))

	if err != nil {
		return rows, err
	}

	for rs.Next() {

		var schema string
		var name string
		var typ string
		var owner string
		var estimatedRowCount int
		var locality string

		err := rs.Scan(&schema, &name, &typ, &owner, &estimatedRowCount, &locality)
		if err != nil {
			return rows, err
		}
		rows = append(rows, ShowTablesRow{
			Schema:            schema,
			Name:              name,
			Type:              typ,
			Owner:             owner,
			EstimatedRowCount: estimatedRowCount,
			Locality:          locality,
		})
	}

	return rows, nil
}

package db

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"strconv"
)

type RbrColumns []RbrColumn
type RbrColumn struct {
	Table  string
	Column string
}

type RbrTableRowLocalities struct {
	Table       string
	RowLocality []RbrRowLocality
}

type RbrRowLocality struct {
	Locality string
	Count    int
}

func (r RbrRowLocality) String() string {
	return fmt.Sprintf("Locality: %s, Row count: %d", r.Locality, r.Count)
}

func (db *Db) GetRowCountsOutsideRegion(region string) ([]RbrTableRowLocalities, error) {

	// Get tables that use crdb_internal_region
	sql := `
SELECT table_schema,
       table_name,
       column_name
FROM information_schema.columns
WHERE data_type = 'USER-DEFINED'
  AND udt_name = 'crdb_internal_region'
ORDER BY table_schema, table_name, column_name
`

	logrus.Debugln(sql)

	var schema string
	var table string
	var column string

	type Row struct {
		Schema string
		Table  string
		Column string
	}

	var rows []Row

	res, err := db.Pool.Query(context.Background(), sql)
	if err != nil {
		return nil, err
	}

	for res.Next() {
		err = res.Scan(&schema, &table, &column)
		if err != nil {
			return nil, err
		}
		rows = append(rows, Row{Schema: schema, Table: table, Column: column})
	}

	logrus.Debugf("Found %d rows\n", len(rows))

	res.Close()

	sql = `
SELECT "%s", count(*) FROM "%s"."%s" WHERE "%s" != $1 GROUP BY "%s"
`

	var localities []RbrTableRowLocalities

	// Loop over each column that uses internal_crdb_region type
	for _, row := range rows {

		// Query for counts per region
		fsql := fmt.Sprintf(sql, row.Column, row.Schema, row.Table, row.Column, row.Column)
		logrus.Debug(fsql)
		r, err := db.Pool.Query(context.Background(), fsql, region)
		if err != nil {
			return nil, err
		}

		var reg string
		var count string
		var rowLocalities []RbrRowLocality
		for r.Next() {
			err := r.Scan(&reg, &count)
			if err != nil {
				return nil, err
			}
			cnt, err := strconv.Atoi(count)
			if err != nil {
				return nil, err
			}
			rowLocalities = append(rowLocalities, RbrRowLocality{
				Locality: reg, Count: cnt,
			})
		}

		logrus.Debugf(row.Table)
		for _, locality := range rowLocalities {
			logrus.Debug(locality)
		}
		l := RbrTableRowLocalities{Table: row.Table, RowLocality: rowLocalities}
		localities = append(localities, l)
	}

	return localities, nil
}

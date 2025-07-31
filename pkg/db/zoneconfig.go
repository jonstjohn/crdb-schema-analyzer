package db

import "context"

type ZoneConfigRow struct {
	Target       string
	RawConfigSql string
}

const zoneConfigForTableSql = `
WITH x AS (SHOW ALL ZONE CONFIGURATIONS) SELECT * FROM x WHERE raw_config_sql IS NOT NULL
`

// AllZoneConfigs retrieves all the zone configuration rows
func (db *Db) AllZoneConfigs() ([]ZoneConfigRow, error) {
	var rows []ZoneConfigRow

	rs, err := db.Pool.Query(context.Background(), zoneConfigForTableSql)
	if err != nil {
		return rows, err
	}
	for rs.Next() {
		var target string
		var rawConfigSql string

		err := rs.Scan(&target, &rawConfigSql)
		if err != nil {
			return rows, err
		}
		rows = append(rows, ZoneConfigRow{target, rawConfigSql})
	}

	return rows, nil
}

package analyze

import (
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
)

const ParallelSqlBlockBegin = "-- BEGIN BLOCK"
const ParallelSqlBlockEnd = "-- END BLOCK"

type Converter struct {
	Config   ConverterConfig
	Db       *db.Db
	Analyzer *Analyzer
}

type ConverterConfig struct {
	DbUrl    string
	Database string
}

func NewConverter(config ConverterConfig) (*Converter, error) {

	d, err := db.NewDbDatasource(config.DbUrl, config.Database, true)
	if err != nil {
		return nil, err
	}

	analyzer, err := NewAnalyzer(AnalyzerConfig{
		DbUrl:    config.DbUrl,
		Database: config.Database,
	})

	if err != nil {
		return nil, err
	}

	return &Converter{
		Config:   config,
		Db:       d,
		Analyzer: analyzer,
	}, nil
}

func (c *Converter) Rbr2rbtSqlStatements(primaryRegion string) ([]string, error) {

	var statements []string

	// Get all zone configurations
	zoneConfigs, err := c.Analyzer.AllZoneConfigurations()
	if err != nil {
		return statements, err
	}

	// Iterate through zone configs. If the first lease preference is not for the primary region
	// add a statement to alter the zone configuration to use the primary region
	// The idea is that this will move all data to the primary region, making FK constraint resolution faster
	statements = append(statements, "-- FILE START zoneconfig.sql")
	for _, zc := range zoneConfigs {
		if len(zc.LeasePreferences) > 0 && zc.LeasePreferences[0].Value != fmt.Sprintf("region=%s", primaryRegion) {
			statements = append(statements, ParallelSqlBlockBegin)
			//sql := fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_voters=%d, voter_constraints = '[+region=%s]', lease_preferences = '[[+region=%s]]'",
			//	zc.Target, zc.NumVoters, primaryRegion, primaryRegion)
			sql := fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas=%d, num_voters=%d, constraints = '[+region=%s]', voter_constraints = '[+region=%s]', lease_preferences = '[[+region=%s]]'",
				zc.Target, zc.NumVoters, zc.NumVoters, primaryRegion, primaryRegion, primaryRegion)
			statements = append(statements, sql)
			statements = append(statements, ParallelSqlBlockEnd)
		}
	}
	statements = append(statements, "-- FILE END")

	// Get all tables
	tables, err := c.Analyzer.Tables(false, true)
	if err != nil {
		return statements, err
	}

	// Iterate over tables, checking for FK constraints that need to be changed
	statements = append(statements, "-- FILE START fk.sql")
	for _, table := range tables {
		// Iterate over pairs of FKs to check for redundant ones
		// If it is region restricted, check to see if it is redundant
		// If it is redundant, drop it and don't add another one
		// If it is not redundant, first add an FK without the region
		// then drop the one with the region
		for i, fk1 := range table.FKs {

			// Only region restricted FKs only need to be modified
			if fk1.RegionRestricted {

				var fkStatements []string

				isRedundant := false

				// Iterate over other FKs
				for j, fk2 := range table.FKs {

					// Skips if this is the same FK
					if i == j {
						continue
					}

					// If this is not redundant, add one without region to replace the original
					if fk1.IsRedundantWith(fk2) {
						skipSql := "-- not replacing since this is a redundant FK"
						fkStatements = append(fkStatements, skipSql)
						isRedundant = true
						break
					}

				}

				if !isRedundant {

					// Build the SQL to add the constraint without a region
					addSql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT IF NOT EXISTS %s FOREIGN KEY (%s) REFERENCES %s (%s)",
						quoteIdentifierWithDatabase(c.Config.Database, fk1.Table),
						quoteIdentifier(fk1.GenerateNameNoRegion()),
						quoteAndJoinIdentifiers(fk1.ColumnsNoRegion),
						quoteIdentifier(fk1.ReferencedTable),
						quoteAndJoinIdentifiers(fk1.ReferencedColumnsNoRegion))
					if fk1.UpdateRule != "" {
						addSql = fmt.Sprintf("%s ON UPDATE %s", addSql, fk1.UpdateRule)
					}
					if fk1.DeleteRule != "" {
						addSql = fmt.Sprintf("%s ON DELETE %s", addSql, fk1.DeleteRule)
					}
					fkStatements = append(fkStatements, addSql)
				}

				// Always drop the
				dropSql := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s",
					quoteIdentifierWithDatabase(c.Config.Database, fk1.Table), quoteIdentifier(fk1.Name))
				fkStatements = append(fkStatements, dropSql)

				statements = append(statements, wrapSqlInBlock(fkStatements)...)

			}

		}

	}

	statements = append(statements, "-- FILE END")
	statements = append(statements, "-- FILE START table_locality.sql")

	// Iterate over tables again to change locality
	for _, table := range tables {
		// Add SQL to alter the locality of the table
		sql := fmt.Sprintf("ALTER TABLE \"%s\".\"%s\" SET LOCALITY REGIONAL BY TABLE IN PRIMARY REGION", table.Database, table.Name)
		statements = append(statements, wrapSqlInBlock([]string{sql})...)
	}

	statements = append(statements, "-- FILE END")

	/*
		// Next iterate over tables again to run update crdb_region to the primary region
		// this allows us to remove the non-primary regions from the database
		statements = append(statements, "-- FILE START update_crdb_region.sql")

		// Iterate over tables again to change locality
		for _, table := range tables {
			// Add SQL to alter the locality of the table
			sql := fmt.Sprintf("UPDATE %s SET crdb_region = '%s' WHERE id IN ( SELECT id FROM %s WHERE crdb_region != '%s' LIMIT 100)",
				quoteIdentifierWithDatabase(table.Database, table.Name), primaryRegion,
				quoteIdentifierWithDatabase(table.Database, table.Name), primaryRegion,
			)
			statements = append(statements, wrapSqlInBlock([]string{sql})...)
		}

		statements = append(statements, "-- FILE END")

	*/

	// As an alternative to updated crdb_region to the primary region, just iterate over tables again to change
	// the crdb_region column to a string
	statements = append(statements, "-- FILE START change_crdb_region_type.sql")

	// Iterate over tables again to change locality
	for _, table := range tables {
		// Add SQL to alter the locality of the table
		var sqls []string
		sqls = append(sqls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN crdb_region SET DATA TYPE STRING",
			quoteIdentifierWithDatabase(table.Database, table.Name)))
		sqls = append(sqls,
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN crdb_region SET DEFAULT default_to_database_primary_region(gateway_region())::STRING",
				quoteIdentifierWithDatabase(table.Database, table.Name)))
		statements = append(statements, wrapSqlInBlock(sqls)...)
	}

	statements = append(statements, "-- FILE END")

	// Discard zone overrides for all tables - they will default to RBT
	statements = append(statements, "-- FILE START zone_config_discard.sql")
	for _, table := range tables {
		// Add SQL to alter the locality of the table
		sql := fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE DISCARD",
			quoteIdentifierWithDatabase(table.Database, table.Name))
		statements = append(statements, wrapSqlInBlock([]string{sql})...)
	}

	statements = append(statements, "-- FILE END")

	return statements, nil
}

// wrapSqlInBlock wraps statements in a begin and end that can be used for parallel execution
func wrapSqlInBlock(statements []string) []string {
	return append([]string{ParallelSqlBlockBegin}, append(statements, ParallelSqlBlockEnd)...)
}

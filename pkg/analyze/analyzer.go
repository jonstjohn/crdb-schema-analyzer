package analyze

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
	"slices"
	"sort"
)

type Analyzer struct {
	Config AnalyzerConfig
	Db     *db.Db
}

type AnalyzerConfig struct {
	DbUrl    string
	Database string
}

func NewAnalyzer(config AnalyzerConfig) (*Analyzer, error) {

	d, err := db.NewDbDatasource(config.DbUrl, config.Database, true, 1)
	if err != nil {
		return nil, err
	}
	return &Analyzer{
		Config: config,
		Db:     d,
	}, nil
}

func (a *Analyzer) Fks(filter *FKFilter) ([]FKConstraint, error) {
	fks, err := a.Db.Fks()
	if err != nil {
		return nil, err
	}

	var constraints []FKConstraint

	for _, fk := range fks {
		constraint := FKConstraint{
			Name:                      fk.ConstraintName,
			Table:                     fk.TableName,
			Columns:                   fk.Columns,
			ReferencedTable:           fk.ReferencedTable,
			ReferencedColumns:         fk.ReferencedColumns,
			UpdateRule:                Rule(fk.UpdateRule),
			DeleteRule:                Rule(fk.DeleteRule),
			RegionRestricted:          slices.Contains(fk.Columns, "crdb_region"),
			ColumnsNoRegion:           removeString(fk.Columns, "crdb_region"),
			ReferencedColumnsNoRegion: removeString(fk.ReferencedColumns, "crdb_region"),
		}
		if filter == nil || filter.Matches(constraint) {
			constraints = append(constraints, constraint)
		}

	}

	return constraints, nil
}

// FKOrphanedRowCount Checks for orphaned FK constraint rows
// Orphaned rows occurred as part of https://github.com/cockroachdb/cockroach/issues/150282
// in certain circumstances under "read committed" isolation, cascading deletes failed to delete related rows
// because the cascade was computed without using locks
func (a *Analyzer) FKOrphanedRowCount(constraint FKConstraint) (int, error) {
	return a.Db.OrphanedCount(
		constraint.Table, constraint.Columns,
		constraint.ReferencedTable, constraint.ReferencedColumns)
}

// FKOrphanedRowCount Checks for orphaned FK constraint rows
// Orphaned rows occurred as part of https://github.com/cockroachdb/cockroach/issues/150282
// in certain circumstances under "read committed" isolation, cascading deletes failed to delete related rows
// because the cascade was computed without using locks
func (a *Analyzer) FKOrphans(constraint FKConstraint) ([]FKOrphan, error) {
	var orphans []FKOrphan
	rows, err := a.Db.OrphanedRows(
		constraint.Table, constraint.Columns,
		constraint.ReferencedTable, constraint.ReferencedColumns)
	if err != nil {
		return orphans, err
	}
	for _, row := range rows {
		orphans = append(orphans, FKOrphan{
			Name:              constraint.Name,
			Table:             row.TableName,
			Columns:           row.Columns,
			ReferencedTable:   row.ReferencedTable,
			ReferencedColumns: row.ReferencedColumns,
			ColumnValues:      row.ColumnValues,
			Constraint:        constraint,
		})
	}
	return orphans, nil
}

// FKRedundants returns foreign key constraints that are redundant - i.e., are already contained in
// another FK constraint. This primarily occurs with regional by row tables, where an FK exists that includes
// crdb_region and one that does not. This is typically used when FKs are implement ON DELETE CASCADE SET NULL
// which is not supported for FKs using crdb_region. This narrow case is the focus of the current redundant detection.
func (a *Analyzer) FKRedundants() ([]FKConstraint, error) {

	// Initialize slice for redundant FK constraints
	var redundants []FKConstraint

	// Get all FK constraints
	fks, err := a.Fks(nil)

	if err != nil {
		return redundants, err
	}

	// Create a map of table to FKs so we can more easily iterate over the FKs
	tmap := make(map[string][]FKConstraint)

	for _, fk := range fks {
		if _, ok := tmap[fk.Table]; !ok {
			tmap[fk.Table] = []FKConstraint{}
		}
		tmap[fk.Table] = append(tmap[fk.Table], fk)
	}

	// for iterate over tables
	for _, tfks := range tmap {
		// iterate over table fks
		for i, tfk := range tfks {
			// iterate over table fks again to detect redundant
			for j, otfk := range tfks {
				// skip the same FK
				if i == j {
					continue
				}
				if tfk.IsRedundantWith(otfk) {
					redundants = append(redundants, tfk)
				}
			}
		}
	}
	return redundants, nil
}

// Tables returns tables for all databases
func (a *Analyzer) Tables(includeSize bool, includeFKs bool) ([]Table, error) {

	var tables []Table
	tmap := make(map[string]Table)

	// Get output of SHOW tables
	srows, err := a.Db.ShowTables(a.Config.Database)
	if err != nil {
		return tables, err
	}
	for _, srow := range srows {
		t := Table{}
		if _, ok := tmap[srow.Name]; ok {
			t = tmap[srow.Name]
		}
		t.Name = srow.Name
		t.Database = a.Config.Database
		t.Owner = srow.Owner
		t.EstimatedRowCount = srow.EstimatedRowCount
		t.Locality = srow.Locality
		tmap[srow.Name] = t
	}

	// Get table size
	if includeSize {
		rows, err := a.Db.TableSize(a.Config.Database)
		if err != nil {
			return tables, err
		}
		for _, row := range rows {
			t := Table{}
			if _, ok := tmap[row.Name]; ok {
				t = tmap[row.Name]
			}
			t.Database = row.Database
			t.Name = row.Name
			t.LogicalSizeBytes = row.LogicalBytes
			tmap[row.Name] = t
		}
	}

	// Add FK relationships
	if includeFKs {
		fks, err := a.Fks(nil)
		if err != nil {
			return tables, err
		}
		for _, fk := range fks {
			// FK defined on this table
			if _, ok := tmap[fk.Table]; ok {
				t := tmap[fk.Table]
				t.FKs = append(t.FKs, fk)
				tmap[fk.Table] = t
			}
			// Table referenced from another table, consider referenced
			// lookup referenced table and update its referenced FKs
			if _, ok := tmap[fk.ReferencedTable]; ok {
				t := tmap[fk.ReferencedTable]
				t.ReferencedFKs = append(t.ReferencedFKs, fk)
				tmap[fk.ReferencedTable] = t
			}
		}
	}

	// Get tables from map
	for _, t := range tmap {
		tables = append(tables, t)
	}

	// Sort tables
	sort.Slice(tables, func(i, j int) bool {
		// sort by logical size bytes desc, if present
		if tables[i].LogicalSizeBytes > 0 || tables[j].LogicalSizeBytes > 0 {
			return tables[i].LogicalSizeBytes > tables[j].LogicalSizeBytes
		}
		// sort by row count desc, if present
		if tables[i].EstimatedRowCount > 0 || tables[j].EstimatedRowCount > 0 {
			return tables[i].EstimatedRowCount > tables[j].EstimatedRowCount
		}
		// otherwise, sort by name asc
		return tables[i].Name < tables[j].Name
	})

	return tables, nil
}

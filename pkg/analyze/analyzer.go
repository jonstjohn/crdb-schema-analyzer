package analyze

import (
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
	"slices"
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

	d, err := db.NewDbDatasource(config.DbUrl, config.Database)
	if err != nil {
		return nil, err
	}
	return &Analyzer{
		Config: AnalyzerConfig{},
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
			Name:              fk.ConstraintName,
			Table:             fk.TableName,
			Columns:           fk.Columns,
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: fk.ReferencedColumns,
			UpdateRule:        Rule(fk.UpdateRule),
			DeleteRule:        Rule(fk.DeleteRule),
			RegionRestricted:  slices.Contains(fk.Columns, "crdb_region"),
			ColumnsNoRegion:   removeString(fk.Columns, "crdb_region"),
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

func equalUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[string]int)

	for _, val := range a {
		counts[val]++
	}

	for _, val := range b {
		if counts[val] == 0 {
			return false
		}
		counts[val]--
	}

	return true
}

// removeString removes string from slice of strings
func removeString(s []string, target string) []string {
	for i, v := range s {
		if v == target {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s // target not found, return original
}

// equalSlices checks to see if slices are equal and ordered the same
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

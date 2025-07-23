package analyze

import "github.com/jonstjohn/crdb-schema-analyzer/pkg/db"

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
		}
		if filter != nil && filter.Matches(constraint) {
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
			PrimaryKeyColumns: row.PrimaryKeyColumns,
			PrimaryKeyValues:  row.PrimaryKeyValues,
		})
	}
	return orphans, nil
}

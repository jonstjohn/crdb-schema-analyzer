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

func (a *Analyzer) Fks() ([]FKConstraint, error) {
	fks, err := a.Db.Fks()
	if err != nil {
		return nil, err
	}

	var constraints []FKConstraint

	for _, fk := range fks {
		constraints = append(constraints, FKConstraint{
			Name:              fk.ConstraintName,
			Table:             fk.TableName,
			Columns:           fk.Columns,
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: fk.ReferencedColumns,
			UpdateRule:        Rule(fk.UpdateRule),
			DeleteRule:        Rule(fk.DeleteRule),
		})
	}

	return constraints, nil
}

func (a *Analyzer) CheckForOrphanedRows(constraint FKConstraint) (int, error) {
	return a.Db.OrphanedCount(
		constraint.Table, constraint.Columns,
		constraint.ReferencedTable, constraint.ReferencedColumns)
}

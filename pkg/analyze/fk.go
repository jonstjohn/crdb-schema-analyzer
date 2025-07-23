package analyze

import (
	"fmt"
	"strings"
)

type FKConstraint struct {
	Name              string
	Table             string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	UpdateRule        Rule
	DeleteRule        Rule
}

type Rule string

const (
	RuleNoAction Rule = "NO ACTION"
	RuleCascade  Rule = "CASCADE"
	RuleSetNull  Rule = "SET NULL"
)

func (fk FKConstraint) String() string {
	return fmt.Sprintf(
		"%s: CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s",
		fk.Table,
		fk.Name,
		strings.Join(fk.Columns, ", "),
		fk.ReferencedTable,
		strings.Join(fk.ReferencedColumns, ", "),
		fk.UpdateRule,
		fk.DeleteRule,
	)
}

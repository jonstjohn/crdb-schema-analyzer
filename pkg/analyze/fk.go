package analyze

import (
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
	"strings"
)

type FKConstraint struct {
	Name                      string
	Table                     string
	Columns                   []string
	ReferencedTable           string
	ReferencedColumns         []string
	UpdateRule                Rule
	DeleteRule                Rule
	RegionRestricted          bool
	ColumnsNoRegion           []string
	ReferencedColumnsNoRegion []string
}

type FKOrphan struct {
	Name              string
	Table             string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	ColumnValues      []any
	Constraint        FKConstraint
}

type FKFilter struct {
	Tables      []string
	Constraints []string
	Rules       []FKFilterRule
}

type FKFilterRule struct {
	Type FKFilterRuleType
	Rule Rule
}

type FKFilterRuleType string

const (
	FKFilterRuleTypeUpdate = "UPDATE"
	FKFilterRuleTypeDelete = "DELETE"
)

type FKRedundant struct {
	Table string
	FKs   []FKConstraint
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

// GenerateNameNoRegion is used to generate an FK constraint name that does not contain crdb_region
// this is used for converting tables from RBR to RBT
func (fk FKConstraint) GenerateNameNoRegion() string {
	return fmt.Sprintf("%s_%s_fkey", fk.Table, strings.Join(fk.ColumnsNoRegion, "_"))
}

func NewFKFilter(tables []string, constraints []string, ruleStrs []string) (*FKFilter, error) {
	var filterRules []FKFilterRule
	if len(ruleStrs) > 0 {
		for _, ruleStr := range ruleStrs {
			r, err := NewFkFilterRuleFromString(ruleStr)
			if err != nil {
				return nil, err
			}
			filterRules = append(filterRules, *r)
		}
	}
	return &FKFilter{
		Tables:      tables,
		Constraints: constraints,
		Rules:       filterRules,
	}, nil
}

func NewFkFilterRuleFromString(str string) (*FKFilterRule, error) {
	// strip "ON"
	str = strings.TrimSpace(strings.ReplaceAll(str, "ON", ""))

	var ruleType FKFilterRuleType
	if strings.Contains(str, FKFilterRuleTypeUpdate) {
		str = strings.TrimSpace(strings.ReplaceAll(str, FKFilterRuleTypeUpdate, ""))
		ruleType = FKFilterRuleTypeUpdate
	}
	if strings.Contains(str, FKFilterRuleTypeDelete) {
		str = strings.TrimSpace(strings.ReplaceAll(str, FKFilterRuleTypeDelete, ""))
		ruleType = FKFilterRuleTypeUpdate
	}

	rule, err := parseRule(str)
	if err != nil {
		return nil, err
	}
	return &FKFilterRule{
		Type: ruleType,
		Rule: rule,
	}, nil

}

func (filter *FKFilter) Matches(fk FKConstraint) bool {
	constraintMatches := true
	tableMatches := true
	ruleMatches := true
	if len(filter.Constraints) > 0 {
		found := false
		for _, constraint := range filter.Constraints {
			if fk.Name == constraint {
				found = true
			}
		}
		constraintMatches = found
	}
	if len(filter.Tables) > 0 {
		found := false
		for _, table := range filter.Tables {
			if fk.Table == table {
				found = true
			}
		}
		tableMatches = found
	}
	if len(filter.Rules) > 0 {
		found := false
		for _, rule := range filter.Rules {
			if rule.Type == FKFilterRuleTypeUpdate && fk.UpdateRule == rule.Rule {
				found = true
			}
			if rule.Type == FKFilterRuleTypeDelete && fk.DeleteRule == rule.Rule {
				found = true
			}
		}
		ruleMatches = found
	}
	return constraintMatches && tableMatches && ruleMatches
}

func (orphan *FKOrphan) Sql() (string, error) {
	return db.DeleteByColumnValuesWithExistsCheckSql(orphan.Table, orphan.Columns, orphan.ColumnValues,
		orphan.Constraint.ReferencedTable, orphan.Constraint.ReferencedColumns, orphan.ColumnValues)
}

func parseRule(s string) (Rule, error) {
	switch Rule(strings.ToUpper(strings.TrimSpace(s))) {
	case RuleNoAction:
		return RuleNoAction, nil
	case RuleCascade:
		return RuleCascade, nil
	case RuleSetNull:
		return RuleSetNull, nil
	default:
		return "", fmt.Errorf("invalid Rule: %q", s)
	}
}

// IsRedundantWith determines whether a FK constraint is redundant with another FK constraint
// this occurs when a region restricted FK constraint with a NO ACTION delete has the same UPDATE rule
// as a non-region restricted FK constraint on the same rows
// this is a very narrow case
func (fk FKConstraint) IsRedundantWith(c2 FKConstraint) bool {

	// Make sure they are referencing the same table with the same columns
	if fk.Table != c2.Table || !equalSlices(fk.ColumnsNoRegion, c2.ColumnsNoRegion) {
		return false
	}
	// If this constraint is not region restricted or the second one is, not redundant
	if !fk.RegionRestricted || c2.RegionRestricted {
		return false
	}

	// If update rules are different, not redundant
	if fk.UpdateRule != c2.UpdateRule {
		return false
	}

	// If delete rule is anything other than NO ACTION, not redundant
	if fk.DeleteRule != RuleNoAction {
		return false
	}

	return true
}

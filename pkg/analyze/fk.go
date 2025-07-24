package analyze

import (
	"fmt"
	"github.com/jonstjohn/crdb-schema-analyzer/pkg/db"
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
	return db.DeleteByColumnValuesWithExistsCheckSql(orphan.ReferencedTable, orphan.ReferencedColumns, orphan.ColumnValues,
		orphan.Constraint.Table, orphan.Constraint.Columns, orphan.ColumnValues)
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

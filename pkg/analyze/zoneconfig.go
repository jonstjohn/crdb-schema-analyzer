package analyze

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type ZoneConfig struct {
	Target           string
	RangeMinBytes    int
	RangeMaxBytes    int
	GcTtlSeconds     int
	NumReplicas      int
	NumVoters        int
	Constraints      []ZoneConfigConstraint
	VoterConstraints []ZoneConfigConstraint
	LeasePreferences []ZoneConfigConstraint
}

type ZoneConfigConstraint struct {
	Value string
	Type  ZoneConfigConstraintType
	Scope int
}

type ZoneConfigConstraintType string

const (
	ZoneConfigConstraintTypeRequired   ZoneConfigConstraintType = "+"
	ZoneConfigConstraintTypeProhibited ZoneConfigConstraintType = "-"
)

// AllZoneConfigurations retrieves all zone configurations
func (a *Analyzer) AllZoneConfigurations() ([]ZoneConfig, error) {
	var zones []ZoneConfig

	rs, err := a.Db.AllZoneConfigs()
	if err != nil {
		return zones, err
	}

	for _, r := range rs {

		if !strings.Contains(r.Target, fmt.Sprintf(" %s", a.Config.Database)) {
			continue
		}
		zc, err := parseZoneConfig(r.RawConfigSql)
		if err != nil {
			return zones, err
		}
		zc.Target = r.Target
		zones = append(zones, zc)
	}
	return zones, nil
}

// parseZoneConfig takes a string zone configuration and parses it into an the ZoneConfig struct
func parseZoneConfig(input string) (ZoneConfig, error) {
	var config ZoneConfig

	// Range min bytes
	re := regexp.MustCompile(`range_min_bytes = (\d+)`)
	matches := re.FindStringSubmatch(input)
	if len(matches) == 2 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return config, err
		}
		config.RangeMinBytes = val
	}

	// Range max bytes
	re = regexp.MustCompile(`range_max_bytes = (\d+)`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return config, err
		}
		config.RangeMaxBytes = val
	}

	// GC TTL seconds
	re = regexp.MustCompile(`gc.ttlseconds = (\d+)`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return config, err
		}
		config.GcTtlSeconds = val
	}

	// Num voters
	re = regexp.MustCompile(`num_voters = (\d+)`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return config, err
		}
		config.NumVoters = val
	}

	// Num replicas
	re = regexp.MustCompile(`num_replicas = (\d+)`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		val, err := strconv.Atoi(matches[1])
		if err != nil {
			return config, err
		}
		config.NumReplicas = val
	}

	// Constraints
	re = regexp.MustCompile(`constraints = '([^']*)'`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		constraints, err := parseConstraints(matches[1])
		if err != nil {
			return config, err
		}
		config.Constraints = constraints
	}

	// Voter Constraints
	re = regexp.MustCompile(`voter_constraints = '([^']*)'`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		vconstraints, err := parseConstraints(matches[1])
		if err != nil {
			return config, err
		}
		config.VoterConstraints = vconstraints
	}

	// Lease preferences
	re = regexp.MustCompile(`lease_preferences = '([^']*)'`)
	matches = re.FindStringSubmatch(input)
	if len(matches) == 2 {
		prefs, err := parseConstraints(matches[1])
		if err != nil {
			return config, err
		}
		config.LeasePreferences = prefs
	}

	return config, nil
}

// parseConstraints from an input string
func parseConstraints(input string) ([]ZoneConfigConstraint, error) {

	var constraints []ZoneConfigConstraint
	// Remove the first and last character if it is either any of []{}
	if len(input) >= 2 && input[0] == '[' && input[len(input)-1] == ']' {
		input = input[1 : len(input)-1]
	}
	if len(input) >= 2 && input[0] == '{' && input[len(input)-1] == '}' {
		input = input[1 : len(input)-1]
	}

	if len(input) == 0 {
		return constraints, nil
	}

	// Split the input
	parts := strings.Split(input, ",")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) >= 2 && part[0] == '[' && part[len(part)-1] == ']' {
			part = part[1 : len(part)-1]
		}
		part = strings.TrimSpace(part)
		constraint := ZoneConfigConstraint{}
		if len(part) == 0 {
			return constraints, errors.New(fmt.Sprintf("error processing constraints, part %d in '%s' is empty", i, input))
		}
		re := regexp.MustCompile(`^([+-])([a-zA-Z0-9_-]+=.+?)(?::\s*(\d+))?$`)
		matches := re.FindStringSubmatch(part)
		if len(matches) < 3 {
			return constraints, fmt.Errorf("zone config: invalid zone config constraint: %s", part)
		}

		typeStr := matches[1]
		if typeStr != "+" && typeStr != "-" {
			return constraints, errors.New("zone config: unknown scope")
		}
		constraint.Type = ZoneConfigConstraintType(typeStr)
		constraint.Value = matches[2]

		if len(matches) == 4 && len(matches[3]) > 0 {
			scope, err := strconv.Atoi(matches[3])
			if err != nil {
				return constraints, errors.New("zone config: invalid scope")
			}
			constraint.Scope = scope
		}

		constraints = append(constraints, constraint)

	}

	return constraints, nil
}

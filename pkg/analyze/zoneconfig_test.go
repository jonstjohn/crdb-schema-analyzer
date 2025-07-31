package analyze

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var configsTest = []string{`
	ALTER TABLE t CONFIGURE ZONE USING
      range_min_bytes = 134217728,
      range_max_bytes = 536870912,
      gc.ttlseconds = 14400,
      num_replicas = 5,
      num_voters = 3,
      constraints = '{+region=us-east1: 3}',
      voter_constraints = '[+region=us-east1]',
      lease_preferences = '[[+region=us-east1],[+region=us-west1]]'
`}

var configsExpected = []ZoneConfig{
	{
		RangeMinBytes: 134217728,
		RangeMaxBytes: 536870912,
		GcTtlSeconds:  14400,
		NumReplicas:   5,
		NumVoters:     3,
		Constraints: []ZoneConfigConstraint{
			{Value: "region=us-east1", Type: ZoneConfigConstraintTypeRequired, Scope: 3},
		},
		VoterConstraints: []ZoneConfigConstraint{
			{Value: "region=us-east1", Type: ZoneConfigConstraintTypeRequired, Scope: 0},
		},
		LeasePreferences: []ZoneConfigConstraint{
			{Value: "region=us-east1", Type: ZoneConfigConstraintTypeRequired, Scope: 0},
			{Value: "region=us-west1", Type: ZoneConfigConstraintTypeRequired, Scope: 0},
		},
	},
}

func TestZoneConfigParser(t *testing.T) {
	for i, test := range configsTest {
		config, err := parseZoneConfig(test)
		assert.NoError(t, err)
		assert.Equal(t, configsExpected[i].RangeMinBytes, config.RangeMinBytes)
		assert.Equal(t, configsExpected[i].RangeMaxBytes, config.RangeMaxBytes)
		assert.Equal(t, configsExpected[i].GcTtlSeconds, config.GcTtlSeconds)
		assert.Equal(t, configsExpected[i].NumReplicas, config.NumReplicas)
		assert.Equal(t, configsExpected[i].NumVoters, config.NumVoters)
		assert.Equal(t, configsExpected[i].Constraints, config.Constraints)
		assert.Equal(t, configsExpected[i].VoterConstraints, config.VoterConstraints)
		assert.Equal(t, configsExpected[i].LeasePreferences, config.LeasePreferences)

		/*
			// LeasePreferences
			constraints, err := parseConstraints(config.Constraints)

			assert.NoError(t, err)
			assert.Equal(t, 1, len(constraints))

			assert.Equal(t, ZoneConfigConstraintTypeRequired, constraints[0].Type)
			assert.Equal(t, "region=us-east1", constraints[0].Value)
			assert.Equal(t, 3, constraints[0].Scope)
			assert.Equal(t, 1, cons)

			// Voter constraints
			vconstraints, err := parseConstraints(config.VoterConstraints)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(vconstraints))
			assert.Equal(t, ZoneConfigConstraintTypeRequired, vconstraints[0].Type)
			assert.Equal(t, "region=us-east1", vconstraints[0].Value)
			assert.Equal(t, 0, vconstraints[0].Scope)

			// Lease preferences
			leaseprefs, err := parseConstraints(config.LeasePreferences)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(leaseprefs))
			assert.Equal(t, ZoneConfigConstraintTypeRequired, leaseprefs[0].Type)
			assert.Equal(t, "region=us-east1", leaseprefs[0].Value)
			assert.Equal(t, 0, leaseprefs[0].Scope)
		*/
	}
}

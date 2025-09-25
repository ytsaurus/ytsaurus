package resourceusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetListTimestamps(t *testing.T) {
	basicTables := []*ResourceUsageTable{
		{Timestamp: 300},
		{Timestamp: 400},
		{Timestamp: 500},
	}
	tests := []struct {
		name          string
		tables        []*ResourceUsageTable
		minTimestamp  int64
		maxTimestamp  int64
		expected      []int64
		expectError   bool
		errorContains string
	}{
		{
			name:         "all tables",
			tables:       basicTables,
			minTimestamp: 300,
			maxTimestamp: 500,
			expected:     []int64{300, 400, 500},
			expectError:  false,
		},
		{
			name:          "no tables, lower bound",
			tables:        basicTables,
			minTimestamp:  100,
			maxTimestamp:  200,
			expectError:   true,
			errorContains: "no timestamps found in the range [100, 200]",
		},
		{
			name:          "no tables, upper bound",
			tables:        basicTables,
			minTimestamp:  600,
			maxTimestamp:  700,
			expectError:   true,
			errorContains: "no timestamps found in the range [600, 700]",
		},
		{
			name:         "one table, no range",
			tables:       basicTables,
			minTimestamp: 400,
			maxTimestamp: 400,
			expected:     []int64{400},
			expectError:  false,
		},
		{
			name:         "one table, lower bound",
			tables:       basicTables,
			minTimestamp: 200,
			maxTimestamp: 300,
			expected:     []int64{300},
			expectError:  false,
		},
		{
			name:         "one table, upper bound",
			tables:       basicTables,
			minTimestamp: 500,
			maxTimestamp: 600,
			expected:     []int64{500},
			expectError:  false,
		},
		{
			name:          "no tables, between",
			tables:        basicTables,
			minTimestamp:  410,
			maxTimestamp:  490,
			expected:      []int64{},
			expectError:   true,
			errorContains: "no timestamps found in the range [410, 490]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &Cluster{
				ResourceUsageTables: tt.tables,
			}

			timestamps, err := cluster.GetListTimestamps(tt.minTimestamp, tt.maxTimestamp)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				if len(tt.expected) == 0 {
					assert.Empty(t, timestamps)
				} else {
					assert.Equal(t, tt.expected, timestamps)
				}
			}
		})
	}
}

func TestGetResourceUsageTableByTimestamp(t *testing.T) {
	tables := []*ResourceUsageTable{
		{Timestamp: 100},
		{Timestamp: 200},
		{Timestamp: 300},
		{Timestamp: 400},
		{Timestamp: 500},
	}

	tests := []struct {
		name                    string
		tables                  []*ResourceUsageTable
		timestamp               int64
		timestampRoundingPolicy string
		expected                int64
		expectError             bool
		errorContains           string
	}{
		{
			name:                    "exact_match_with_closest_policy",
			tables:                  tables,
			timestamp:               300,
			timestampRoundingPolicy: roundingPolicyClosest,
			expected:                300,
			expectError:             false,
		},
		{
			name:                    "closest_match_with_forward_policy",
			tables:                  tables,
			timestamp:               351,
			timestampRoundingPolicy: roundingPolicyClosest,
			expected:                400,
			expectError:             false,
		},
		{
			name:                    "closest_match_with_backward_policy",
			tables:                  tables,
			timestamp:               349,
			timestampRoundingPolicy: roundingPolicyClosest,
			expected:                300,
			expectError:             false,
		},
		{
			name:                    "exact_match_with_backward_policy",
			tables:                  tables,
			timestamp:               300,
			timestampRoundingPolicy: roundingPolicyBackward,
			expected:                300,
			expectError:             false,
		},
		{
			name:                    "backward_policy_match",
			tables:                  tables,
			timestamp:               350,
			timestampRoundingPolicy: roundingPolicyBackward,
			expected:                300,
			expectError:             false,
		},
		{
			name:                    "backward_policy_no_match",
			tables:                  tables,
			timestamp:               50,
			timestampRoundingPolicy: roundingPolicyBackward,
			expectError:             true,
			errorContains:           "no backward timestamp found for 50",
		},
		{
			name:                    "exact_match_with_forward_policy",
			tables:                  tables,
			timestamp:               300,
			timestampRoundingPolicy: roundingPolicyForward,
			expected:                300,
			expectError:             false,
		},
		{
			name:                    "forward_policy_match",
			tables:                  tables,
			timestamp:               250,
			timestampRoundingPolicy: roundingPolicyForward,
			expected:                300,
			expectError:             false,
		},
		{
			name:                    "forward_policy_no_match",
			tables:                  tables,
			timestamp:               600,
			timestampRoundingPolicy: roundingPolicyForward,
			expectError:             true,
			errorContains:           "no forward timestamp found for 600",
		},
		{
			name:                    "invalid_policy",
			tables:                  tables,
			timestamp:               300,
			timestampRoundingPolicy: "invalid",
			expectError:             true,
			errorContains:           "invalid timestamp rounding policy: invalid",
		},
		{
			name:                    "empty_tables_with_closest_policy",
			tables:                  []*ResourceUsageTable{},
			timestamp:               300,
			timestampRoundingPolicy: roundingPolicyClosest,
			expectError:             true,
			errorContains:           "no closest timestamp found for 300",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &Cluster{
				ResourceUsageTables: tt.tables,
			}

			result, err := cluster.GetResourceUsageTableByTimestamp(tt.timestamp, tt.timestampRoundingPolicy)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tt.expected, result.Timestamp)
			}
		})
	}
}

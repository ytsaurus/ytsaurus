package ytsys

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkIntegrity_Check(t *testing.T) {
	type Query struct {
		i      *ChunkIntegrity
		maxURC float64
	}

	for _, tc := range []struct {
		name                string
		q                   *Query
		intact              bool
		intactUnrecoverable bool
	}{
		{
			name: "intact",
			q: &Query{
				i: &ChunkIntegrity{
					RequisitionUpdateEnabled: true,
					RefreshEnabled:           true,
					ReplicatorEnabled:        true,
				},
			},
			intact:              true,
			intactUnrecoverable: true,
		},
		{
			name: "lvc",
			q: &Query{
				i: &ChunkIntegrity{
					LVC:                      1,
					RequisitionUpdateEnabled: true,
					RefreshEnabled:           true,
					ReplicatorEnabled:        true,
				},
			},
			intact:              false,
			intactUnrecoverable: false,
		},
		{
			name: "qmc",
			q: &Query{
				i: &ChunkIntegrity{
					QMC:                      1,
					RequisitionUpdateEnabled: true,
					RefreshEnabled:           true,
					ReplicatorEnabled:        true,
				},
			},
			intact:              false,
			intactUnrecoverable: false,
		},
		{
			name: "urc",
			q: &Query{
				i: &ChunkIntegrity{
					C:                        3,
					URC:                      2,
					RequisitionUpdateEnabled: true,
					RefreshEnabled:           true,
					ReplicatorEnabled:        true,
				},
				maxURC: 0.5, // 2/3 > 0.5
			},
			intact:              false,
			intactUnrecoverable: true,
		},
		{
			name: "requisition_update_disabled",
			q: &Query{
				i: &ChunkIntegrity{
					RequisitionUpdateEnabled: false,
					RefreshEnabled:           true,
					ReplicatorEnabled:        true,
				},
			},
			intact:              false,
			intactUnrecoverable: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf(tc.q.i.String())
			require.Equal(t, tc.intact, tc.q.i.Check(tc.q.maxURC))
			require.Equal(t, tc.intactUnrecoverable, tc.q.i.CheckUnrecoverable())
		})
	}
}

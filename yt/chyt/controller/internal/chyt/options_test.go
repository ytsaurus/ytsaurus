package chyt

import (
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"testing"
)

func TestDescribeResourceDefaults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		config      Config
		cpu, memory uint64
	}{
		{name: "builtin", cpu: defaultInstanceCPU, memory: (&InstanceMemory{}).totalMemory()},
		{name: "configured", config: Config{ResourcesConfig: &ResourcesConfig{DefaultInstanceCPU: ptr.Uint64(8), DefaultInstanceMemory: ptr.Uint64(40 * gib)}}, cpu: 8, memory: 40 * gib},
		{name: "partial", config: Config{ResourcesConfig: &ResourcesConfig{DefaultInstanceCPU: ptr.Uint64(32)}}, cpu: 32, memory: (&InstanceMemory{}).totalMemory()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{config: tc.config}
			edited := Speclet{Resources: Resources{InstanceCount: ptr.Uint64(3), InstanceCPU: ptr.Uint64(6), InstanceTotalMemory: ptr.Uint64(30 * gib)}}
			creation := c.DescribeOptions(Speclet{})[0]
			editing := c.DescribeOptions(edited)[0]
			defaults := []uint64{defaultInstanceCount, tc.cpu, tc.memory}
			currents := []*uint64{edited.InstanceCount, edited.InstanceCPU, edited.InstanceTotalMemory}
			types := []strawberry.OptionType{strawberry.TypeInt64, strawberry.TypeInt64, strawberry.TypeByteCount}
			for i, option := range creation.Options {
				require.EqualValues(t, defaults[i], option.DefaultValue)
				require.Equal(t, types[i], option.Type)
				require.NotNil(t, option.MinValue)
				require.NotNil(t, option.MaxValue)
				require.Equal(t, currents[i], editing.Options[i].CurrentValue)
				editing.Options[i].CurrentValue = option.CurrentValue
				require.Equal(t, option, editing.Options[i])
			}
		})
	}
}

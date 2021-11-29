package chyt

import (
	"testing"

	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/ptr"
	"github.com/stretchr/testify/require"
)

func createFakeController() (c *Controller) {
	c = &Controller{
		l:       &nop.Logger{},
		ytc:     nil,
		root:    "",
		cluster: "",
	}
	return
}

func requireNotNilResources(t *testing.T, r *Resources) {
	require.NotNil(t, r.CliqueCPU)
	require.NotNil(t, r.CliqueMemory)
	require.NotNil(t, r.InstanceCount)
	require.NotNil(t, r.InstanceCPU)
	require.NotNil(t, r.InstanceTotalMemory)
	require.NotNil(t, r.InstanceMemory)

	require.NotNil(t, r.InstanceMemory.ClickHouse)
	require.NotNil(t, r.InstanceMemory.ChunkMetaCache)
	require.NotNil(t, r.InstanceMemory.CompressedCache)
	require.NotNil(t, r.InstanceMemory.UncompressedCache)
	require.NotNil(t, r.InstanceMemory.Reader)

}

func TestPopulateResourcesClique(t *testing.T) {
	c := createFakeController()

	resources := &Resources{
		CliqueCPU: ptr.Uint64(160),
	}
	err := c.populateResourcesClique(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(10), *resources.InstanceCount)

	resources = &Resources{
		CliqueMemory: ptr.Uint64(180 * gib),
	}
	err = c.populateResourcesClique(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(2), *resources.InstanceCount)

	resources = &Resources{
		CliqueCPU:    ptr.Uint64(160),
		CliqueMemory: ptr.Uint64(180 * gib),
	}
	err = c.populateResourcesClique(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(2), *resources.InstanceCount)

	resources = &Resources{
		CliqueCPU:    ptr.Uint64(16),
		CliqueMemory: ptr.Uint64(180 * gib),
	}
	err = c.populateResourcesClique(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(1), *resources.InstanceCount)

	// Should not specify InstanceCount and Clique resources together.
	resources = &Resources{
		CliqueCPU:     ptr.Uint64(16),
		InstanceCount: ptr.Uint64(1),
	}
	err = c.populateResourcesClique(resources)
	require.Error(t, err)

	// Should not specify Instance resources and Clique resources together.
	resources = &Resources{
		CliqueCPU:   ptr.Uint64(16),
		InstanceCPU: ptr.Uint64(16),
	}
	err = c.populateResourcesClique(resources)
	require.Error(t, err)
}

func TestPopulateResourcesInstance(t *testing.T) {
	c := createFakeController()

	resources := &Resources{
		InstanceCount: ptr.Uint64(2),
	}
	err := c.populateResourcesInstance(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, memDefault.totalMemory(), resources.InstanceMemory.totalMemory())
	require.Equal(t, memDefault.totalMemory(), *resources.InstanceTotalMemory)
	require.Equal(t, 2*memDefault.totalMemory(), *resources.CliqueMemory)
	require.Equal(t, uint64(cpu), *resources.InstanceCPU)
	require.Equal(t, uint64(2*cpu), *resources.CliqueCPU)

	resources = &Resources{
		InstanceCount:       ptr.Uint64(2),
		InstanceTotalMemory: ptr.Uint64(memNonElastic + memElastic/2),
	}
	err = c.populateResourcesInstance(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(memClickHouse/2), *resources.InstanceMemory.ClickHouse)
	require.Equal(t, uint64(memChunkMetaCache/2), *resources.InstanceMemory.ChunkMetaCache)
	require.Equal(t, uint64(memCompressedBlockCache/2), *resources.InstanceMemory.CompressedCache)
	require.Equal(t, uint64(memUncompressedBlockCache/2), *resources.InstanceMemory.UncompressedCache)
	require.Equal(t, uint64(memReader/2), *resources.InstanceMemory.Reader)

	resources = &Resources{
		InstanceCount: ptr.Uint64(2),
		InstanceMemory: &InstanceMemory{
			ClickHouse: ptr.Uint64(2 * memClickHouse),
		},
	}
	err = c.populateResourcesInstance(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(2*memClickHouse), *resources.InstanceMemory.ClickHouse)
	require.Equal(t, uint64(memChunkMetaCache), *resources.InstanceMemory.ChunkMetaCache)
	require.Equal(t, uint64(memCompressedBlockCache), *resources.InstanceMemory.CompressedCache)
	require.Equal(t, uint64(memUncompressedBlockCache), *resources.InstanceMemory.UncompressedCache)
	require.Equal(t, uint64(memReader), *resources.InstanceMemory.Reader)

	// InstanceCount should be present.
	resources = &Resources{}
	err = c.populateResourcesInstance(resources)
	require.Error(t, err)

	// Should not specify InstanceMemory and InstanceTotalMemory simultaniusly.
	resources = &Resources{
		InstanceCount:       ptr.Uint64(2),
		InstanceMemory:      memDefault,
		InstanceTotalMemory: ptr.Uint64(50 * gib),
	}
	err = c.populateResourcesInstance(resources)
	require.Error(t, err)

	// Too low memory limit.
	resources = &Resources{
		InstanceCount:       ptr.Uint64(2),
		InstanceTotalMemory: ptr.Uint64(memNonElastic - 1),
	}
	err = c.populateResourcesInstance(resources)
	require.Error(t, err)
}

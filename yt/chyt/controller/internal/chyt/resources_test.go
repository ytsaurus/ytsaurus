package chyt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/ptr"
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

	memDefault := &InstanceMemory{}

	resources := &Resources{
		InstanceCount: ptr.Uint64(2),
	}
	err := c.populateResourcesInstance(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, memDefault.totalMemory(), resources.InstanceMemory.totalMemory())
	require.Equal(t, memDefault.totalMemory(), *resources.InstanceTotalMemory)
	require.Equal(t, 2*memDefault.totalMemory(), *resources.CliqueMemory)
	require.Equal(t, uint64(defaultInstanceCPU), *resources.InstanceCPU)
	require.Equal(t, uint64(2*defaultInstanceCPU), *resources.CliqueCPU)

	resources = &Resources{
		InstanceCount:       ptr.Uint64(2),
		InstanceTotalMemory: ptr.Uint64(memNonElastic + memElastic/2),
	}
	err = c.populateResourcesInstance(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(defaultMemoryClickHouse/2), resources.InstanceMemory.ClickHouseOrDefault())
	require.Equal(t, uint64(defaultMemoryChunkMetaCache/2), resources.InstanceMemory.ChunkMetaCacheOrDefault())
	require.Equal(t, uint64(defaultMemoryCompressedBlockCache/2), resources.InstanceMemory.CompressedBlockCacheOrDefault())
	require.Equal(t, uint64(defaultMemoryUncompressedBlockCache/2), resources.InstanceMemory.UncompressedBlockCacheOrDefault())
	require.Equal(t, uint64(defaultMemoryReader/2), resources.InstanceMemory.ReaderOrDefault())

	resources = &Resources{
		InstanceCount: ptr.Uint64(2),
		InstanceMemory: &InstanceMemory{
			ClickHouse: ptr.Uint64(2 * defaultMemoryClickHouse),
		},
	}
	err = c.populateResourcesInstance(resources)
	require.NoError(t, err)
	requireNotNilResources(t, resources)
	require.Equal(t, uint64(2*defaultMemoryClickHouse), resources.InstanceMemory.ClickHouseOrDefault())
	require.Equal(t, uint64(defaultMemoryChunkMetaCache), resources.InstanceMemory.ChunkMetaCacheOrDefault())
	require.Equal(t, uint64(defaultMemoryCompressedBlockCache), resources.InstanceMemory.CompressedBlockCacheOrDefault())
	require.Equal(t, uint64(defaultMemoryUncompressedBlockCache), resources.InstanceMemory.UncompressedBlockCacheOrDefault())
	require.Equal(t, uint64(defaultMemoryReader), resources.InstanceMemory.ReaderOrDefault())

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

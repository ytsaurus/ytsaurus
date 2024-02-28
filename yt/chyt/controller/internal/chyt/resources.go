package chyt

import (
	"fmt"

	"go.ytsaurus.tech/library/go/ptr"
)

const (
	gib = 1024 * 1024 * 1024

	defaultMemoryClickHouse             = 16 * gib
	defaultMemoryChunkMetaCache         = 1 * gib
	defaultMemoryCompressedBlockCache   = 16 * gib
	defaultMemoryUncompressedBlockCache = 0
	defaultMemoryReader                 = 12 * gib

	memElastic = defaultMemoryClickHouse +
		defaultMemoryChunkMetaCache +
		defaultMemoryCompressedBlockCache +
		defaultMemoryUncompressedBlockCache +
		defaultMemoryReader

	defaultMemoryFootprint = 10 * gib

	defaultMemoryClickHouseWatermark = 10 * gib

	defaultMemoryWatchdogOOMWindowWatermark = 20 * gib
	defaultMemoryWatchdogOOMWatermark       = 4 * gib

	memNonElastic = defaultMemoryFootprint +
		defaultMemoryClickHouseWatermark

	defaultInstanceCPU = 16

	defaultInstanceCount = 1
	maxInstanceCount     = 100
)

type InstanceMemory struct {
	ClickHouse     *uint64 `yson:"clickhouse" json:"clickhouse"`
	ChunkMetaCache *uint64 `yson:"chunk_meta_cache" json:"chunk_meta_cache"`
	// NOTE(dakovalkov): compressed and uncompressed block caches misses "block" in yson-name
	// for compatibility reasons.
	// TODO(dakovalkov): rename it and migrate existing configs.
	CompressedBlockCache   *uint64 `yson:"compressed_cache" json:"compressed_cache"`
	UncompressedBlockCache *uint64 `yson:"uncompressed_cache" json:"uncompressed_cache"`
	Reader                 *uint64 `yson:"reader" json:"reader"`
	// Some less useful fields.
	ClickHouseWatermark        *uint64 `yson:"clickhouse_watermark" json:"clickhouse_watermark"`
	WatchdogOOMWatermark       *uint64 `yson:"watchdog_oom_watermark" json:"watchdog_oom_watermark"`
	WatchdogOOMWindowWatermark *uint64 `yson:"watchdog_oom_window_watermark" json:"watchdog_oom_window_watermark"`
	Footprint                  *uint64 `yson:"footprint" json:"footprint"`
}

func (mem *InstanceMemory) ClickHouseOrDefault() uint64 {
	if mem.ClickHouse != nil {
		return *mem.ClickHouse
	}
	return defaultMemoryClickHouse
}

func (mem *InstanceMemory) ChunkMetaCacheOrDefault() uint64 {
	if mem.ChunkMetaCache != nil {
		return *mem.ChunkMetaCache
	}
	return defaultMemoryChunkMetaCache
}

func (mem *InstanceMemory) CompressedBlockCacheOrDefault() uint64 {
	if mem.CompressedBlockCache != nil {
		return *mem.CompressedBlockCache
	}
	return defaultMemoryCompressedBlockCache
}

func (mem *InstanceMemory) UncompressedBlockCacheOrDefault() uint64 {
	if mem.UncompressedBlockCache != nil {
		return *mem.UncompressedBlockCache
	}
	return defaultMemoryUncompressedBlockCache
}

func (mem *InstanceMemory) ReaderOrDefault() uint64 {
	if mem.Reader != nil {
		return *mem.Reader
	}
	return defaultMemoryReader
}

func (mem *InstanceMemory) ClickHouseWatermarkOrDefault() uint64 {
	if mem.ClickHouseWatermark != nil {
		return *mem.ClickHouseWatermark
	}
	return defaultMemoryClickHouseWatermark
}

func (mem *InstanceMemory) WatchdogOOMWatermarkOrDefault() uint64 {
	if mem.WatchdogOOMWatermark != nil {
		return *mem.WatchdogOOMWatermark
	}
	return defaultMemoryWatchdogOOMWatermark
}

func (mem *InstanceMemory) WatchdogOOMWindowWatermarkOrDefault() uint64 {
	if mem.WatchdogOOMWindowWatermark != nil {
		return *mem.WatchdogOOMWindowWatermark
	}
	return defaultMemoryWatchdogOOMWindowWatermark
}

func (mem *InstanceMemory) FootprintOrDefault() uint64 {
	if mem.Footprint != nil {
		return *mem.Footprint
	}
	return defaultMemoryFootprint
}

func (mem *InstanceMemory) maxServerMemoryUsage() uint64 {
	return mem.ClickHouseOrDefault() +
		mem.ChunkMetaCacheOrDefault() +
		mem.CompressedBlockCacheOrDefault() +
		mem.UncompressedBlockCacheOrDefault() +
		mem.ReaderOrDefault() +
		mem.FootprintOrDefault()
}

func (mem *InstanceMemory) ytServerClickHouseMemoryLimit() uint64 {
	return mem.maxServerMemoryUsage() + mem.ClickHouseWatermarkOrDefault()
}

func (mem *InstanceMemory) totalMemory() uint64 {
	return mem.ytServerClickHouseMemoryLimit()
}

func (mem *InstanceMemory) memoryConfig() map[string]uint64 {
	return map[string]uint64{
		"reader":                        mem.ReaderOrDefault(),
		"chunk_meta_cache":              mem.ChunkMetaCacheOrDefault(),
		"compressed_block_cache":        mem.CompressedBlockCacheOrDefault(),
		"uncompressed_block_cache":      mem.UncompressedBlockCacheOrDefault(),
		"memory_limit":                  mem.ytServerClickHouseMemoryLimit(),
		"max_server_memory_usage":       mem.maxServerMemoryUsage(),
		"watchdog_oom_watermark":        mem.WatchdogOOMWatermarkOrDefault(),
		"watchdog_oom_window_watermark": mem.WatchdogOOMWindowWatermarkOrDefault(),
	}
}

type Resources struct {
	// CliqueCPU and CliqueMemory are shorthands for those who wants
	// to throw some resources into clique and not think about actual
	// instance configuration.
	CliqueCPU    *uint64 `yson:"clique_cpu"`
	CliqueMemory *uint64 `yson:"clique_memory"`

	InstanceCount *uint64 `yson:"instance_count"`

	InstanceCPU *uint64 `yson:"instance_cpu"`

	// InstanceTotalMemory is a total instance memory; should not be less than
	// memNonElastic. If set, all additive memory parts are
	// scaled to fit into given total memory.
	InstanceTotalMemory *uint64 `yson:"instance_total_memory"`

	// InstanceMemory is the most detailed way to specify memory.
	InstanceMemory *InstanceMemory `yson:"instance_memory"`
}

func buildResources(instanceCount uint64, instanceCPU uint64, instanceMemory *InstanceMemory) *Resources {
	return &Resources{
		InstanceCount:       ptr.Uint64(instanceCount),
		InstanceMemory:      instanceMemory,
		InstanceCPU:         ptr.Uint64(instanceCPU),
		InstanceTotalMemory: ptr.Uint64(instanceMemory.totalMemory()),
		CliqueCPU:           ptr.Uint64(instanceCPU * instanceCount),
		CliqueMemory:        ptr.Uint64(instanceMemory.totalMemory() * instanceCount),
	}
}

func (c *Controller) populateResourcesClique(resources *Resources) error {
	c.l.Debug("populating resources in clique mode")
	if resources.InstanceCPU != nil || resources.InstanceTotalMemory != nil || resources.InstanceMemory != nil {
		return fmt.Errorf("chyt: total_{cpu,memory} should not be specified simultaneously with instance_{cpu,memory}")
	}
	if resources.InstanceCount != nil {
		return fmt.Errorf("chyt: total_{cpu,memory} should not be specified simultaneously with instance_count")
	}

	var instanceMemory InstanceMemory

	var instanceCount uint64 = maxInstanceCount

	if resources.CliqueCPU != nil {
		instanceCountCPU := *resources.CliqueCPU / defaultInstanceCPU
		if instanceCount > instanceCountCPU {
			instanceCount = instanceCountCPU
		}
	}

	if resources.CliqueMemory != nil {
		instanceCountMem := *resources.CliqueMemory / instanceMemory.totalMemory()
		if instanceCount > instanceCountMem {
			instanceCount = instanceCountMem
		}
	}

	if instanceCount == 0 {
		return fmt.Errorf("chyt: given total resource limits are not enough for running even one instance")
	}

	*resources = *buildResources(instanceCount, defaultInstanceCPU, &instanceMemory)

	return nil
}

func (c *Controller) populateResourcesInstance(resources *Resources) error {
	c.l.Debug("populating resources in instance mode")
	if resources.InstanceCount == nil {
		return fmt.Errorf("chyt: if total_{cpu,memory} is missing, instance_count should be present")
	}

	if resources.InstanceCPU == nil {
		resources.InstanceCPU = ptr.Uint64(defaultInstanceCPU)
	}

	if resources.InstanceTotalMemory != nil && resources.InstanceMemory != nil {
		return fmt.Errorf("chyt: instance_memory and instance_total_memory cannot be specified simultaneously")
	}

	var mem InstanceMemory

	if resources.InstanceMemory != nil {
		mem = *resources.InstanceMemory
	} else if resources.InstanceTotalMemory != nil {
		instanceTotalMemory := *resources.InstanceTotalMemory
		if instanceTotalMemory < memNonElastic {
			return fmt.Errorf("chyt: instance memory cannot be less than %v", memNonElastic)
		}

		// Transform InstanceTotalMemory into InstanceMemory.

		scale := float64(instanceTotalMemory-memNonElastic) / memElastic

		applyScale := func(value uint64) *uint64 {
			return ptr.Uint64(uint64(float64(value) * scale))
		}

		mem.ChunkMetaCache = applyScale(mem.ChunkMetaCacheOrDefault())
		mem.CompressedBlockCache = applyScale(mem.CompressedBlockCacheOrDefault())
		mem.UncompressedBlockCache = applyScale(mem.UncompressedBlockCacheOrDefault())
		mem.ClickHouse = applyScale(mem.ClickHouseOrDefault())
		mem.Reader = applyScale(mem.ReaderOrDefault())
	}
	*resources = *buildResources(*resources.InstanceCount, *resources.InstanceCPU, &mem)
	return nil
}

// TODO(dakovalkov): It's weird, because it changes the speclet. Overthink it.
func (c *Controller) populateResources(speclet *Speclet) (err error) {
	if speclet.CliqueCPU != nil || speclet.CliqueMemory != nil {
		err = c.populateResourcesClique(&speclet.Resources)
	} else if speclet.InstanceCPU != nil || speclet.InstanceTotalMemory != nil || speclet.InstanceMemory != nil || speclet.InstanceCount != nil {
		err = c.populateResourcesInstance(&speclet.Resources)
	} else {
		speclet.Resources = *buildResources(defaultInstanceCount, defaultInstanceCPU, &InstanceMemory{})
	}
	return
}

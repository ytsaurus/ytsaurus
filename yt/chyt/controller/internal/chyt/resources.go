package chyt

import (
	"fmt"

	"a.yandex-team.ru/library/go/ptr"
)

const (
	gib = 1024 * 1024 * 1024

	memClickHouse             = 16 * gib
	memUncompressedBlockCache = 16 * gib
	memReader                 = 12 * gib

	memElastic = memClickHouse + memUncompressedBlockCache + memReader

	memLogTailer = 2 * gib
	memFootprint = 10 * gib

	memClickHouseWatermark        = 10 * gib
	memWatchdogOOMWindowWatermark = 20 * gib
	memWatchdogOOMWatermark       = 4 * gib

	memNonElastic = memLogTailer + memFootprint + memClickHouseWatermark

	cpu = 16

	defaultInstanceCount = 8
	maxInstanceCount     = 100
)

type InstanceMemory struct {
	ClickHouse        *uint64 `yson:"clickhouse"`
	UncompressedCache *uint64 `yson:"uncompressed_cache"`
	Reader            *uint64 `yson:"reader"`
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
	// memFootprint + memLogTailer. If set, all additive memory parts are
	// scaled to fit into given total memory.
	InstanceTotalMemory *uint64 `yson:"instance_memory"`

	// InstanceMemory is the most detailed way to specify memory.
	InstanceMemory *InstanceMemory `yson:"instance_memory"`
}

func (r *InstanceMemory) maxServerMemoryUsage() uint64 {
	return *r.ClickHouse + *r.UncompressedCache + *r.Reader + memFootprint
}

func (r *InstanceMemory) ytServerClickHouseMemoryLimit() uint64 {
	return *r.ClickHouse + *r.UncompressedCache + *r.Reader + memFootprint + memClickHouseWatermark
}

func (r *InstanceMemory) totalMemory() uint64 {
	return *r.ClickHouse + *r.UncompressedCache + *r.Reader + memLogTailer + memFootprint + memClickHouseWatermark
}

func (r *InstanceMemory) memoryConfig() map[string]uint64 {
	return map[string]uint64{
		"reader":                        *r.Reader,
		"uncompressed_block_cache":      *r.UncompressedCache,
		"memory_limit":                  r.ytServerClickHouseMemoryLimit(),
		"max_server_memory_usage":       r.maxServerMemoryUsage(),
		"watchdog_oom_watermark":        memWatchdogOOMWatermark,
		"watchdog_oom_window_watermark": memWatchdogOOMWindowWatermark,
	}
}

var memDefault = &InstanceMemory{
	ClickHouse:        ptr.Uint64(memClickHouse),
	UncompressedCache: ptr.Uint64(memUncompressedBlockCache),
	Reader:            ptr.Uint64(memReader),
}

func buildResources(instanceCount uint64, instanceCPU uint64, memory *InstanceMemory) *Resources {
	instanceTotalMemory := *memory.UncompressedCache + *memory.ClickHouse + *memory.Reader + memFootprint + memLogTailer
	return &Resources{
		InstanceCount:       ptr.Uint64(instanceCount),
		InstanceMemory:      memory,
		InstanceCPU:         ptr.Uint64(instanceCPU),
		InstanceTotalMemory: ptr.Uint64(instanceTotalMemory),
		CliqueCPU:           ptr.Uint64(instanceCPU * instanceCount),
		CliqueMemory:        ptr.Uint64(instanceTotalMemory * instanceCount),
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

	var modelMemory uint64 = memClickHouse + memUncompressedBlockCache + memReader + memLogTailer + memFootprint + memClickHouseWatermark

	var instanceCount uint64 = maxInstanceCount

	if resources.CliqueCPU != nil {
		instanceCountCPU := *resources.CliqueCPU / cpu
		if instanceCount > instanceCountCPU {
			instanceCount = instanceCountCPU
		}
	}

	if resources.CliqueMemory != nil {
		instanceCountMem := *resources.InstanceTotalMemory / modelMemory
		if instanceCount > instanceCountMem {
			instanceCount = instanceCountMem
		}
	}

	if instanceCount == 0 {
		return fmt.Errorf("chyt: given total resource limits are not enough for running even one instance")
	}

	*resources = *buildResources(instanceCount, cpu, memDefault)

	return nil
}

func (c *Controller) populateResourcesInstance(resources *Resources) error {
	c.l.Debug("populating resources in instance mode")
	if resources.InstanceCount == nil {
		return fmt.Errorf("chyt: if total_{cpu,memory} is missing, instance_count should be present")
	}

	if resources.InstanceCPU == nil {
		resources.InstanceCPU = ptr.Uint64(cpu)
	}

	if resources.InstanceTotalMemory != nil && resources.InstanceMemory != nil {
		return fmt.Errorf("chyt: instance_memory and instance_total_memory cannot be specified simultaneously")
	}

	if resources.InstanceTotalMemory == nil && resources.InstanceMemory == nil {
		resources.InstanceTotalMemory = ptr.Uint64(memElastic + memNonElastic)
	}

	var mem InstanceMemory

	if resources.InstanceTotalMemory != nil {
		if *resources.InstanceTotalMemory < memNonElastic {
			return fmt.Errorf("chyt: instance memory cannot be less than %v", memNonElastic)
		}

		// Transform InstanceTotalMemory into InstanceMemory.
		scale := float64(*resources.InstanceTotalMemory-memNonElastic) / memElastic

		mem = *memDefault
		*mem.UncompressedCache = uint64(float64(*mem.UncompressedCache) * scale)
		*mem.ClickHouse = uint64(float64(*mem.ClickHouse) * scale)
		*mem.Reader = uint64(float64(*mem.Reader) * scale)
	} else {
		mem = *resources.InstanceMemory
		if mem.UncompressedCache == nil {
			mem.UncompressedCache = ptr.Uint64(memUncompressedBlockCache)
		}
		if mem.ClickHouse == nil {
			mem.ClickHouse = ptr.Uint64(memClickHouse)
		}
		if mem.Reader == nil {
			mem.Reader = ptr.Uint64(memReader)
		}
	}
	*resources = *buildResources(*resources.InstanceCount, *resources.InstanceCPU, &mem)
	return nil
}

func (c *Controller) populateResources(speclet *Speclet) (err error) {
	if speclet.Resources == nil {
		speclet.Resources = buildResources(defaultInstanceCount, cpu, memDefault)
	} else if speclet.Resources.CliqueCPU != nil || speclet.Resources.CliqueMemory != nil {
		err = c.populateResourcesClique(speclet.Resources)
	} else {
		err = c.populateResourcesInstance(speclet.Resources)
	}

	return
}

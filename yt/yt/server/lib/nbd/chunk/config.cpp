#include "config.h"

#include <yt/yt/ytlib/exec_node/public.h>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

void TPageCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("page_size", &TThis::PageSize)
        .Default(4_KB)
        .GreaterThan(0);
    registrar.Parameter("capacity", &TThis::Capacity)
        .Default(64_MB)
        .GreaterThan(0);
    registrar.Parameter("writeback_period", &TThis::WritebackPeriod)
        .Default(TDuration::Seconds(2));
    registrar.Parameter("dirty_data_soft_limit_capacity_fraction", &TThis::DirtyDataSoftLimitCapacityFraction)
        .Default(0.25)
        .GreaterThan(0.0)
        .LessThanOrEqual(1.0);
    registrar.Parameter("dirty_data_hard_limit_capacity_fraction", &TThis::DirtyDataHardLimitCapacityFraction)
        .Default(0.5)
        .GreaterThan(0.0)
        .LessThanOrEqual(1.0);
    registrar.Parameter("max_dirty_data_per_writeback_capacity_fraction", &TThis::MaxDirtyDataPerWritebackCapacityFraction)
        .Default(0.0625)
        .GreaterThan(0.0)
        .LessThanOrEqual(1.0);
    registrar.Parameter("max_dirty_data_per_write", &TThis::MaxDirtyDataPerWrite)
        .Default(1_MB)
        .GreaterThan(0);
    registrar.Parameter("max_inflight_write_requests", &TThis::MaxInflightWriteRequests)
        .Default(4)
        .GreaterThan(0);

    registrar.Postprocessor([] (TThis* config) {
        // PageSize must be a positive multiple of 4096.
        static constexpr i64 MinPageSize = 4_KB;
        if (config->PageSize % MinPageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"page_size\" must be a positive multiple of %v, got %v",
                MinPageSize,
                config->PageSize);
        }
        // Capacity must be a positive multiple of PageSize.
        if (config->Capacity % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"capacity\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->Capacity);
        }
        // MaxDirtyDataPerWrite must be a positive multiple of PageSize so a merged
        // subrequest always covers a whole number of pages.
        if (config->MaxDirtyDataPerWrite % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_write\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->MaxDirtyDataPerWrite);
        }
        // MaxDirtyDataPerWritebackCapacityFraction * Capacity is rounded down to a page
        // multiple in TPageCache, so the effective budget always maps to a whole number of pages.
        i64 maxDirtyDataPerWriteback = static_cast<i64>(config->MaxDirtyDataPerWritebackCapacityFraction * config->Capacity) / config->PageSize * config->PageSize;
        if (maxDirtyDataPerWriteback <= 0) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_writeback_capacity_fraction\" (%v) is too small for \"capacity\" (%v) and \"page_size\" (%v): effective budget is 0",
                config->MaxDirtyDataPerWritebackCapacityFraction,
                config->Capacity,
                config->PageSize);
        }
        // DirtyDataSoftLimitCapacityFraction must not exceed DirtyDataHardLimitCapacityFraction.
        if (config->DirtyDataSoftLimitCapacityFraction > config->DirtyDataHardLimitCapacityFraction) {
            THROW_ERROR_EXCEPTION(
                "\"dirty_data_soft_limit_capacity_fraction\" (%v) must not exceed \"dirty_data_hard_limit_capacity_fraction\" (%v)",
                config->DirtyDataSoftLimitCapacityFraction,
                config->DirtyDataHardLimitCapacityFraction);
        }
        // A single merged write must not exceed what one writeback processes: the merge cap
        // (MaxDirtyDataPerWrite) is applied within a writeback task that itself covers at most
        // maxDirtyDataPerWriteback bytes, so a larger per-write cap could never be reached and
        // would be misleading.
        if (config->MaxDirtyDataPerWrite > maxDirtyDataPerWriteback) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_write\" (%v) must not exceed the effective max dirty data per writeback (%v, from \"max_dirty_data_per_writeback_capacity_fraction\" %v of \"capacity\" %v)",
                config->MaxDirtyDataPerWrite,
                maxDirtyDataPerWriteback,
                config->MaxDirtyDataPerWritebackCapacityFraction,
                config->Capacity);
        }
        // MaxDirtyDataPerWriteback must not exceed Capacity. The fraction is already
        // constrained to (0, 1] by the parameter validator, so this is a structural
        // invariant, but we check the effective byte value explicitly for a clear
        // error message in case of rounding edge cases.
        if (maxDirtyDataPerWriteback > config->Capacity) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_writeback_capacity_fraction\" (%v) must not exceed \"capacity\" (%v)",
                config->MaxDirtyDataPerWritebackCapacityFraction,
                config->Capacity);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
    registrar.Parameter("medium_index", &TThis::MediumIndex)
        .Default(0);
    registrar.Parameter("fs_type", &TThis::FsType)
        .Default(EFilesystemType::Ext4);
    registrar.Parameter("keep_session_alive_period", &TThis::KeepSessionAlivePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("data_node_nbd_service_rpc_timeout", &TThis::DataNodeNbdServiceRpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("data_node_nbd_service_make_timeout", &TThis::DataNodeNbdServiceMakeTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("multiplexing_parallelism", &TThis::MultiplexingParallelism)
        .GreaterThanOrEqual(1)
        .Default(NExecNode::DefaultNbdMultiplexingParallelism);
    registrar.Parameter("page_cache", &TThis::PageCache)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        // When page cache is enabled, TChunkBlockDevice::Size must be a multiple
        // of the page size so every block-device offset maps to a whole page.
        if (config->PageCache && config->Size % config->PageCache->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"size\" (%v) must be a multiple of the page cache \"page_size\" (%v)",
                config->Size,
                config->PageCache->PageSize);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk

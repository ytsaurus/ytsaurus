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
    registrar.Parameter("dirty_data_soft_limit", &TThis::DirtyDataSoftLimit)
        .Default(16_MB)
        .GreaterThan(0);
    registrar.Parameter("dirty_data_hard_limit", &TThis::DirtyDataHardLimit)
        .Default(32_MB)
        .GreaterThan(0);
    registrar.Parameter("max_dirty_data_per_writeback", &TThis::MaxDirtyDataPerWriteback)
        .Default(4_MB)
        .GreaterThan(0);
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
        // MaxDirtyDataPerWriteback must be a positive multiple of PageSize so it maps to a
        // whole number of pages per writeback.
        if (config->MaxDirtyDataPerWriteback % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_writeback\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->MaxDirtyDataPerWriteback);
        }
        // DirtyDataSoftLimit must be a positive multiple of PageSize.
        if (config->DirtyDataSoftLimit % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"dirty_data_soft_limit\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->DirtyDataSoftLimit);
        }
        // DirtyDataHardLimit must be a positive multiple of PageSize.
        if (config->DirtyDataHardLimit % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"dirty_data_hard_limit\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->DirtyDataHardLimit);
        }
        // A single merged write must not exceed what one writeback processes: the merge cap
        // (MaxDirtyDataPerWrite) is applied within a writeback task that itself covers at most
        // MaxDirtyDataPerWriteback bytes, so a larger per-write cap could never be reached and
        // would be misleading.
        if (config->MaxDirtyDataPerWrite > config->MaxDirtyDataPerWriteback) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_write\" (%v) must not exceed \"max_dirty_data_per_writeback\" (%v)",
                config->MaxDirtyDataPerWrite,
                config->MaxDirtyDataPerWriteback);
        }
        // DirtyDataSoftLimit must not exceed DirtyDataHardLimit.
        if (config->DirtyDataSoftLimit > config->DirtyDataHardLimit) {
            THROW_ERROR_EXCEPTION(
                "\"dirty_data_soft_limit\" (%v) must not exceed \"dirty_data_hard_limit\" (%v)",
                config->DirtyDataSoftLimit,
                config->DirtyDataHardLimit);
        }
        // DirtyDataHardLimit must not exceed Capacity.
        if (config->DirtyDataHardLimit > config->Capacity) {
            THROW_ERROR_EXCEPTION(
                "\"dirty_data_hard_limit\" (%v) must not exceed \"capacity\" (%v)",
                config->DirtyDataHardLimit,
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

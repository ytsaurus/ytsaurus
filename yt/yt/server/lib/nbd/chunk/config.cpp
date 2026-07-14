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
        .GreaterThan(0);
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("max_dirty_data_per_flush", &TThis::MaxDirtyDataPerFlush)
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
        // MaxDirtyDataPerFlush must be a positive multiple of PageSize so it maps to a
        // whole number of pages per flush.
        if (config->MaxDirtyDataPerFlush % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_flush\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->MaxDirtyDataPerFlush);
        }
        // A single merged write must not exceed what one flush processes: the merge cap
        // (MaxDirtyDataPerWrite) is applied within a flush task that itself covers at most
        // MaxDirtyDataPerFlush bytes, so a larger per-write cap could never be reached and
        // would be misleading.
        if (config->MaxDirtyDataPerWrite > config->MaxDirtyDataPerFlush) {
            THROW_ERROR_EXCEPTION(
                "\"max_dirty_data_per_write\" (%v) must not exceed \"max_dirty_data_per_flush\" (%v)",
                config->MaxDirtyDataPerWrite,
                config->MaxDirtyDataPerFlush);
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

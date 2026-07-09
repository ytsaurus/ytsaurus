#include "config.h"

#include <yt/yt/ytlib/exec_node/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

void TBlockDeviceConfigBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TPageCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("page_size", &TThis::PageSize)
        .Default(4_KB)
        .GreaterThan(0);
    registrar.Parameter("size", &TThis::Size)
        .GreaterThan(0);
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Postprocessor([] (TThis* config) {
        // PageSize must be a positive multiple of 4096.
        static constexpr i64 MinPageSize = 4_KB;
        if (config->PageSize % MinPageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"page_size\" must be a positive multiple of %v, got %v",
                MinPageSize,
                config->PageSize);
        }
        // Size must be a positive multiple of PageSize.
        if (config->Size % config->PageSize != 0) {
            THROW_ERROR_EXCEPTION(
                "\"size\" must be a positive multiple of \"page_size\" (%v), got %v",
                config->PageSize,
                config->Size);
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

void TFileSystemBlockDeviceConfig::Register(TRegistrar)
{ }

////////////////////////////////////////////////////////////////////////////////

void TMemoryBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
    registrar.Parameter("block_size", &TThis::BlockSize)
        .GreaterThanOrEqual(0);
    registrar.Parameter("read_batch_size", &TThis::ReadBatchSize)
        .Default(16).GreaterThan(0);
    registrar.Parameter("write_batch_size", &TThis::WriteBatchSize)
        .Default(16).GreaterThan(0);
    registrar.Parameter("table_path", &TThis::TablePath);
}

////////////////////////////////////////////////////////////////////////////////

void TIdsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(10809);
    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(1'000);
}

////////////////////////////////////////////////////////////////////////////////

void TUdsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("/tmp/nbd.sock");
    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(1'000);
}

////////////////////////////////////////////////////////////////////////////////

void TNbdTestOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("sleep_on_read", &TThis::SleepOnRead)
        .Default();
    registrar.Parameter("sleep_on_write", &TThis::SleepOnWrite)
        .Default();
    registrar.Parameter("set_error_on_read", &TThis::SetErrorOnRead)
        .Default();
    registrar.Parameter("set_error_on_write", &TThis::SetErrorOnWrite)
        .Default();
    registrar.Parameter("abort_connection_on_read", &TThis::AbortConnectionOnRead)
        .Default();
    registrar.Parameter("abort_connection_on_write", &TThis::AbortConnectionOnWrite)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TNbdServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("internet_domain_socket", &TThis::InternetDomainSocket)
        .Default();
    registrar.Parameter("unix_domain_socket", &TThis::UnixDomainSocket)
        .Default();
    registrar.Parameter("thread_count", &TThis::ThreadCount)
        .Default(1);
    registrar.Parameter("test_options", &TThis::TestOptions)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->InternetDomainSocket && config->UnixDomainSocket) {
            THROW_ERROR_EXCEPTION("\"internet_domain_socket\" and \"unix_domain_socket\" cannot be both present");
        }

        if (!config->InternetDomainSocket && !config->UnixDomainSocket) {
            THROW_ERROR_EXCEPTION("\"internet_domain_socket\" and \"unix_domain_socket\" cannot be both missing");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

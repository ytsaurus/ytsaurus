#include "config.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

void TCypressFileBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("test_sleep_before_read", &TThis::TestSleepBeforeRead)
        .Default(TDuration::Zero());
}

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

void TNbdServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("internet_domain_socket", &TThis::InternetDomainSocket)
        .Default();
    registrar.Parameter("unix_domain_socket", &TThis::UnixDomainSocket)
        .Default();
    registrar.Parameter("test_block_device_sleep_before_read", &TThis::TestBlockDeviceSleepBeforeRead)
        .Default();
    registrar.Parameter("test_abort_connection_on_read", &TThis::TestAbortConnectionOnRead)
        .Default();

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

#include "config.h"

#include <library/cpp/yt/string/format.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

void TBlockDeviceConfigBase::Register(TRegistrar /*registrar*/)
{ }

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
    registrar.Parameter("http_port", &TThis::HttpPort)
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

std::string TNbdServerConfig::GetAddress() const
{
    // Exactly one of the sockets is set (see the postprocessor above).
    return UnixDomainSocket
        ? UnixDomainSocket->Path
        : Format(":%v", InternetDomainSocket->Port);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

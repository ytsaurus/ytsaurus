#include "config.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

void TCypressFileBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

////////////////////////////////////////////////////////////////////////////////

void TMemoryBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TIdsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(10809);
    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

void TUdsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TNbdServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("internet_domain_socket", &TThis::InternetDomainSocket)
        .Default();
    registrar.Parameter("unix_domain_socket", &TThis::UnixDomainSocket)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

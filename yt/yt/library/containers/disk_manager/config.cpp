#include "config.h"

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

void TDiskManagerProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_manager_address", &TThis::DiskManagerAddress)
        .Default("unix:/var/run/diskman.sock");
    registrar.Parameter("disk_manager_service_name", &TThis::DiskManagerServiceName)
        .Default("diskman.DiskManager");

    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskManagerProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

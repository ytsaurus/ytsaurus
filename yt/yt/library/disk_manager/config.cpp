#include "config.h"

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

void TDiskInfoProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_ids", &TThis::DiskIds)
        .Default();
    registrar.Parameter("yt_disk_prefix", &TThis::YtDiskPrefix)
        .Default("/yt");
}

////////////////////////////////////////////////////////////////////////////////

void TDiskManagerProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_manager_address", &TThis::DiskManagerAddress)
        .Default("unix:/run/yandex-diskmanager/yandex-diskmanager.sock");
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

} // namespace NYT::NDiskManager

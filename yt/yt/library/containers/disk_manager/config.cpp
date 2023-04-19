#include "config.h"

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

void TMockedDiskConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_id", &TThis::DiskId)
        .Default();
    registrar.Parameter("device_path", &TThis::DevicePath)
        .Default();
    registrar.Parameter("device_name", &TThis::DeviceName)
        .Default();
    registrar.Parameter("disk_model", &TThis::DiskModel)
        .Default();
    registrar.Parameter("partition_fs_labels", &TThis::PartitionFsLabels)
        .Default();
    registrar.Parameter("state", &TThis::State)
        .Default(EDiskState::Ok);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskManagerProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_manager_address", &TThis::DiskManagerAddress)
        .Default("unix:/var/run/diskman.sock");
    registrar.Parameter("disk_manager_service_name", &TThis::DiskManagerServiceName)
        .Default("diskman.DiskManager");

    registrar.Parameter("is_mock", &TThis::IsMock)
        .Default(false);
    registrar.Parameter("mock_disks", &TThis::MockDisks)
        .Default();
    registrar.Parameter("mock_yt_paths", &TThis::MockYtPaths)
        .Default();

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

void TActiveDiskCheckerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

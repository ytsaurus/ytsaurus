#include "disk_manager_proxy.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NContainers {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

EDiskState MapDiskState(diskman::DiskStatus_State state)
{
    switch (state) {
        case diskman::DiskStatus_State::DiskStatus_State_OK:
            return EDiskState::Ok;
        case diskman::DiskStatus_State::DiskStatus_State_FAILED:
            return EDiskState::Failed;
        case diskman::DiskStatus_State::DiskStatus_State_RECOVER_WAIT:
            return EDiskState::RecoverWait;
        default:
            return EDiskState::Unknown;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDiskManagerProxy::TDiskManagerProxy(
    NRpc::IChannelPtr channel,
    TString serviceName,
    TDiskManagerProxyConfigPtr config)
    : TProxyBase(
        std::move(channel),
        NRpc::TServiceDescriptor(std::move(serviceName)))
    , Config_(std::move(config))
    , DynamicConfig_(New<TDiskManagerProxyDynamicConfig>())
{ }

TFuture<THashSet<TString>> TDiskManagerProxy::GetYtDiskDeviceNames()
{
    auto request = GetYTMountedDevices();
    auto responseFuture = request->Invoke()
        .WithTimeout(GetRequestTimeout());

    return responseFuture.Apply(BIND([] (const TErrorOr<TRspGetYTMountedDevicesPtr>& responseOrError) {
        if (!responseOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to fetch disk names from disk manager")
                << responseOrError;
        }

        auto& response = responseOrError.Value();

        THashSet<TString> paths;
        paths.reserve(response->mounted_devices().size());

        for (const auto& device : response->mounted_devices()) {
            paths.insert(NFS::GetFileName(device.device_path()));
        }

        return paths;
    }));
}

TFuture<std::vector<TDiskInfo>> TDiskManagerProxy::GetDisks()
{
    auto request = ListDisks();
    auto responseFuture = request->Invoke()
        .WithTimeout(GetRequestTimeout());

    return responseFuture.Apply(BIND([] (const TErrorOr<TRspListDisksPtr>& responseOrError) {
        if (!responseOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to fetch disks info from disk manager")
                << responseOrError;
        }

        auto& response = responseOrError.Value();
        std::vector<TDiskInfo> disks;
        disks.reserve(response->disks().size());

        for (const auto& device : response->disks()) {
            disks.emplace_back(
                TDiskInfo{
                    .DiskId = device.meta().id(),
                    .DevicePath = device.spec().device_path(),
                    .DeviceName = NFS::GetFileName(device.spec().device_path()),
                    .DiskModel = device.spec().model(),
                    .State = MapDiskState(device.status().hw_state())
                }
            );
        }

        return disks;
    }));
}

TDuration TDiskManagerProxy::GetRequestTimeout() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig->RequestTimeout.value_or(Config_->RequestTimeout);
}

void TDiskManagerProxy::OnDynamicConfigChanged(
    const TDiskManagerProxyDynamicConfigPtr& newConfig)
{
    DynamicConfig_.Store(newConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

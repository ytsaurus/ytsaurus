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

static EDiskState MapDiskState(diskman::DiskStatus_State state)
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
    NYT::NRpc::IChannelPtr channel,
    TString serviceName,
    TDiskManagerProxyConfigPtr config)
    : TProxyBase(std::move(channel)
    , NYT::NRpc::TServiceDescriptor(std::move(serviceName)))
    , Config_(config)
{ }

TFuture<std::vector<TString>> TDiskManagerProxy::GetYTDiskDeviceNames()
{
    auto responseFuture = ExecuteApiCall<TReqGetYTMountedDevices, TRspGetYTMountedDevices>(
        &TDiskManagerProxy::GetYTMountedDevices,
        [] (auto /*request*/) {}
    );

    return responseFuture.Apply(BIND([] (const TRspGetYTMountedDevicesPtr response) {
        std::vector<TString> paths(response->mounted_devices().size());

        for (const auto& device : response->mounted_devices()) {
            paths.emplace_back(NFS::GetFileName(TString(device.device_path())));
        }

        return paths;
    }));
}

TFuture<std::vector<TDiskInfo>> TDiskManagerProxy::GetDisks()
{
    auto responseFuture = ExecuteApiCall<TReqListDisks, TRspListDisks>(
        &TDiskManagerProxy::ListDisks,
        [] (auto /*request*/) {}
    );

    return responseFuture.Apply(BIND([] (const TRspListDisksPtr response) {
        std::vector<TDiskInfo> disks(response->disks().size());

        for (const auto& device : response->disks()) {
            disks.emplace_back(
                TDiskInfo{
                    .DiskId = device.meta().id(),
                    .DevicePath = device.spec().device_path(),
                    .DeviceName = NFS::GetFileName(TString(device.spec().device_path())),
                    .DiskModel = device.spec().model(),
                    .State = MapDiskState(device.status().hw_state())
                }
            );
        }

        return disks;
    }));
}

template <class Request, class Response>
TFuture<TIntrusivePtr<Response>> TDiskManagerProxy::ExecuteApiCall(
    TIntrusivePtr<Request> (TDiskManagerProxy::*callMethod)(),
    std::function<void(TIntrusivePtr<Request>)> enrichRequest)
{
    try {
        auto req = (this->*callMethod)();
        enrichRequest(req);
        return req->Invoke()
            .WithTimeout(GetHealthCheckTimeout());
    } catch(const std::exception& exception) {
        THROW_ERROR_EXCEPTION("Failed to execute request to Disk Manager")
            << exception;
    }
}

TDuration TDiskManagerProxy::GetHealthCheckTimeout() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->HealthCheckTimeout.value_or(Config_->HealthCheckTimeout)
        : Config_->HealthCheckTimeout;
}

void TDiskManagerProxy::OnDynamicConfigChanged(
    const TDiskManagerProxyDynamicConfigPtr& newNodeConfig)
{
    DynamicConfig_.Store(newNodeConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

#pragma once

#include "public.h"

#include <yt/yt/library/containers/disk_manager/config.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/rpc/client.h>

#include <infra/diskmanager/proto/diskman.pb.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

using TReqGetYTMountedDevices = diskman::GetYTMountedDevicesRequest;
using TRspGetYTMountedDevices = diskman::GetYTMountedDevicesResponse;

using TReqListDisks = diskman::ListDisksRequest;
using TRspListDisks = diskman::ListDisksResponse;

////////////////////////////////////////////////////////////////////////////////

class TDiskManagerProxy
    : public NYT::NRpc::TProxyBase
    , public TRefCounted
{
public:
    TDiskManagerProxy(
        NYT::NRpc::IChannelPtr channel,
        TString serviceName,
        TDiskManagerProxyConfigPtr config);

    TFuture<std::vector<TString>> GetYTDiskDeviceNames();
    TFuture<std::vector<TDiskInfo>> GetDisks();

    void OnDynamicConfigChanged(const TDiskManagerProxyDynamicConfigPtr& newNodeConfig);

private:
    const TDiskManagerProxyConfigPtr Config_;
    TAtomicObject<TDiskManagerProxyDynamicConfigPtr> DynamicConfig_;

    DEFINE_RPC_PROXY_METHOD(NContainers, GetYTMountedDevices);
    DEFINE_RPC_PROXY_METHOD(NContainers, ListDisks);

    template <class Request, class Response>
    TFuture<TIntrusivePtr<Response>> ExecuteApiCall(
        TIntrusivePtr<Request> (TDiskManagerProxy::*callMethod)(),
        std::function<void(TIntrusivePtr<Request>)> enrichRequest);

    TDuration GetHealthCheckTimeout() const;
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

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

using TReqRecoverDisk = diskman::RecoverDiskRequest;
using TRspRecoverDisk = diskman::RecoverDiskResponse;

class TDiskManagerApi
    : public NRpc::TProxyBase
{
public:
    explicit TDiskManagerApi(NRpc::IChannelPtr channel, TString serviceName);

    DEFINE_RPC_PROXY_METHOD(NContainers, GetYTMountedDevices);
    DEFINE_RPC_PROXY_METHOD(NContainers, ListDisks);
    DEFINE_RPC_PROXY_METHOD(NContainers, RecoverDisk);
};

////////////////////////////////////////////////////////////////////////////////

class TDiskManagerProxy
    : public TRefCounted
{
public:
    explicit TDiskManagerProxy(TDiskManagerProxyConfigPtr config);

    TFuture<THashSet<TString>> GetYtDiskDeviceNames();
    TFuture<std::vector<TDiskInfo>> GetDisks();
    TFuture<void> RecoverDiskById(TString diskId, ERecoverPolicy recoverPolicy);

    void OnDynamicConfigChanged(const TDiskManagerProxyDynamicConfigPtr& newConfig);

private:
    const NRpc::IChannelPtr Channel_;
    const TString ServiceName_;

    const TDiskManagerProxyConfigPtr Config_;
    TAtomicObject<TDiskManagerProxyDynamicConfigPtr> DynamicConfig_;
    TDuration GetRequestTimeout() const;
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

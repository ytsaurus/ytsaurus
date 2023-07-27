#include "disk_manager_proxy.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <infra/diskmanager/proto/diskman.pb.h>

namespace NYT::NContainers {

using namespace NConcurrency;

using namespace NRpc;
using namespace NRpc::NGrpc;

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateDiskManagerRpcChannel(TString diskManagerAddress)
{
    auto channelConfig = New<TChannelConfig>();
    channelConfig->Address = std::move(diskManagerAddress);
    return CreateGrpcChannel(std::move(channelConfig));
}

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

diskman::DiskSpec::RecoverPolicy MapRecoverPolicy(ERecoverPolicy recoveryPolicy)
{
    switch (recoveryPolicy) {
        case ERecoverPolicy::RecoverMount:
            return diskman::DiskSpec::RecoverPolicy::DiskSpec_RecoverPolicy_RECOVER_MOUNT;
        case ERecoverPolicy::RecoverLayout:
            return diskman::DiskSpec::RecoverPolicy::DiskSpec_RecoverPolicy_RECOVER_LAYOUT;
        case ERecoverPolicy::RecoverDisk:
            return diskman::DiskSpec::RecoverPolicy::DiskSpec_RecoverPolicy_RECOVER_DISK;
        default:
            return diskman::DiskSpec::RecoverPolicy::DiskSpec_RecoverPolicy_RECOVER_AUTO;
    }
}

////////////////////////////////////////////////////////////////////////////////

using TReqGetYTMountedDevices = diskman::GetYTMountedDevicesRequest;
using TRspGetYTMountedDevices = diskman::GetYTMountedDevicesResponse;

using TReqListDisks = diskman::ListDisksRequest;
using TRspListDisks = diskman::ListDisksResponse;

using TReqRecoverDisk = diskman::RecoverDiskRequest;
using TRspRecoverDisk = diskman::RecoverDiskResponse;

using TReqFailDisk = diskman::FailDiskRequest;
using TRspFailDisk = diskman::FailDiskResponse;

class TDiskManagerApi
    : public NRpc::TProxyBase
{
public:
    TDiskManagerApi(IChannelPtr channel, TString serviceName)
        : TProxyBase(
            std::move(channel),
            TServiceDescriptor(std::move(serviceName)))
    { }

    DEFINE_RPC_PROXY_METHOD(NContainers, GetYTMountedDevices);
    DEFINE_RPC_PROXY_METHOD(NContainers, ListDisks);
    DEFINE_RPC_PROXY_METHOD(NContainers, RecoverDisk);
    DEFINE_RPC_PROXY_METHOD(NContainers, FailDisk);
};

////////////////////////////////////////////////////////////////////////////////

class TDiskManagerProxy
    : public IDiskManagerProxy
{
public:
    TDiskManagerProxy(TDiskManagerProxyConfigPtr config)
        : Channel_(CreateDiskManagerRpcChannel(config->DiskManagerAddress))
        , ServiceName_(config->DiskManagerServiceName)
        , Config_(std::move(config))
        , DynamicConfig_(New<TDiskManagerProxyDynamicConfig>())
    { }

    TFuture<THashSet<TString>> GetYtDiskMountPaths() override
    {
        // fast path for tests
        if (Config_->IsMock) {
            THashSet<TString> paths;

            for (const auto& path : Config_->MockYtPaths) {
                paths.insert(path);
            }

            return MakeFuture(std::move(paths));
        }

        TDiskManagerApi api(Channel_, ServiceName_);
        auto request = api.GetYTMountedDevices();
        request->SetTimeout(GetRequestTimeout());
        auto responseFuture = request->Invoke();

        return responseFuture.Apply(BIND([] (const TErrorOr<TDiskManagerApi::TRspGetYTMountedDevicesPtr>& responseOrError) {
            if (!responseOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to fetch disk names from disk manager")
                    << responseOrError;
            }

            auto& response = responseOrError.Value();

            THashSet<TString> paths;
            paths.reserve(response->mounted_devices().size());

            for (const auto& device : response->mounted_devices()) {
                paths.insert(device.mount_path());
            }

            return paths;
        }));
    }

    TFuture<std::vector<TDiskInfo>> GetDisks() override
    {
        // fast path for tests
        if (Config_->IsMock) {
            std::vector<TDiskInfo> disks;

            for (const auto& disk : Config_->MockDisks) {
                THashSet<TString> labels;

                for (const auto& label : disk->PartitionFsLabels) {
                    labels.insert(label);
                }

                disks.emplace_back(TDiskInfo{
                    .DiskId = disk->DiskId,
                    .DevicePath = disk->DevicePath,
                    .DeviceName = disk->DeviceName,
                    .DiskModel = disk->DiskModel,
                    .PartitionFsLabels = labels,
                    .State = disk->State
                });
            }

            return MakeFuture(disks);
        }

        TDiskManagerApi api(Channel_, ServiceName_);
        auto request = api.ListDisks();
        request->SetTimeout(GetRequestTimeout());
        auto responseFuture = request->Invoke();

        return responseFuture.Apply(BIND([] (const TErrorOr<TDiskManagerApi::TRspListDisksPtr>& responseOrError) {
            if (!responseOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to fetch disks info from disk manager")
                    << responseOrError;
            }

            auto& response = responseOrError.Value();
            std::vector<TDiskInfo> disks;
            disks.reserve(response->disks().size());

            for (const auto& device : response->disks()) {
                THashSet<TString> partitionFsLabels;

                for (const auto& partition : device.spec().partitions()) {
                    partitionFsLabels.insert(partition.fs_label());
                }

                disks.emplace_back(
                    TDiskInfo{
                        .DiskId = device.meta().id(),
                        .DevicePath = device.spec().device_path(),
                        .DeviceName = NFS::GetFileName(device.spec().device_path()),
                        .DiskModel = device.spec().model(),
                        .PartitionFsLabels = std::move(partitionFsLabels),
                        .State = MapDiskState(device.status().hw_state())
                    }
                );
            }

            return disks;
        }));
    }

    TFuture<void> RecoverDiskById(
        const TString& diskId,
        ERecoverPolicy recoverPolicy) override
    {
        // fast path for tests
        if (Config_->IsMock) {
            return MakeFuture(TError());
        }

        TDiskManagerApi api(Channel_, ServiceName_);
        auto request = api.RecoverDisk();
        request->set_disk_id(diskId);
        request->set_policy(MapRecoverPolicy(recoverPolicy));
        request->SetTimeout(GetRequestTimeout());

        auto responseFuture = request->Invoke();

        return responseFuture.Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TDiskManagerApi::TRspRecoverDiskPtr>& responseOrError) {
            if (!responseOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to send request to recover disk")
                    << responseOrError
                    << TErrorAttribute("disk_id", diskId)
                    << TErrorAttribute("recover_policy", recoverPolicy);
            }
        }));
    }

    TFuture<void> FailDiskById(
        const TString& diskId,
        const TString& reason) override
    {
        // fast path for tests
        if (Config_->IsMock) {
            return MakeFuture(TError());
        }

        TDiskManagerApi api(Channel_, ServiceName_);
        auto request = api.FailDisk();
        request->set_disk_id(diskId);
        request->set_reason(reason);
        request->SetTimeout(GetRequestTimeout());

        auto responseFuture = request->Invoke();

        return responseFuture.Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TDiskManagerApi::TRspFailDiskPtr>& responseOrError) {
            if (!responseOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to send request to fail disk")
                    << responseOrError
                    << TErrorAttribute("disk_id", diskId)
                    << TErrorAttribute("reason", reason);
            }
        }));
    }

    TDuration GetRequestTimeout() const
    {
        auto dynamicConfig = DynamicConfig_.Load();
        return dynamicConfig->RequestTimeout.value_or(Config_->RequestTimeout);
    }

    void OnDynamicConfigChanged(
        const TDiskManagerProxyDynamicConfigPtr& newConfig) override
    {
        DynamicConfig_.Store(newConfig);
    }

private:
    const NRpc::IChannelPtr Channel_;
    const TString ServiceName_;

    const TDiskManagerProxyConfigPtr Config_;
    TAtomicObject<TDiskManagerProxyDynamicConfigPtr> DynamicConfig_;

};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxy)

////////////////////////////////////////////////////////////////////////////////

IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr config)
{
    return New<TDiskManagerProxy>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

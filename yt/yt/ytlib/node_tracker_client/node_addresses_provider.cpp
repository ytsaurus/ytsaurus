#include "node_addresses_provider.h"

#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/node_tracker_client/private.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

namespace NYT::NNodeTrackerClient {

using namespace NApi;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NCellMasterClient;
using namespace NObjectClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> GetAddresses(ENodeRole nodeRole, const TMasterYPathProxy::TRspGetClusterMetaPtr& rsp)
{
    switch (nodeRole) {
        case ENodeRole::MasterCache:
            return FromProto<std::vector<TString>>(rsp->master_cache_node_addresses());
        case ENodeRole::TimestampProvider:
            return FromProto<std::vector<TString>>(rsp->timestamp_provider_node_addresses());
        default:
            YT_ABORT();
    }
}

EMasterChannelKind GetChannelKind(ENodeRole nodeRole)
{
    switch (nodeRole) {
        case ENodeRole::MasterCache:
            return EMasterChannelKind::MasterCache;
        case ENodeRole::TimestampProvider:
            return EMasterChannelKind::Cache;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNodeAddressesProvider
    : public IRoamingChannelProvider
{
public:
    TNodeAddressesProvider(
        TDuration syncPeriod,
        TDuration syncPeriodSplay,
        TWeakPtr<ICellDirectory> cellDirectory,
        ENodeRole nodeRole,
        TCallback<IChannelPtr(const std::vector<TString>&)> channelBuilder)
        : CellDirectory_(std::move(cellDirectory))
        , NodeRole_(nodeRole)
        , ChannelBuilder_(std::move(channelBuilder))
        , SyncPeriod_(syncPeriod)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TNodeAddressesProvider::OnSync, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = syncPeriod,
                .Splay = syncPeriodSplay
            }))
        , Logger(NodeTrackerClientLogger.WithTag("NodeRole: %v", NodeRole_))
        , NullChannel_(ChannelBuilder_({}))
    { }

    const TString& GetEndpointDescription() const override
    {
        return TryGetChannel()->GetEndpointDescription();
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return TryGetChannel()->GetEndpointAttributes();
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        EnsureStarted();
        auto guard = Guard(Lock_);
        if (!TerminationError_.IsOK()) {
            return MakeFuture<IChannelPtr>(TError(
                NRpc::EErrorCode::TransportError,
                "Channel terminated")
                << NullChannel_->GetEndpointAttributes()
                << TerminationError_);
        }
        return ChannelPromise_;
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    void Terminate(const TError& error) override
    {
        {
            auto guard = Guard(Lock_);
            if (TerminationError_.IsOK()) {
                TerminationError_ = error;
                if (ChannelPromise_.IsSet()) {
                    const auto& channelOrError = ChannelPromise_.Get();
                    if (channelOrError.IsOK()) {
                        const auto& channel = channelOrError.Value();
                        channel->Terminate(error);
                    }
                }
            }
        }
        YT_UNUSED_FUTURE(SyncExecutor_->Stop());
    }

private:
    const TWeakPtr<ICellDirectory> CellDirectory_;
    const ENodeRole NodeRole_;
    const TCallback<IChannelPtr(const std::vector<TString>&)> ChannelBuilder_;
    const TDuration SyncPeriod_;
    const TPeriodicExecutorPtr SyncExecutor_;
    const NYT::NLogging::TLogger Logger;

    const IChannelPtr NullChannel_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    mutable bool Started_ = false;
    TPromise<IChannelPtr> ChannelPromise_ = NewPromise<IChannelPtr>();
    TError TerminationError_;

    std::optional<std::vector<TString>> Addresses_;

    void EnsureStarted() const
    {
        {
            auto guard = Guard(Lock_);
            if (!TerminationError_.IsOK() || Started_) {
                return;
            }
            Started_ = true;
        }
        SyncExecutor_->Start();
        // YTADMINREQ-24943
        // Executor is configured with a splay; this can cause the very first sync
        // to be delayed for a long period of time and cause timeout on first access
        // to the channel.
        SyncExecutor_->ScheduleOutOfBand();
    }

    IChannelPtr TryGetChannel() const
    {
        EnsureStarted();
        auto guard = Guard(Lock_);
        if (!ChannelPromise_.IsSet()) {
            return NullChannel_;
        }
        const auto& channelOrError = ChannelPromise_.Get();
        if (!channelOrError.IsOK()) {
            return NullChannel_;
        }
        return channelOrError.Value();
    }

    void SetChannel(IChannelPtr channel)
    {
        TPromise<IChannelPtr> promise;
        {
            auto guard = Guard(Lock_);
            if (!TerminationError_.IsOK()) {
                guard.Release();
                channel->Terminate(TerminationError_);
                return;
            }
            if (ChannelPromise_.IsSet()) {
                ChannelPromise_ = NewPromise<IChannelPtr>();
            }
            promise = ChannelPromise_;
        }
        promise.Set(std::move(channel));
    }

    void OnSync()
    {
        try {
            auto cellDirectory = CellDirectory_.Lock();
            if (!cellDirectory) {
                return;
            }

            YT_LOG_DEBUG("Started updating node list");

            auto channel = cellDirectory->GetMasterChannelOrThrow(GetChannelKind(NodeRole_), cellDirectory->GetPrimaryMasterCellId());
            auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(channel));
            auto req = TMasterYPathProxy::GetClusterMeta();
            switch (NodeRole_) {
                case ENodeRole::MasterCache: {
                    req->set_populate_master_cache_node_addresses(true);
                    break;
                }
                case ENodeRole::TimestampProvider: {
                    req->set_populate_timestamp_provider_node_addresses(true);
                    break;
                }
                default:
                    YT_ABORT();
            }

            // TODO(aleksandra-zh): think of a better way of annotating req and batchReq.
            TGetClusterMetaOptions options;
            auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
            cachingHeaderExt->set_expire_after_successful_update_time(ToProto<i64>(
                std::min(options.ExpireAfterSuccessfulUpdateTime, SyncPeriod_)));
            cachingHeaderExt->set_expire_after_failed_update_time(ToProto<i64>(
                std::min(options.ExpireAfterFailedUpdateTime, SyncPeriod_)));

            auto batchReq = proxy.ExecuteBatch();
            batchReq->SetSuppressTransactionCoordinatorSync(true);
            auto* balancingHeaderExt = batchReq->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
            balancingHeaderExt->set_enable_stickiness(true);
            batchReq->AddRequest(req);

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();
            auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspGetClusterMeta>(0)
                .ValueOrThrow();
            auto newAddresses = GetAddresses(NodeRole_, rsp);

            YT_LOG_DEBUG("Received node list (Addresses: %v)", newAddresses);

            if (Addresses_ == newAddresses) {
                YT_LOG_DEBUG("Node list has not changed");
                return;
            }

            YT_LOG_INFO("Node list has been changed (OldAddresses: %v, NewAddresses: %v)",
                Addresses_,
                newAddresses);

            Addresses_ = std::move(newAddresses);
            SetChannel(ChannelBuilder_(*Addresses_));

            YT_LOG_DEBUG("Finished updating node list");
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed updating node list");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateNodeAddressesChannel(
    TDuration syncPeriod,
    TDuration syncPeriodSplay,
    TWeakPtr<ICellDirectory> cellDirectory,
    ENodeRole nodeRole,
    TCallback<IChannelPtr(const std::vector<TString>&)> channelBuilder)
{
    return CreateRoamingChannel(New<TNodeAddressesProvider>(
        syncPeriod,
        syncPeriodSplay,
        std::move(cellDirectory),
        nodeRole,
        std::move(channelBuilder)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

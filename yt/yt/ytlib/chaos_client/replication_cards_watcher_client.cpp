#include "replication_cards_watcher_client.h"
#include "chaos_node_service_proxy.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>
#include <yt/yt/ytlib/chaos_client/master_cache_channel.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosClient {

using namespace NHydra;
using namespace NRpc;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NApi::NNative;
using namespace NLogging;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ReplicationCardWatcherClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardsWatcherClient
    : public IReplicationCardsWatcherClient
{
public:
    TReplicationCardsWatcherClient(
        std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
        TWeakPtr<NApi::NNative::IConnection> connection)
        : Connection_(std::move(connection))
        , Callbacks_(std::move(callbacks))
    { }

    TReplicationCardsWatcherClient(
        std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
        IChannelPtr chaosCacheChannel,
        TWeakPtr<NApi::NNative::IConnection> connection)
        : Connection_(std::move(connection))
        , ChaosCacheChannel_(std::move(chaosCacheChannel))
        , Callbacks_(std::move(callbacks))
    { }

    void WatchReplicationCard(TReplicationCardId replicationCardId) override
    {
        auto guard = Guard(Lock_);
        auto& [future, timestamp] = WatchingFutures_[replicationCardId];
        if (future) {
            return;
        }

        future = WatchUpstream(replicationCardId, timestamp);
    }

    void StopWatchingReplicationCard(TReplicationCardId replicationCardId) override
    {
        TFuture<void> localFuture;
        {
            auto guard = Guard(Lock_);
            auto it = WatchingFutures_.find(replicationCardId);
            if (it == WatchingFutures_.end()) {
                return;
            }
            localFuture = std::move(it->second.first);
            WatchingFutures_.erase(it);
        }

        localFuture.Cancel(TError("Stopped watching"));
    }

private:
    const TWeakPtr<IConnection> Connection_;
    const IChannelPtr ChaosCacheChannel_;

    std::unique_ptr<IReplicationCardWatcherClientCallbacks> Callbacks_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TReplicationCardId, std::pair<TFuture<void>, TTimestamp>> WatchingFutures_;

    TFuture<void> WatchUpstream(TReplicationCardId replicationCardId, TTimestamp timestamp)
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return MakeFuture(TError("Connection is not available"));
        }

        auto channel = ChaosCacheChannel_;
        if (!channel) {
            channel = connection->GetChaosChannelByCardId(replicationCardId, EPeerKind::Leader);
        }

        auto proxy = TChaosNodeServiceProxy(std::move(channel));
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosWatcherClientRequestTimeout);

        auto req = proxy.WatchReplicationCard();
        ToProto(req->mutable_replication_card_id(), replicationCardId);
        req->set_replication_card_cache_timestamp(timestamp);

        SetChaosCacheStickyGroupBalancingHint(
            replicationCardId,
            req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));

        return req->Invoke().ApplyUnique(
            BIND(
                &TReplicationCardsWatcherClient::OnReplicationCardWatchResponse,
                MakeStrong(this),
                replicationCardId)
            .AsyncVia(GetCurrentInvoker()));
    }

    void OnReplicationCardWatchResponse(
        TReplicationCardId replicationCardId,
        TErrorOr<TChaosNodeServiceProxy::TRspWatchReplicationCardPtr>&& response)
    {
        if (!response.IsOK()) {
            auto guard = Guard(Lock_);
            WatchingFutures_.erase(replicationCardId);
            YT_LOG_DEBUG(response, "Error watching replication card");
            return;
        }

        const auto& value = response.Value();
        auto guard = Guard(Lock_);
        auto& [future, timestamp] = WatchingFutures_[replicationCardId];
        auto localFuture = std::move(future);
        if (value->has_replication_card_deleted()) {
            WatchingFutures_.erase(replicationCardId);
            guard.Release();
            Callbacks_->OnReplicationCardDeleted(replicationCardId);
            return;
        }

        if (value->has_unknown_replication_card()) {
            WatchingFutures_.erase(replicationCardId);
            guard.Release();
            YT_LOG_DEBUG("Unknown replication card (Response: %v)", response);
            Callbacks_->OnUnknownReplicationCard(replicationCardId);
            return;
        }

        auto connection = Connection_.Lock();
        auto residencyCache = connection ? connection->GetChaosResidencyCache() : nullptr;

        if (value->has_replication_card_changed()) {
            const auto& newCardResponse = value->replication_card_changed();
            timestamp = FromProto<TTimestamp>(newCardResponse.replication_card_cache_timestamp());

            auto replicationCard = New<TReplicationCard>();
            FromProto(replicationCard.Get(), newCardResponse.replication_card());

            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Replication card changed (Response: %v)", response);
            if (residencyCache) {
                residencyCache->PingChaosObjectResidency(replicationCardId);
            }

            Callbacks_->OnReplicationCardUpdated(replicationCardId, std::move(replicationCard), timestamp);
            return;
        }

        if (value->has_replication_card_not_changed()) {
            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Replication card not changed (Response: %v)", response);
            if (residencyCache) {
                residencyCache->PingChaosObjectResidency(replicationCardId);
            }

            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }

        if (value->has_replication_card_migrated()) {
            const auto& migratedResponse = value->replication_card_migrated();
            auto newCellId = FromProto<TCellId>(migratedResponse.migrate_to_cell_id());
            auto newCellTag = NObjectClient::CellTagFromId(newCellId);
            if (residencyCache) {
                residencyCache->UpdateChaosObjectResidency(replicationCardId, newCellTag);
            }

            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Replication card migrated (Response: %v)", response);
            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }

        if (value->has_instance_is_not_leader()) {
            if (residencyCache) {
                residencyCache->RemoveChaosObjectResidency(replicationCardId);
            }

            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Instance is not leader (Response: %v)", response);
            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }
    }
};

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClient(
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
    TWeakPtr<IConnection> connection)
{
    return New<TReplicationCardsWatcherClient>(
        std::move(callbacks),
        std::move(connection));
}

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClient(
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
    IChannelPtr chaosCacheChannel,
    TWeakPtr<NApi::NNative::IConnection> connection)
{
    return New<TReplicationCardsWatcherClient>(
        std::move(callbacks),
        std::move(chaosCacheChannel),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

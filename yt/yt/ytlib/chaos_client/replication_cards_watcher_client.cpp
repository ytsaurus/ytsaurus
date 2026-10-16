#include "replication_cards_watcher_client.h"
#include "chaos_node_service_proxy.h"
#include "object_watcher_client.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>
#include <yt/yt/ytlib/chaos_client/master_cache_channel.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosClient {

using namespace NHydra;
using namespace NRpc;
using namespace NThreading;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NApi::NNative;
using namespace NLogging;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ReplicationCardWatcherClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardsWatcherClient
    : public IReplicationCardsWatcherClient
    , public TObjectWatcherClientBase
{
public:
    TReplicationCardsWatcherClient(
        std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
        TWeakPtr<NApi::NNative::IConnection> connection)
        : TObjectWatcherClientBase(std::move(connection))
        , Callbacks_(std::move(callbacks))
    { }

    TReplicationCardsWatcherClient(
        std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
        IChannelPtr chaosCacheChannel,
        TWeakPtr<NApi::NNative::IConnection> connection)
        : TObjectWatcherClientBase(std::move(connection), std::move(chaosCacheChannel))
        , Callbacks_(std::move(callbacks))
    { }

    void WatchReplicationCard(TReplicationCardId replicationCardId) override
    {
        WatchObject(replicationCardId);
    }

    void StopWatchingReplicationCard(TReplicationCardId replicationCardId) override
    {
        if (!StopWatchingObject(replicationCardId)) {
            return;
        }

        YT_TLOG_DEBUG("Stopped watching replication card")
            .With("ReplicationCardId", replicationCardId);
    }

private:
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> Callbacks_;

    TFuture<void> WatchUpstream(TReplicationCardId replicationCardId, TTimestamp timestamp) override
    {
        auto connection = GetConnection();
        if (!connection) {
            return MakeFuture(TError("Connection is not available"));
        }

        auto channel = GetWatchChannel(connection, replicationCardId);

        auto proxy = TChaosNodeServiceProxy(std::move(channel));
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosWatcherClientRequestTimeout);

        auto req = proxy.WatchReplicationCard();
        ToProto(req->mutable_replication_card_id(), replicationCardId);
        req->set_replication_card_cache_timestamp(ToProto(timestamp));

        SetChaosCacheStickyGroupBalancingHint(
            replicationCardId,
            req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));

        auto traceContext = TTraceContext::NewRoot("ReplicationCardWatcherClient");
        TTraceContextGuard traceContextGuard(traceContext);

        return req->Invoke().AsUnique().Apply(
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
            RemoveWatch(replicationCardId);
            YT_TLOG_DEBUG("Error watching replication card")
                .With(response);
            return;
        }

        const auto& value = response.Value();
        if (value->has_replication_card_deleted()) {
            RemoveWatch(replicationCardId);
            Callbacks_->OnReplicationCardDeleted(replicationCardId);
            return;
        }

        if (value->has_unknown_replication_card()) {
            RemoveWatch(replicationCardId);
            YT_TLOG_DEBUG("Unknown replication card")
                .With("Response", response);
            Callbacks_->OnUnknownReplicationCard(replicationCardId);
            return;
        }

        auto connection = GetConnection();
        auto residencyCache = connection ? connection->GetChaosResidencyCache() : nullptr;

        if (value->has_replication_card_changed()) {
            const auto& newCardResponse = value->replication_card_changed();
            auto responseTimestamp = FromProto<TTimestamp>(newCardResponse.replication_card_cache_timestamp());

            auto replicationCard = New<TReplicationCard>();
            FromProto(replicationCard.Get(), newCardResponse.replication_card());

            if (!RearmWatch(replicationCardId, responseTimestamp)) {
                YT_TLOG_DEBUG("Changed response received but card was already removed from cache")
                    .With("ReplicationCardId", replicationCardId);
            }

            YT_TLOG_DEBUG("Replication card changed")
                .With("Response", response);
            if (residencyCache) {
                residencyCache->PingChaosObjectResidency(replicationCardId);
            }

            Callbacks_->OnReplicationCardUpdated(replicationCardId, std::move(replicationCard), responseTimestamp);
            return;
        }

        if (value->has_replication_card_not_changed()) {
            if (!RearmWatch(replicationCardId)) {
                YT_TLOG_DEBUG("Nothing changed response received but card was already removed from cache")
                    .With("ReplicationCardId", replicationCardId);
            }

            YT_TLOG_DEBUG("Replication card not changed")
                .With("Response", response);
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

            if (!RearmWatch(replicationCardId)) {
                YT_TLOG_DEBUG("Replication card migrated response received but card was already removed from cache")
                    .With("ReplicationCardId", replicationCardId);
            }

            YT_TLOG_DEBUG("Replication card migrated")
                .With("Response", response);
            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }

        if (value->has_instance_is_not_leader()) {
            if (residencyCache) {
                residencyCache->RemoveChaosObjectResidency(replicationCardId);
            }

            if (!RearmWatch(replicationCardId)) {
                YT_TLOG_DEBUG("Leader switch response received but card was already removed from cache")
                    .With("ReplicationCardId", replicationCardId);
            }

            YT_TLOG_DEBUG("Instance is not leader")
                .With("Response", response);
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

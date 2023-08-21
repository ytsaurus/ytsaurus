#include "native_replication_card_cache_detail.h"

#include "chaos_cell_directory_synchronizer.h"
#include "chaos_node_service_proxy.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/chaos_client/config.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NChaosClient {

using namespace NApi;

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYTree;

using NNative::IClientPtr;
using NNative::IConnectionPtr;

using NYT::FromProto;

///////////////////////////////////////////////////////////////////////////////

class TReplicationCardCache
    : public IReplicationCardCache
    , public TAsyncExpiringCache<TReplicationCardCacheKey, TReplicationCardPtr>
{
public:
    TReplicationCardCache(
        TReplicationCardCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);
    TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardCacheKey& key) override;
    TFuture<TReplicationCardPtr> DoGet(const TReplicationCardCacheKey& key, bool isPeriodicUpdate) noexcept override;
    void ForceRefresh(const TReplicationCardCacheKey& key, const TReplicationCardPtr& replicationCard) override;
    void Clear() override;

    IChannelPtr GetChaosCacheChannel();

protected:
    class TGetSession;

    const TReplicationCardCacheConfigPtr Config_;
    const TWeakPtr<NNative::IConnection> Connection_;
    const IChannelPtr ChaosCacheChannel_;
    const NLogging::TLogger Logger;

    IChannelPtr CreateChaosCacheChannel(const NNative::IConnectionPtr& connection);
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCache::TGetSession
    : public TRefCounted
{
public:
    TGetSession(
        TReplicationCardCache* owner,
        const TReplicationCardCacheKey& key,
        const NLogging::TLogger& logger,
        TGuid sessionId)
        : Owner_(owner)
        , Key_ (key)
        , Logger(logger
            .WithTag("ReplicationCardId: %v, CacheSessionId: %v",
                Key_.CardId,
                sessionId))
    { }

    TReplicationCardPtr Run()
    {
        auto channel = Owner_->ChaosCacheChannel_;
        auto proxy = TChaosNodeServiceProxy(channel);
        auto req = proxy.GetReplicationCard();
        ToProto(req->mutable_replication_card_id(), Key_.CardId);
        ToProto(req->mutable_fetch_options(), Key_.FetchOptions);
        if (Key_.RefreshEra != InvalidReplicationEra) {
            req->set_refresh_era(Key_.RefreshEra);
        }

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        auto replicationCard = New<TReplicationCard>();

        FromProto(replicationCard.Get(), rsp->replication_card());

        YT_LOG_DEBUG("Got replication card (ReplicationCard: %v)",
            *replicationCard);

        if (auto connection = Owner_->Connection_.Lock()) {
            const auto& synchronizer = connection->GetChaosCellDirectorySynchronizer();
            synchronizer->AddCellTag(CellTagFromId(Key_.CardId));
            synchronizer->AddCellIds(replicationCard->CoordinatorCellIds);

            const auto& cellDirectory = connection->GetCellDirectory();
            auto isSyncCell = [&] (auto cellId) {
                return static_cast<bool>(cellDirectory->FindChannelByCellTag(CellTagFromId(cellId)));
            };
            auto isSyncCells = [&] (const std::vector<TCellId>& cellIds) {
                for (auto cellId : cellIds) {
                    if (!isSyncCell(cellId)) {
                        return false;
                    }
                }
                return true;
            };

            if (!isSyncCell(Key_.CardId) || !isSyncCells(replicationCard->CoordinatorCellIds)) {
                YT_LOG_DEBUG("Synchronizing replication card chaos cells");
                WaitFor(synchronizer->Sync())
                    .ThrowOnError();
                YT_LOG_DEBUG("Finished synchronizing replication card chaos cells");
            }
        }

        return replicationCard;
    }

private:
    const TIntrusivePtr<TReplicationCardCache> Owner_;
    const TReplicationCardCacheKey Key_;

    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

TReplicationCardCache::TReplicationCardCache(
    TReplicationCardCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(config)
    , Config_(std::move(config))
    , Connection_(connection)
    , ChaosCacheChannel_(CreateChaosCacheChannel(std::move(connection)))
    , Logger(logger)
{ }

TFuture<TReplicationCardPtr> TReplicationCardCache::GetReplicationCard(const TReplicationCardCacheKey& key)
{
    return TAsyncExpiringCache::Get(key);
}

TFuture<TReplicationCardPtr> TReplicationCardCache::DoGet(const TReplicationCardCacheKey& key, bool /*isPeriodicUpdate*/) noexcept
{
    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TReplicationCardPtr>(
            TError("Unable to get replication card: connection terminated")
                << TErrorAttribute("replication_card_id", key.CardId));
    }

    auto invoker = connection->GetInvoker();
    auto sessionId = TGuid::Create();
    auto session = New<TGetSession>(this, key, Logger, sessionId);

    YT_LOG_DEBUG("Requesting replication card (ReplicationCardId: %v, CacheSessionId: %v)",
        key.CardId,
        sessionId);

    return BIND(&TGetSession::Run, std::move(session))
        .AsyncVia(std::move(invoker))
        .Run();
}

void TReplicationCardCache::ForceRefresh(const TReplicationCardCacheKey& key, const TReplicationCardPtr& replicationCard)
{
    TAsyncExpiringCache<TReplicationCardCacheKey, TReplicationCardPtr>::ForceRefresh(key, replicationCard);
}

void TReplicationCardCache::Clear()
{
    TAsyncExpiringCache::Clear();
}

IChannelPtr TReplicationCardCache::CreateChaosCacheChannel(const NNative::IConnectionPtr& connection)
{
    auto channelFactory = connection->GetChannelFactory();
    auto endpointDescription = TString("ChaosCache");
    auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("chaos_cache").Value(true)
        .EndMap());
    auto channel = CreateBalancingChannel(
        Config_,
        std::move(channelFactory),
        std::move(endpointDescription),
        std::move(endpointAttributes));
    channel = CreateRetryingChannel(
        Config_,
        std::move(channel));
    return channel;
}

////////////////////////////////////////////////////////////////////////////////

IReplicationCardCachePtr CreateNativeReplicationCardCache(
    TReplicationCardCacheConfigPtr config,
    IConnectionPtr connection,
    NLogging::TLogger logger)
{
    return New<TReplicationCardCache>(
        std::move(config),
        std::move(connection),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

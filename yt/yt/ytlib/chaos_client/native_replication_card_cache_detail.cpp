#include "native_replication_card_cache_detail.h"

#include "chaos_cell_directory_synchronizer.h"
#include "chaos_node_service_proxy.h"
#include "master_cache_channel.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/chaos_client/replication_cards_watcher.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher_client.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/chaos_client/config.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>
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

////////////////////////////////////////////////////////////////////////////////

class TReplicationCacheCallbacks
    : public IReplicationCardWatcherClientCallbacks
{
public:
    TReplicationCacheCallbacks(
        TWeakPtr<TAsyncExpiringCache<TReplicationCardCacheKey, TReplicationCardPtr>> cache,
        NLogging::TLogger logger)
        : Cache_(std::move(cache))
        , Logger(std::move(logger))
    { }

    void OnReplicationCardUpdated(
        TReplicationCardId replicationCardId,
        const TReplicationCardPtr& replicationCard,
        NTransactionClient::TTimestamp timestamp) override
    {
        YT_TLOG_DEBUG("Replication card updated")
            .With("ReplicationCardId", replicationCardId)
            .With("Timestamp", timestamp)
            .With("ReplicationCard", *replicationCard);

        if (auto cache = Cache_.Lock()) {
            cache->Set(GetKey(replicationCardId), replicationCard);
        }
    }

    void OnReplicationCardDeleted(TReplicationCardId replicationCardId) override
    {
        YT_TLOG_DEBUG("Replication card deleted")
            .With("ReplicationCardId", replicationCardId);

        if (auto cache = Cache_.Lock()) {
            cache->InvalidateActive(GetKey(replicationCardId));
        }
    }

    void OnUnknownReplicationCard(TReplicationCardId replicationCardId) override
    {
        OnReplicationCardDeleted(replicationCardId);
    }

    void OnNothingChanged(TReplicationCardId replicationCardId) override
    {
        YT_TLOG_DEBUG("Nothing changed")
            .With("ReplicationCardId", replicationCardId);
    }

private:
    const TWeakPtr<TAsyncExpiringCache<TReplicationCardCacheKey, TReplicationCardPtr>> Cache_;
    const NLogging::TLogger Logger;

    static TReplicationCardCacheKey GetKey(TReplicationCardId replicationCardId)
    {
        return TReplicationCardCacheKey{replicationCardId, MinimalFetchOptions};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardFetcher
    : public TRefCounted
{
public:
    TReplicationCardFetcher(
        TReplicationCardCacheConfigPtr config,
        IConnectionPtr connection,
        NLogging::TLogger logger);

    TFuture<TReplicationCardPtr> Fetch(
        const TReplicationCardCacheKey& key,
        TAsyncExpiringCacheConfigPtr cacheConfig);

    const IChannelPtr& GetChaosCacheChannel() const;

private:
    class TGetSession;

    const TWeakPtr<NNative::IConnection> Connection_;
    const IChannelPtr ChaosCacheChannel_;
    const NLogging::TLogger Logger;
};

using TReplicationCardFetcherPtr = TIntrusivePtr<TReplicationCardFetcher>;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardFetcher::TGetSession
    : public TRefCounted
{
public:
    TGetSession(
        TReplicationCardFetcher* owner,
        const TReplicationCardCacheKey& key,
        TAsyncExpiringCacheConfigPtr cacheConfig,
        const NLogging::TLogger& logger,
        TGuid sessionId,
        TDuration timeout)
        : Owner_(owner)
        , Key_(key)
        , CacheConfig_(std::move(cacheConfig))
        , Timeout_(timeout)
        , Logger(logger
            .WithTag("ReplicationCardId", Key_.CardId)
            .WithTag("CacheSessionId", sessionId))
    { }

    TReplicationCardPtr Run()
    {
        auto channel = Owner_->ChaosCacheChannel_;
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.GetReplicationCard();
        req->SetTimeout(Timeout_);
        ToProto(req->mutable_replication_card_id(), Key_.CardId);
        ToProto(req->mutable_fetch_options(), Key_.FetchOptions);
        if (Key_.RefreshEra != InvalidReplicationEra) {
            req->set_refresh_era(Key_.RefreshEra);
        }

        SetChaosCacheStickyGroupBalancingHint(Key_.CardId,
            req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));

        auto refreshTime = CacheConfig_->RefreshTime.value_or(TDuration::Max());
        SetChaosCacheCachingHeader(
            std::min(CacheConfig_->ExpireAfterSuccessfulUpdateTime, refreshTime),
            std::min(CacheConfig_->ExpireAfterFailedUpdateTime, refreshTime),
            Key_.RefreshEra,
            req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext));

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        auto replicationCard = New<TReplicationCard>();

        FromProto(replicationCard.Get(), rsp->replication_card());

        YT_TLOG_DEBUG("Got replication card")
            .With("ReplicationCard", *replicationCard);

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
                YT_TLOG_DEBUG("Synchronizing replication card chaos cells");
                WaitFor(synchronizer->Sync())
                    .ThrowOnError();
                YT_TLOG_DEBUG("Finished synchronizing replication card chaos cells");
            }
        }

        return replicationCard;
    }

private:
    const TReplicationCardFetcherPtr Owner_;
    const TReplicationCardCacheKey Key_;
    const TAsyncExpiringCacheConfigPtr CacheConfig_;
    const TDuration Timeout_;

    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

TReplicationCardFetcher::TReplicationCardFetcher(
    TReplicationCardCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    NLogging::TLogger logger)
    : Connection_(connection)
    , ChaosCacheChannel_(CreateChaosCacheChannel(std::move(connection), std::move(config)))
    , Logger(std::move(logger))
{ }

TFuture<TReplicationCardPtr> TReplicationCardFetcher::Fetch(
    const TReplicationCardCacheKey& key,
    TAsyncExpiringCacheConfigPtr cacheConfig)
{
    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TReplicationCardPtr>(
            TError("Unable to get replication card: connection terminated")
                .With("replication_card_id", key.CardId));
    }

    auto timeout = connection->GetConfig()->DefaultChaosNodeServiceTimeout;
    auto invoker = connection->GetInvoker();
    auto sessionId = TGuid::Create();
    auto session = New<TGetSession>(this, key, std::move(cacheConfig), Logger, sessionId, timeout);

    YT_TLOG_DEBUG("Requesting replication card")
        .With("ReplicationCardId", key.CardId)
        .With("CacheSessionId", sessionId);

    return BIND(&TGetSession::Run, std::move(session))
        .AsyncVia(std::move(invoker))
        .Run();
}

const IChannelPtr& TReplicationCardFetcher::GetChaosCacheChannel() const
{
    return ChaosCacheChannel_;
}

////////////////////////////////////////////////////////////////////////////////

class TFetchingReplicationCardCache
    : public TAsyncExpiringCache<TReplicationCardCacheKey, TReplicationCardPtr>
{
public:
    TFetchingReplicationCardCache(
        TAsyncExpiringCacheConfigPtr config,
        TReplicationCardFetcherPtr fetcher)
        : TAsyncExpiringCache(
            std::move(config),
            NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , Fetcher_(std::move(fetcher))
    { }

protected:
    TFuture<TReplicationCardPtr> DoGet(
        const TReplicationCardCacheKey& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return Fetcher_->Fetch(key, GetConfig());
    }

private:
    const TReplicationCardFetcherPtr Fetcher_;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

TReplicationCardCacheKey GetWatchedCacheKey(const TReplicationCardCacheKey& key)
{
    return TReplicationCardCacheKey{
        .CardId = key.CardId,
        .FetchOptions = MinimalFetchOptions,
        .RefreshEra = key.RefreshEra,
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TWatchedReplicationCardCache
    : public TFetchingReplicationCardCache
{
public:
    TWatchedReplicationCardCache(
        TAsyncExpiringCacheConfigPtr config,
        TReplicationCardFetcherPtr fetcher,
        NLogging::TLogger logger)
        : TFetchingReplicationCardCache(std::move(config), std::move(fetcher))
        , Logger(std::move(logger))
    { }

    TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardCacheKey& key)
    {
        auto future = Get(GetWatchedCacheKey(key));

        YT_TLOG_DEBUG("Will watch replication card")
            .With("ReplicationCardId", key.CardId);

        future.Subscribe(BIND([watcherClient = WatcherClient_, id = key.CardId] (const TErrorOr<TReplicationCardPtr>& card) {
            if (card.IsOK()) {
                watcherClient->WatchReplicationCard(id);
            }
        }));

        return future;
    }

    void ForceRefresh(
        const TReplicationCardCacheKey& key,
        const TReplicationCardPtr& replicationCard)
    {
        TFetchingReplicationCardCache::ForceRefresh(GetWatchedCacheKey(key), replicationCard);
    }

    void SetWatcherClient(IReplicationCardsWatcherClientPtr watcherClient)
    {
        YT_VERIFY(!WatcherClient_);
        WatcherClient_ = std::move(watcherClient);
    }

protected:
    void OnRemoved(const TReplicationCardCacheKey& key) noexcept override
    {
        TFetchingReplicationCardCache::OnRemoved(key);
        if (WatcherClient_) {
            WatcherClient_->StopWatchingReplicationCard(key.CardId);
        }
    }

private:
    IReplicationCardsWatcherClientPtr WatcherClient_;
    const NLogging::TLogger Logger;
};

using TWatchedReplicationCardCachePtr = TIntrusivePtr<TWatchedReplicationCardCache>;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardProgressCache
    : public TFetchingReplicationCardCache
{
public:
    using TFetchingReplicationCardCache::TFetchingReplicationCardCache;

    TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardCacheKey& key)
    {
        return Get(key);
    }
};

using TReplicationCardProgressCachePtr = TIntrusivePtr<TReplicationCardProgressCache>;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCache
    : public IReplicationCardCache
{
public:
    TReplicationCardCache(
        TReplicationCardCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);

    TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardCacheKey& key) override;
    void ForceRefresh(
        const TReplicationCardCacheKey& key,
        const TReplicationCardPtr& replicationCard) override;
    void Clear() override;
    void Reconfigure(const TReplicationCardCacheConfigPtr& config) override;

private:
    const TReplicationCardFetcherPtr Fetcher_;
    const TWatchedReplicationCardCachePtr WatchedCache_;
    const TReplicationCardProgressCachePtr ProgressCache_;

    std::atomic<bool> EnableWatching_ = false;

    bool ShouldWatch(const TReplicationCardCacheKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(osidorkin) Use better cache that is aware of era.
TReplicationCardCache::TReplicationCardCache(
    TReplicationCardCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : Fetcher_(New<TReplicationCardFetcher>(config, connection, logger))
    , WatchedCache_(New<TWatchedReplicationCardCache>(
        config->WatchedCacheConfig,
        Fetcher_,
        logger))
    , ProgressCache_(New<TReplicationCardProgressCache>(
        config,
        Fetcher_))
    , EnableWatching_(config->EnableWatching)
{
    WatchedCache_->SetWatcherClient(CreateReplicationCardsWatcherClient(
        std::make_unique<TReplicationCacheCallbacks>(
            MakeWeak(WatchedCache_),
            logger),
        Fetcher_->GetChaosCacheChannel(),
        connection));
}

bool TReplicationCardCache::ShouldWatch(const TReplicationCardCacheKey& key) const
{
    return EnableWatching_.load() && MinimalFetchOptions.Contains(key.FetchOptions);
}

TFuture<TReplicationCardPtr> TReplicationCardCache::GetReplicationCard(
    const TReplicationCardCacheKey& key)
{
    if (!ShouldWatch(key)) {
        return ProgressCache_->GetReplicationCard(key);
    }

    return WatchedCache_->GetReplicationCard(key);
}

void TReplicationCardCache::ForceRefresh(
    const TReplicationCardCacheKey& key,
    const TReplicationCardPtr& replicationCard)
{
    if (ShouldWatch(key)) {
        WatchedCache_->ForceRefresh(key, replicationCard);
    } else {
        ProgressCache_->ForceRefresh(key, replicationCard);
    }
}

void TReplicationCardCache::Clear()
{
    WatchedCache_->Clear();
    ProgressCache_->Clear();
}

void TReplicationCardCache::Reconfigure(const TReplicationCardCacheConfigPtr& config)
{
    bool wasWatchingEnabled = EnableWatching_.exchange(config->EnableWatching);
    if (wasWatchingEnabled && !config->EnableWatching) {
        WatchedCache_->Clear();
    }

    WatchedCache_->Reconfigure(config->WatchedCacheConfig);
    ProgressCache_->Reconfigure(config);
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

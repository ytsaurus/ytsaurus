#include "native_chaos_lease_cache.h"

#include "chaos_cell_directory_synchronizer.h"
#include "chaos_leases_watcher_client.h"
#include "chaos_node_service_proxy.h"
#include "master_cache_channel.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/chaos_client/chaos_lease.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/fluent.h>

#include <algorithm>
#include <atomic>

namespace NYT::NChaosClient {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

using TLeaseCache = TAsyncExpiringCache<TChaosLeaseId, TChaosLeasePtr>;

static void SyncChaosLeaseCells(
    const IConnectionPtr& connection,
    TChaosLeaseId chaosLeaseId,
    const TChaosLeasePtr& chaosLease,
    const NLogging::TLogger& Logger)
{
    const auto& synchronizer = connection->GetChaosCellDirectorySynchronizer();
    synchronizer->AddCellTag(CellTagFromId(chaosLeaseId));
    synchronizer->AddCellIds(chaosLease->CoordinatorCellIds);

    const auto& cellDirectory = connection->GetCellDirectory();
    auto isKnownCell = [&] (TCellId cellId) {
        return cellDirectory->FindChannelByCellTag(CellTagFromId(cellId)) != nullptr;
    };

    bool needsSync = !isKnownCell(chaosLeaseId);
    for (auto coordinatorCellId : chaosLease->CoordinatorCellIds) {
        needsSync |= !isKnownCell(coordinatorCellId);
    }

    if (needsSync) {
        YT_TLOG_DEBUG("Synchronizing chaos lease cells")
            .With("ChaosLeaseId", chaosLeaseId);
        WaitFor(synchronizer->Sync())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TChaosLeaseCacheCallbacks
    : public IChaosLeaseWatcherClientCallbacks
{
public:
    TChaosLeaseCacheCallbacks(
        TWeakPtr<TLeaseCache> cache,
        NLogging::TLogger logger)
        : Cache_(std::move(cache))
        , Logger(std::move(logger))
    { }

    void OnChaosLeaseUpdated(
        TChaosLeaseId chaosLeaseId,
        const TChaosLeasePtr& chaosLease,
        NTransactionClient::TTimestamp /*timestamp*/) override
    {
        if (auto cache = Cache_.Lock()) {
            cache->Set(chaosLeaseId, chaosLease);
        }
    }

    void OnChaosLeaseDeleted(TChaosLeaseId chaosLeaseId) override
    {
        Invalidate(chaosLeaseId);
    }

    void OnUnknownChaosLease(TChaosLeaseId chaosLeaseId) override
    {
        Invalidate(chaosLeaseId);
    }

    void OnChaosLeaseMigrated(TChaosLeaseId chaosLeaseId) override
    {
        Invalidate(chaosLeaseId);
    }

    void OnNothingChanged(TChaosLeaseId chaosLeaseId) override
    {
        YT_TLOG_DEBUG("Nothing changed")
            .With("ChaosLeaseId", chaosLeaseId);
    }

private:
    const TWeakPtr<TLeaseCache> Cache_;
    const NLogging::TLogger Logger;

    void Invalidate(TChaosLeaseId chaosLeaseId)
    {
        if (auto cache = Cache_.Lock()) {
            cache->InvalidateActive(chaosLeaseId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChaosLeaseFetcher
    : public TRefCounted
{
public:
    TChaosLeaseFetcher(
        TChaosLeaseCacheConfigPtr config,
        IConnectionPtr connection,
        NLogging::TLogger logger)
        : Connection_(connection)
        , ChaosCacheChannel_(config->Addresses || config->Endpoints
            ? CreateChaosCacheChannel(connection, config)
            : nullptr)
        , Logger(std::move(logger))
    { }

    TFuture<TChaosLeasePtr> Fetch(
        TChaosLeaseId chaosLeaseId,
        TAsyncExpiringCacheConfigPtr cacheConfig)
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            return MakeFuture<TChaosLeasePtr>(
                TError("Unable to get chaos lease: connection terminated")
                    .With("chaos_lease_id", chaosLeaseId));
        }

        YT_TLOG_DEBUG("Requesting chaos lease")
            .With("ChaosLeaseId", chaosLeaseId);

        auto invoker = connection->GetInvoker();
        return BIND(
            &TChaosLeaseFetcher::DoFetch,
            MakeStrong(this),
            std::move(connection),
            chaosLeaseId,
            std::move(cacheConfig))
            .AsyncVia(std::move(invoker))
            .Run();
    }

    const IChannelPtr& GetChaosCacheChannel() const
    {
        return ChaosCacheChannel_;
    }

private:
    const TWeakPtr<IConnection> Connection_;
    const IChannelPtr ChaosCacheChannel_;
    const NLogging::TLogger Logger;

    TChaosLeasePtr DoFetch(
        const IConnectionPtr& connection,
        TChaosLeaseId chaosLeaseId,
        const TAsyncExpiringCacheConfigPtr& cacheConfig)
    {
        auto channel = ChaosCacheChannel_
            ? ChaosCacheChannel_
            : connection->GetChaosChannelByObjectIdOrThrow(chaosLeaseId);
        TChaosNodeServiceProxy proxy(std::move(channel));
        auto req = proxy.GetChaosLease();
        req->SetTimeout(connection->GetConfig()->DefaultChaosNodeServiceTimeout);
        ToProto(req->mutable_chaos_lease_id(), chaosLeaseId);

        if (ChaosCacheChannel_) {
            SetChaosCacheStickyGroupBalancingHint(
                chaosLeaseId,
                req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));
            auto refreshTime = cacheConfig->RefreshTime.value_or(TDuration::Max());
            SetChaosCacheCachingHeader(
                std::min(cacheConfig->ExpireAfterSuccessfulUpdateTime, refreshTime),
                std::min(cacheConfig->ExpireAfterFailedUpdateTime, refreshTime),
                InvalidReplicationEra,
                req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext));
        }

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        auto chaosLease = New<TChaosLease>();
        chaosLease->Timeout = FromProto<TDuration>(rsp->timeout());
        FromProto(&chaosLease->CoordinatorCellIds, rsp->coordinator_cell_ids());

        SyncChaosLeaseCells(connection, chaosLeaseId, chaosLease, Logger);

        return chaosLease;
    }
};

using TChaosLeaseFetcherPtr = TIntrusivePtr<TChaosLeaseFetcher>;

////////////////////////////////////////////////////////////////////////////////

class TNativeChaosLeaseCache
    : public IChaosLeaseCache
    , public TLeaseCache
{
public:
    TNativeChaosLeaseCache(
        TChaosLeaseCacheConfigPtr config,
        TChaosLeaseFetcherPtr fetcher,
        NLogging::TLogger logger)
        : TLeaseCache(config, NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , Fetcher_(std::move(fetcher))
        , Logger(std::move(logger))
        , EnableWatching_(config->EnableWatching)
    { }

    TFuture<TChaosLeasePtr> GetChaosLease(TChaosLeaseId chaosLeaseId) override
    {
        auto future = Get(chaosLeaseId);

        if (!EnableWatching_.load()) {
            return future;
        }

        YT_TLOG_DEBUG("Will watch chaos lease")
            .With("ChaosLeaseId", chaosLeaseId);

        future.Subscribe(BIND([watcherClient = WatcherClient_, cache = MakeWeak(this), id = chaosLeaseId] (const TErrorOr<TChaosLeasePtr>& lease) {
            if (lease.IsOK()) {
                if (auto owner = cache.Lock(); owner && owner->EnableWatching_.load()) {
                    watcherClient->WatchChaosLease(id);
                }
            }
        }));

        return future;
    }

    void Clear() override
    {
        TLeaseCache::Clear();
    }

    void Reconfigure(const TChaosLeaseCacheConfigPtr& config) override
    {
        bool wasWatchingEnabled = EnableWatching_.exchange(config->EnableWatching);
        if (wasWatchingEnabled && !config->EnableWatching) {
            TLeaseCache::Clear();
        }

        TLeaseCache::Reconfigure(config);
    }

    void SetWatcherClient(IChaosLeasesWatcherClientPtr watcherClient)
    {
        YT_VERIFY(!WatcherClient_);
        WatcherClient_ = std::move(watcherClient);
    }

private:
    const TChaosLeaseFetcherPtr Fetcher_;
    const NLogging::TLogger Logger;

    std::atomic<bool> EnableWatching_;
    IChaosLeasesWatcherClientPtr WatcherClient_;

    TFuture<TChaosLeasePtr> DoGet(
        const TChaosLeaseId& key,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return Fetcher_->Fetch(key, GetConfig());
    }

    void OnRemoved(const TChaosLeaseId& chaosLeaseId) noexcept override
    {
        TLeaseCache::OnRemoved(chaosLeaseId);
        if (WatcherClient_) {
            WatcherClient_->StopWatchingChaosLease(chaosLeaseId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosLeaseCachePtr CreateNativeChaosLeaseCache(
    TChaosLeaseCacheConfigPtr config,
    IConnectionPtr connection,
    NLogging::TLogger logger)
{
    auto fetcher = New<TChaosLeaseFetcher>(config, connection, logger);
    auto cache = New<TNativeChaosLeaseCache>(config, fetcher, logger);
    cache->SetWatcherClient(CreateChaosLeasesWatcherClient(
        std::make_unique<TChaosLeaseCacheCallbacks>(MakeWeak(cache), logger),
        fetcher->GetChaosCacheChannel(),
        connection));
    return cache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

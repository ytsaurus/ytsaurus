#include "chaos_cache_service.h"

#include "chaos_cache.h"
#include "private.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chaos_client/public.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher_client.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NMasterCache {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree::NProto;
using namespace NApi::NNative;
using namespace NChaosClient;
using namespace NChaosCache;
using namespace NTransactionClient;
using namespace NLogging;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TCacheWatcherCallbacks
    : public IReplicationCardWatcherClientCallbacks
{
public:
    TCacheWatcherCallbacks(
        IReplicationCardsWatcherPtr replicationCardWatcher,
        TChaosCachePtr chaosCache,
        TDuration expirationDelay,
        TLogger logger)
        : ReplicationCardsWatcher_(std::move(replicationCardWatcher))
        , ChaosCache_(chaosCache)
        , ExpirationDelay_(expirationDelay)
        , Logger(logger)
    { }

    void OnReplicationCardUpdated(
        const TReplicationCardId& replicationCardId,
        TReplicationCardPtr replicationCard,
        TTimestamp timestamp) override
    {
        YT_LOG_DEBUG("Replication card updated (ReplicationCardId: %v, Timestamp: %v)",
            replicationCardId,
            timestamp);

        ReplicationCardsWatcher_->OnReplcationCardUpdated(replicationCardId, replicationCard, timestamp);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnReplicationCardDeleted(
        const TReplicationCardId& replicationCardId) override
    {
        YT_LOG_DEBUG("Replication card deleted (ReplicationCardId: %v)",
            replicationCardId);

        ReplicationCardsWatcher_->OnReplicationCardRemoved(replicationCardId);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnUnknownReplicationCard(
        const TReplicationCardId& replicationCardId) override
    {
        YT_LOG_DEBUG("Unknown replication cardc (ReplicationCardId: %v)",
            replicationCardId);

        ReplicationCardsWatcher_->OnReplicationCardRemoved(replicationCardId);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnNothingChanged(
        const TReplicationCardId& replicationCardId) override
    {
        YT_LOG_DEBUG("Nothing changed (ReplicationCardId: %v)",
            replicationCardId);

        if (ReplicationCardsWatcher_->GetLastSeenWatchers(replicationCardId) + ExpirationDelay_ < TInstant::Now() &&
            ReplicationCardsWatcher_->TryUnregisterReplicationCard(replicationCardId))
        {
            if (auto owner = Owner_.Lock(); owner != nullptr) {
                owner->StopWatchingReplicationCard(replicationCardId);
            }
        }
    }

    void SetOwner(TWeakPtr<IReplicationCardsWatcherClient> owner)
    {
        Owner_ = owner;
    }

private:
    const IReplicationCardsWatcherPtr ReplicationCardsWatcher_;
    const TChaosCachePtr ChaosCache_;
    const TDuration ExpirationDelay_;
    const TLogger Logger;

    TWeakPtr<IReplicationCardsWatcherClient> Owner_ = nullptr;

    static TChaosCacheKey GetKey(const TReplicationCardId& replicationCardId)
    {
        return TChaosCacheKey{
            .CardId = replicationCardId,
            .FetchOptions = MinimalFetchOptions,
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClientWithCallbacks(
    IReplicationCardsWatcherPtr replicationCardsWatcher,
    TChaosCachePtr chaosCache,
    TDuration expirationDelay,
    IConnectionPtr connection,
    const TLogger& logger)
{
    auto callbacks = std::make_unique<TCacheWatcherCallbacks>(
        std::move(replicationCardsWatcher),
        std::move(chaosCache),
        expirationDelay,
        logger);

    auto* callbacksPtr = callbacks.get();

    auto watcherClient = CreateReplicationCardsWatcherClient(
        std::move(callbacks),
        std::move(connection));
    callbacksPtr->SetOwner(watcherClient);

    return watcherClient;
}

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheService
    : public TServiceBase
{
public:
    TChaosCacheService(
        TChaosCacheConfigPtr config,
        IInvokerPtr invoker,
        IClientPtr client,
        TChaosCachePtr cache,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            invoker,
            TChaosNodeServiceProxy::GetDescriptor(),
            MasterCacheLogger(),
            NullRealmId,
            std::move(authenticator))
        , Cache_(std::move(cache))
        , Client_(std::move(client))
        , ReplicationCardsWatcher_(CreateReplicationCardsWatcher(
            config->ReplicationCardsWatcher,
            std::move(invoker)))
        , ReplicationCardsWatcherClient_(CreateReplicationCardsWatcherClientWithCallbacks(
            ReplicationCardsWatcher_,
            Cache_,
            config->UnwatchedCardExpirationDelay,
            Client_->GetNativeConnection(),
            Logger))
        , EnableWatching_(config->EnableWatching)
    {
        ReplicationCardsWatcher_->Start(std::vector<std::pair<TReplicationCardId, TReplicationCardPtr>>());

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchReplicationCard));
    }

    ~TChaosCacheService()
    {
        ReplicationCardsWatcher_->Stop();
    }

private:
    const TChaosCachePtr Cache_;
    const IClientPtr Client_;
    const IReplicationCardsWatcherPtr ReplicationCardsWatcher_;
    const IReplicationCardsWatcherClientPtr ReplicationCardsWatcherClient_;
    const bool EnableWatching_ = false;

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCard);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, WatchReplicationCard);
};

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, GetReplicationCard)
{
    auto requestId = context->GetRequestId();
    auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
    auto fetchOptions = FromProto<TReplicationCardFetchOptions>(request->fetch_options());
    auto refreshEra = request->has_refresh_era()
        ? request->refresh_era()
        : InvalidReplicationEra;

    context->SetRequestInfo("ReplicationCardId: %v, FetchOptions: %v, RefreshEra: %v",
        replicationCardId,
        fetchOptions,
        refreshEra);

    const auto& extendedFetchOptions = MinimalFetchOptions.Contains(fetchOptions)
        ? MinimalFetchOptions
        : fetchOptions;

    TFuture<TReplicationCardPtr> replicationCardFuture;
    const auto& requestHeader = context->GetRequestHeader();
    if (requestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
        const auto& cachingRequestHeaderExt = requestHeader.GetExtension(TCachingHeaderExt::caching_header_ext);
        const auto& user = context->GetAuthenticationIdentity().User;

        auto key = TChaosCacheKey{
            .CardId = replicationCardId,
            .FetchOptions = extendedFetchOptions,
        };

        YT_LOG_DEBUG("Serving request from cache (RequestId: %v, Key: %v, User: %v)",
            requestId,
            key,
            user);

        auto expireAfterSuccessfulUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt.expire_after_successful_update_time());
        auto expireAfterFailedUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt.expire_after_failed_update_time());

        auto cookie = Cache_->BeginLookup(
            requestId,
            key,
            expireAfterSuccessfulUpdateTime,
            expireAfterFailedUpdateTime,
            refreshEra,
            user);

        replicationCardFuture = cookie.GetValue().Apply(BIND([] (const TErrorOr<TChaosCacheEntryPtr>& entry) -> TErrorOr<TReplicationCardPtr> {
            if (entry.IsOK()) {
                return entry.Value()->GetReplicationCard();
            } else {
                return TError(entry);
            }
        }));

        if (cookie.IsActive()) {
            NApi::TGetReplicationCardOptions getCardOptions;
            getCardOptions.BypassCache = true;
            static_cast<TReplicationCardFetchOptions&>(getCardOptions) = extendedFetchOptions;

            // TODO(max42): switch to Subscribe.
            YT_UNUSED_FUTURE(Client_->GetReplicationCard(replicationCardId, getCardOptions).Apply(
                BIND([=, this, this_ = MakeStrong(this), cookie = std::move(cookie)] (const TErrorOr<TReplicationCardPtr>& replicationCardOrError) mutable {
                    Cache_->EndLookup(
                        requestId,
                        std::move(cookie),
                        replicationCardOrError);
                })));
        }
    } else {
        NApi::TGetReplicationCardOptions getCardOptions;
        getCardOptions.BypassCache = true;
        static_cast<TReplicationCardFetchOptions&>(getCardOptions) = fetchOptions;

        replicationCardFuture = Client_->GetReplicationCard(replicationCardId, getCardOptions);
    }

    context->ReplyFrom(replicationCardFuture
        .Apply(BIND([context, response, fetchOptions] (const TReplicationCardPtr& replicationCard) {
            ToProto(response->mutable_replication_card(), *replicationCard, fetchOptions);
        })));
}

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, WatchReplicationCard)
{
    auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
    auto cacheTimestamp = FromProto<TTimestamp>(request->replication_card_cache_timestamp());

    context->SetRequestInfo("ReplicationCardId: %v, CacheTimestamp: %v",
        replicationCardId,
        cacheTimestamp);

    if (!EnableWatching_) {
        context->Reply(TError("Watching is disabled"));
        return;
    }

    auto state = ReplicationCardsWatcher_->WatchReplicationCard(replicationCardId, cacheTimestamp, context, true);
    if (state != EReplicationCardWatherState::Deleted) {
        ReplicationCardsWatcherClient_->WatchReplicationCard(replicationCardId);
    }
}

IServicePtr CreateChaosCacheService(
    TChaosCacheConfigPtr config,
    IInvokerPtr invoker,
    IClientPtr client,
    TChaosCachePtr cache,
    IAuthenticatorPtr authenticator)
{
    return New<TChaosCacheService>(
        std::move(config),
        std::move(invoker),
        std::move(client),
        std::move(cache),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache

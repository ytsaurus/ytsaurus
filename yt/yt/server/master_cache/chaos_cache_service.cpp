#include "chaos_cache_service.h"

#include "chaos_cache.h"
#include "private.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/server/lib/chaos_node/replication_card_watcher_service_callbacks.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>
#include <yt/yt/ytlib/chaos_client/public.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher_client.h>

#include <yt/yt/client/object_client/helpers.h>

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
using namespace NChaosNode;
using namespace NObjectClient;
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
        , ChaosCache_(std::move(chaosCache))
        , ExpirationDelay_(expirationDelay)
        , Logger(std::move(logger))
    { }

    void OnReplicationCardUpdated(
        TReplicationCardId replicationCardId,
        const TReplicationCardPtr& replicationCard,
        TTimestamp timestamp) override
    {
        YT_LOG_DEBUG("Replication card updated (ReplicationCardId: %v, Timestamp: %v)",
            replicationCardId,
            timestamp);

        ReplicationCardsWatcher_->OnReplcationCardUpdated(replicationCardId, replicationCard, timestamp);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnReplicationCardDeleted(TReplicationCardId replicationCardId) override
    {
        YT_LOG_DEBUG("Replication card deleted (ReplicationCardId: %v)",
            replicationCardId);

        ReplicationCardsWatcher_->OnReplicationCardRemoved(replicationCardId);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnUnknownReplicationCard(TReplicationCardId replicationCardId) override
    {
        YT_LOG_DEBUG("Unknown replication card (ReplicationCardId: %v)",
            replicationCardId);

        ReplicationCardsWatcher_->OnReplicationCardRemoved(replicationCardId);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnNothingChanged(TReplicationCardId replicationCardId) override
    {
        if (ReplicationCardsWatcher_->GetLastSeenWatchers(replicationCardId) + ExpirationDelay_ < TInstant::Now() &&
            ReplicationCardsWatcher_->TryUnregisterReplicationCard(replicationCardId))
        {
            if (auto owner = Owner_.Lock()) {
                owner->StopWatchingReplicationCard(replicationCardId);
            }

            YT_LOG_DEBUG("Watching request expired. Watcher expired and unregistered (ReplicationCardId: %v)",
                replicationCardId);
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

    TWeakPtr<IReplicationCardsWatcherClient> Owner_;

    static TChaosCacheKey GetKey(TReplicationCardId replicationCardId)
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
    TLogger logger)
{
    auto callbacks = std::make_unique<TCacheWatcherCallbacks>(
        std::move(replicationCardsWatcher),
        std::move(chaosCache),
        expirationDelay,
        std::move(logger));

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
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
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
    {
        ReplicationCardsWatcher_->Start({});

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCardResidency));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChaosObjectResidency));
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

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCard);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, WatchReplicationCard);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCardResidency);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetChaosObjectResidency);

    TFuture<TCellTag> DoGetChaosObjectResidency(
        TChaosObjectId chaosObjectId,
        std::optional<TCellTag> cellTagToForceRefresh);
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

    auto state = ReplicationCardsWatcher_->WatchReplicationCard(
        replicationCardId,
        cacheTimestamp,
        CreateReplicationCardWatcherCallbacks(context),
        /*allowUnregistered*/ true);
    if (state != EReplicationCardWatherState::Deleted) {
        ReplicationCardsWatcherClient_->WatchReplicationCard(replicationCardId);
    }
}

TFuture<TCellTag> TChaosCacheService::DoGetChaosObjectResidency(
    TChaosObjectId chaosObjectId,
    std::optional<TCellTag> cellTagToForceRefresh)
{
    auto chaosResidencyCache = Client_->GetNativeConnection()->GetChaosResidencyCache();
    auto residencyFuture = chaosResidencyCache->GetChaosResidency(chaosObjectId);
    if (!cellTagToForceRefresh) {
        return residencyFuture;
    }

    auto refreshedFuture = residencyFuture
        .Apply(BIND([
            chaosResidencyCache = std::move(chaosResidencyCache),
            chaosObjectId,
            cellTagToForceRefresh = *cellTagToForceRefresh
        ] (const TCellTag& cellTag)
        {
            if (cellTagToForceRefresh == cellTag) {
                chaosResidencyCache->ForceRefresh(chaosObjectId, cellTag);
                return chaosResidencyCache->GetChaosResidency(chaosObjectId);
            } else {
                return MakeFuture(cellTag);
            }
        }));
    return refreshedFuture;
}

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, GetChaosObjectResidency)
{
    auto chaosObjectId = FromProto<TReplicationCardId>(request->chaos_object_id());
    auto cellTagToForceRefresh =
        request->has_force_refresh_chaos_object_cell_tag()
            ? std::make_optional(FromProto<TCellTag>(
                request->force_refresh_chaos_object_cell_tag()))
            : std::optional<TCellTag>();

    context->SetRequestInfo("ChaosObjectId: %v, ChaosObjectType: %v CellTagToForceRefresh: %v",
        chaosObjectId,
        TypeFromId(chaosObjectId),
        cellTagToForceRefresh);

    auto replier = BIND([context, response] (const TCellTag& cellTag) {
        response->set_chaos_object_cell_tag(ToProto<ui32>(cellTag));
    });

    auto residencyFuture = DoGetChaosObjectResidency(chaosObjectId, cellTagToForceRefresh);

    context->ReplyFrom(residencyFuture
        .Apply(replier));
}

// COMPAT(gryzlov-ad)
DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, GetReplicationCardResidency)
{
    auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
    auto cellTagToForceRefresh =
        request->has_force_refresh_replication_card_cell_tag()
            ? std::make_optional(FromProto<NObjectClient::TCellTag>(
                request->force_refresh_replication_card_cell_tag()))
            : std::optional<NObjectClient::TCellTag>();

    context->SetRequestInfo("ReplicationCardId: %v, CellTagToForceRefresh: %v",
        replicationCardId,
        cellTagToForceRefresh);

    auto replier = BIND([context, response] (const TCellTag& cellTag) {
        response->set_replication_card_cell_tag(ToProto<ui32>(cellTag));
    });

    auto residencyFuture = DoGetChaosObjectResidency(replicationCardId, cellTagToForceRefresh);

    context->ReplyFrom(residencyFuture
        .Apply(replier));
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

#include "chaos_cache_service.h"

#include "chaos_cache.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/server/lib/chaos_node/chaos_lease_watcher_service_callbacks.h>
#include <yt/yt/server/lib/chaos_node/replication_card_watcher_service_callbacks.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_leases_watcher.h>
#include <yt/yt/ytlib/chaos_client/chaos_leases_watcher_client.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>
#include <yt/yt/ytlib/chaos_client/public.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/replication_card_updates_batcher.h>
#include <yt/yt/ytlib/chaos_client/replication_card_updates_batcher_serialization.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher.h>
#include <yt/yt/ytlib/chaos_client/replication_cards_watcher_client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/chaos_client/chaos_lease.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChaosCache {

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
        YT_TLOG_DEBUG("Replication card updated")
            .With("ReplicationCardId", replicationCardId)
            .With("Timestamp", timestamp);

        ReplicationCardsWatcher_->OnObjectUpdated(replicationCardId, replicationCard, timestamp);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnReplicationCardDeleted(TReplicationCardId replicationCardId) override
    {
        YT_TLOG_DEBUG("Replication card deleted")
            .With("ReplicationCardId", replicationCardId);

        ReplicationCardsWatcher_->OnObjectRemoved(replicationCardId);
        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnUnknownReplicationCard(TReplicationCardId replicationCardId) override
    {
        YT_TLOG_DEBUG("Unknown replication card")
            .With("ReplicationCardId", replicationCardId);

        ChaosCache_->TryRemove(GetKey(replicationCardId));
    }

    void OnNothingChanged(TReplicationCardId replicationCardId) override
    {
        if (ReplicationCardsWatcher_->GetLastSeenWatchersTime(replicationCardId) + ExpirationDelay_ < TInstant::Now() &&
            ReplicationCardsWatcher_->TryUnregisterObject(replicationCardId))
        {
            if (auto owner = Owner_.Lock()) {
                owner->StopWatchingReplicationCard(replicationCardId);
            }

            YT_TLOG_DEBUG("Watching request expired; watcher expired and unregistered")
                .With("ReplicationCardId", replicationCardId);
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

class TChaosLeaseCacheWatcherCallbacks
    : public IChaosLeaseWatcherClientCallbacks
{
public:
    TChaosLeaseCacheWatcherCallbacks(
        IChaosLeasesWatcherPtr chaosLeasesWatcher,
        TDuration expirationDelay,
        TLogger logger)
        : ChaosLeasesWatcher_(std::move(chaosLeasesWatcher))
        , ExpirationDelay_(expirationDelay)
        , Logger(std::move(logger))
    { }

    void OnChaosLeaseUpdated(
        TChaosLeaseId chaosLeaseId,
        const TChaosLeasePtr& chaosLease,
        TTimestamp timestamp) override
    {
        YT_TLOG_DEBUG("Chaos lease updated")
            .With("ChaosLeaseId", chaosLeaseId)
            .With("Timestamp", timestamp);

        ChaosLeasesWatcher_->OnObjectUpdated(chaosLeaseId, chaosLease, timestamp);
    }

    void OnChaosLeaseDeleted(TChaosLeaseId chaosLeaseId) override
    {
        YT_TLOG_DEBUG("Chaos lease deleted")
            .With("ChaosLeaseId", chaosLeaseId);

        ChaosLeasesWatcher_->OnObjectRemoved(chaosLeaseId);
    }

    void OnUnknownChaosLease(TChaosLeaseId chaosLeaseId) override
    {
        YT_TLOG_DEBUG("Unknown chaos lease")
            .With("ChaosLeaseId", chaosLeaseId);
    }

    void OnChaosLeaseMigrated(TChaosLeaseId chaosLeaseId) override
    {
        YT_TLOG_DEBUG("Chaos lease migrated; upstream watch was redirected")
            .With("ChaosLeaseId", chaosLeaseId);
    }

    void OnNothingChanged(TChaosLeaseId chaosLeaseId) override
    {
        if (ChaosLeasesWatcher_->GetLastSeenWatchersTime(chaosLeaseId) + ExpirationDelay_ < TInstant::Now() &&
            ChaosLeasesWatcher_->TryUnregisterObject(chaosLeaseId))
        {
            if (auto owner = Owner_.Lock()) {
                owner->StopWatchingChaosLease(chaosLeaseId);
            }

            YT_TLOG_DEBUG("Chaos lease watching request expired; watcher expired and unregistered")
                .With("ChaosLeaseId", chaosLeaseId);
        }
    }

    void SetOwner(TWeakPtr<IChaosLeasesWatcherClient> owner)
    {
        Owner_ = std::move(owner);
    }

private:
    const IChaosLeasesWatcherPtr ChaosLeasesWatcher_;
    const TDuration ExpirationDelay_;
    const TLogger Logger;

    TWeakPtr<IChaosLeasesWatcherClient> Owner_;
};

////////////////////////////////////////////////////////////////////////////////

IChaosLeasesWatcherClientPtr CreateChaosLeasesWatcherClientWithCallbacks(
    IChaosLeasesWatcherPtr chaosLeasesWatcher,
    TDuration expirationDelay,
    IConnectionPtr connection,
    TLogger logger)
{
    auto callbacks = std::make_unique<TChaosLeaseCacheWatcherCallbacks>(
        std::move(chaosLeasesWatcher),
        expirationDelay,
        std::move(logger));

    auto* callbacksPtr = callbacks.get();

    auto watcherClient = CreateChaosLeasesWatcherClient(
        std::move(callbacks),
        std::move(connection));
    callbacksPtr->SetOwner(watcherClient);

    return watcherClient;
}

const TReplicationCardFetchOptions& ExtendFetchOptions(const TReplicationCardFetchOptions& fetchOptions)
{
    if (MinimalFetchOptions.Contains(fetchOptions)) {
        return MinimalFetchOptions;
    }

    if (FetchOptionsWithProgress.Contains(fetchOptions)) {
        return FetchOptionsWithProgress;
    }

    // Seems to be request from master.
    return fetchOptions;
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
        IAuthenticatorPtr authenticator,
        TLogger logger)
        : TServiceBase(
            invoker,
            TChaosNodeServiceProxy::GetDescriptor(),
            std::move(logger),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , Cache_(std::move(cache))
        , Client_(std::move(client))
        , ReplicationCardsWatcher_(CreateReplicationCardsWatcher(
            config->ReplicationCardsWatcher,
            invoker))
        , ReplicationCardsWatcherClient_(CreateReplicationCardsWatcherClientWithCallbacks(
            ReplicationCardsWatcher_,
            Cache_,
            config->UnwatchedCardExpirationDelay,
            Client_->GetNativeConnection(),
            Logger))
        , ChaosLeasesWatcher_(CreateChaosLeasesWatcher(
            config->ChaosLeasesWatcher,
            invoker))
        , ChaosLeasesWatcherClient_(CreateChaosLeasesWatcherClientWithCallbacks(
            ChaosLeasesWatcher_,
            config->UnwatchedLeaseExpirationDelay,
            Client_->GetNativeConnection(),
            Logger))
        , ReplicationCardUpdatesBatcher_(CreateMasterCacheReplicationCardUpdatesBatcher(
            config->ReplicationCardUpdateBatcher,
            Client_->GetNativeConnection(),
            std::move(invoker),
            Logger))
    {
        ReplicationCardsWatcher_->Start({});
        ChaosLeasesWatcher_->Start({});
        ReplicationCardUpdatesBatcher_->Start();

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChaosLease));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchChaosLease));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCardResidency));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChaosObjectResidency));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateTableProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateMultipleTableProgresses));
    }

    ~TChaosCacheService()
    {
        ReplicationCardUpdatesBatcher_->Stop();
        ChaosLeasesWatcher_->Stop();
        ReplicationCardsWatcher_->Stop();
    }

private:
    const TChaosCachePtr Cache_;
    const IClientPtr Client_;
    const IReplicationCardsWatcherPtr ReplicationCardsWatcher_;
    const IReplicationCardsWatcherClientPtr ReplicationCardsWatcherClient_;
    const IChaosLeasesWatcherPtr ChaosLeasesWatcher_;
    const IChaosLeasesWatcherClientPtr ChaosLeasesWatcherClient_;
    const IReplicationCardUpdatesBatcherPtr ReplicationCardUpdatesBatcher_;

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCard);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetChaosLease);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, WatchReplicationCard);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, WatchChaosLease);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCardResidency);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetChaosObjectResidency);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateTableProgress);
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, UpdateMultipleTableProgresses);

    TFuture<TCellTag> DoGetChaosObjectResidency(
        TChaosObjectId chaosObjectId,
        std::optional<TCellTag> cellTagToForceRefresh);

    TFuture<TChaosLeasePtr> FetchChaosLeaseFromCell(TChaosLeaseId chaosLeaseId, TInstant deadline);
};

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, GetReplicationCard)
{
    auto requestId = context->GetRequestId();
    auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
    auto fetchOptions = FromProto<TReplicationCardFetchOptions>(request->fetch_options());
    // TODO(osidorkin): Get rid of refresh_era here, use cachingRequestHeaderExt.refresh_revision.
    auto refreshEra = request->has_refresh_era()
        ? request->refresh_era()
        : InvalidReplicationEra;

    const auto& extendedFetchOptions = ExtendFetchOptions(fetchOptions);

    TFuture<TReplicationCardPtr> replicationCardFuture;
    const auto& requestHeader = context->GetRequestHeader();
    if (requestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
        const auto& cachingRequestHeaderExt = requestHeader.GetExtension(TCachingHeaderExt::caching_header_ext);
        if (refreshEra == InvalidReplicationEra) {
            if (cachingRequestHeaderExt.has_refresh_revision()) {
                refreshEra = cachingRequestHeaderExt.refresh_revision();
            }
        }

        const auto& user = context->GetAuthenticationIdentity().User;

        context->AnnotateRequest()
            .With("ReplicationCardId", replicationCardId)
            .With("FetchOptions", fetchOptions)
            .With("RefreshEra", refreshEra);

        auto key = TChaosCacheKey{
            .CardId = replicationCardId,
            .FetchOptions = extendedFetchOptions,
        };

        YT_TLOG_DEBUG("Serving request from cache")
            .With("RequestId", requestId)
            .With("Key", key);

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

            Client_->GetReplicationCard(replicationCardId, getCardOptions).AsUnique().Subscribe(
                BIND([
                    requestId,
                    cache = Cache_,
                    cookie = std::move(cookie)
                ] (TErrorOr<TReplicationCardPtr>&& replicationCardOrError) mutable {
                    cache->EndLookup(
                        requestId,
                        std::move(cookie),
                        std::move(replicationCardOrError));
                }));
        }
    } else {
        context->AnnotateRequest()
            .With("ReplicationCardId", replicationCardId)
            .With("FetchOptions", fetchOptions);

        YT_TLOG_DEBUG("Serving request directly")
            .With("RequestId", requestId);

        // TODO(osidorkin): Get rid of Client_ here: No need to call WaitFor and checking options again inside it.
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

    context->AnnotateRequest()
        .With("ReplicationCardId", replicationCardId)
        .With("CacheTimestamp", cacheTimestamp);

    auto state = ReplicationCardsWatcher_->WatchObject(
        replicationCardId,
        cacheTimestamp,
        CreateReplicationCardWatcherCallbacks(context),
        /*allowUnregistered*/ true);
    if (state != EObjectWatcherState::Deleted) {
        ReplicationCardsWatcherClient_->WatchReplicationCard(replicationCardId);
    }
}

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, GetChaosLease)
{
    auto chaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id());
    context->AnnotateRequest()
        .With("ChaosLeaseId", chaosLeaseId);

    if (context->GetRequestHeader().HasExtension(TCachingHeaderExt::caching_header_ext)) {
        if (auto chaosLease = ChaosLeasesWatcher_->FindObject(chaosLeaseId)) {
            YT_TLOG_DEBUG("Serving chaos lease from watcher")
                .With("ChaosLeaseId", chaosLeaseId);

            response->set_timeout(ToProto(chaosLease->Timeout));
            response->set_last_ping_time(ToProto(TInstant::Zero()));
            ToProto(response->mutable_coordinator_cell_ids(), chaosLease->CoordinatorCellIds);
            context->Reply();
            return;
        }
    }

    auto timeout = context->GetTimeout().value_or(
        Client_->GetNativeConnection()->GetConfig()->DefaultChaosNodeServiceTimeout);
    auto startTime = context->GetStartTime().value_or(TInstant::Now());
    auto chaosLeaseFuture = FetchChaosLeaseFromCell(chaosLeaseId, startTime + timeout);

    context->ReplyFrom(chaosLeaseFuture
        .Apply(BIND([context, response] (const TChaosLeasePtr& chaosLease) {
            response->set_timeout(ToProto(chaosLease->Timeout));
            response->set_last_ping_time(ToProto(chaosLease->LastPingTime));
            ToProto(response->mutable_coordinator_cell_ids(), chaosLease->CoordinatorCellIds);
        })));
}

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, WatchChaosLease)
{
    auto chaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id());
    auto cacheTimestamp = FromProto<TTimestamp>(request->chaos_lease_cache_timestamp());

    context->AnnotateRequest()
        .With("ChaosLeaseId", chaosLeaseId)
        .With("CacheTimestamp", cacheTimestamp);

    auto state = ChaosLeasesWatcher_->WatchObject(
        chaosLeaseId,
        cacheTimestamp,
        CreateChaosLeaseWatcherCallbacks(context),
        /*allowUnregistered*/ true);
    if (state != EObjectWatcherState::Deleted) {
        ChaosLeasesWatcherClient_->WatchChaosLease(chaosLeaseId);
    }
}

TFuture<TChaosLeasePtr> TChaosCacheService::FetchChaosLeaseFromCell(
    TChaosLeaseId chaosLeaseId,
    TInstant deadline)
{
    auto connection = Client_->GetNativeConnection();
    return BIND([client = Client_, connection, chaosLeaseId, deadline] {
        auto fetch = [&] () -> TErrorOr<TChaosLeasePtr> {
            auto remaining = deadline - TInstant::Now();
            if (remaining <= TDuration::Zero()) {
                return TError(NYT::EErrorCode::Timeout, "Timed out getting chaos lease %v", chaosLeaseId);
            }

            TGetChaosLeaseOptions options;
            options.BypassCache = true;
            options.Timeout = remaining;
            return WaitFor(client->GetChaosLease(chaosLeaseId, options));
        };

        auto result = fetch();
        if (!result.IsOK() && TError(result).FindMatching(NYTree::EErrorCode::ResolveError)) {
            connection->GetChaosResidencyCache()->RemoveChaosObjectResidency(chaosLeaseId);
            result = fetch();
        }

        return result.ValueOrThrow();
    })
        .AsyncVia(connection->GetInvoker())
        .Run();
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

    context->AnnotateRequest()
        .With("ChaosObjectId", chaosObjectId)
        .With("ChaosObjectType", TypeFromId(chaosObjectId))
        .With("CellTagToForceRefresh", cellTagToForceRefresh);

    auto replier = BIND([context, response] (const TCellTag& cellTag) {
        response->set_chaos_object_cell_tag(ToProto(cellTag));
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

    context->AnnotateRequest()
        .With("ReplicationCardId", replicationCardId)
        .With("CellTagToForceRefresh", cellTagToForceRefresh);

    auto replier = BIND([context, response] (const TCellTag& cellTag) {
        response->set_replication_card_cell_tag(ToProto(cellTag));
    });

    auto residencyFuture = DoGetChaosObjectResidency(replicationCardId, cellTagToForceRefresh);

    context->ReplyFrom(residencyFuture
        .Apply(replier));
}

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, UpdateTableProgress)
{
    auto replicationProgressUpdate = NYT::FromProto<TReplicationCardProgressUpdate>(
        request->replication_card_progress_update());

    context->AnnotateRequest()
        .With("ReplicationCardId", replicationProgressUpdate.ReplicationCardId)
        .With("FetchOptions", replicationProgressUpdate.FetchOptions);

    auto futureCard = ReplicationCardUpdatesBatcher_->AddReplicationCardProgressesUpdate(std::move(
        replicationProgressUpdate));

    auto futureResponse = futureCard.AsUnique().Apply(BIND([context, response] (TReplicationCardPtr&& card) {
        if (card) {
            ToProto(response->mutable_replication_card(), *card);
        }
    }));

    context->ReplyFrom(std::move(futureResponse));
}

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, UpdateMultipleTableProgresses)
{
    auto replicationProgressUpdatesBatch = NYT::FromProto<TReplicationCardProgressUpdatesBatch>(*request);
    context->AnnotateRequest()
        .With("ReplicationCardIdsCount", replicationProgressUpdatesBatch.ReplicationCardProgressUpdates.size());

    auto futureCardsByIds = ReplicationCardUpdatesBatcher_->AddBulkReplicationCardProgressesUpdate(std::move(
        replicationProgressUpdatesBatch));

    auto futureCards = std::vector<TFuture<TReplicationCardPtr>>();
    futureCards.reserve(futureCardsByIds.size());
    for (const auto& futureCardById : futureCardsByIds) {
        futureCards.push_back(futureCardById.second);
    }

    auto futureAllCardsUpdated = AllSet(std::move(futureCards));

    auto futureUpdateResult = futureAllCardsUpdated.AsUnique().Apply(BIND(
        [
            context,
            response,
            futureCardsByIds = std::move(futureCardsByIds)
        ] (std::vector<TErrorOr<TReplicationCardPtr>>&& /*results*/) {
            response->mutable_replication_card_progress_update_results()->Reserve(futureCardsByIds.size());

            for (auto& [replicatrionCardId, replicationCardFuture] : futureCardsByIds) {
                auto* protoUpdateResult = response->add_replication_card_progress_update_results();
                ToProto(protoUpdateResult->mutable_replication_card_id(), replicatrionCardId);

                if (const auto& replicationCardOrError = replicationCardFuture.GetOrCrash(); replicationCardOrError.IsOK()) {
                    auto* protoProgressUpdateResult = protoUpdateResult->mutable_result();
                    if (const auto& replicationCardPtr = replicationCardOrError.Value()) {
                        ToProto(protoProgressUpdateResult->mutable_replication_card(), *replicationCardPtr);
                    }
                } else {
                    ToProto(protoUpdateResult->mutable_error(), replicationCardOrError);
                }
            }
        }));

    context->ReplyFrom(std::move(futureUpdateResult));
}

IServicePtr CreateChaosCacheService(
    TChaosCacheConfigPtr config,
    IInvokerPtr invoker,
    IClientPtr client,
    TChaosCachePtr cache,
    IAuthenticatorPtr authenticator,
    TLogger logger)
{
    return New<TChaosCacheService>(
        std::move(config),
        std::move(invoker),
        std::move(client),
        std::move(cache),
        std::move(authenticator),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache

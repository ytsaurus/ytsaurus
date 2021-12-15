#include "config.h"
#include "chaos_cache_service.h"
#include "chaos_cache.h"
#include "private.h"

#include <yt/yt/ytlib/chaos_client/public.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NMasterCache {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree::NProto;
using namespace NApi;
using namespace NChaosClient;

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheService
    : public TServiceBase
{
public:
    TChaosCacheService(
        IInvokerPtr invoker,
        IClientPtr masterClient,
        TChaosCachePtr cache)
        : TServiceBase(
            std::move(invoker),
            TChaosServiceProxy::GetDescriptor(),
            MasterCacheLogger)
        , Cache_(std::move(cache))
        , Client_(std::move(masterClient))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
    }

private:
    const TChaosCachePtr Cache_;
    const IClientPtr Client_;

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetReplicationCard);
};

DEFINE_RPC_SERVICE_METHOD(TChaosCacheService, GetReplicationCard)
{
    auto requestId = context->GetRequestId();
    auto replicationCardToken = FromProto<TReplicationCardToken>(request->replication_card_token());
    bool requestCoordinators = request->request_coordinators();
    bool requestProgress = request->request_replication_progress();
    bool requestHistory = request->request_history();
    auto options = TGetReplicationCardOptions{
        .IncludeCoordinators = requestCoordinators,
        .IncludeProgress = requestProgress,
        .IncludeHistory = requestHistory,
        .BypassCache = true
    };

    context->SetRequestInfo("ReplicationCardToken: %v",
        replicationCardToken);

    TFuture<TReplicationCardPtr> asyncReplicationCard;
    const auto& requestHeader = context->GetRequestHeader();
    if (requestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
        const auto& cachingRequestHeaderExt = requestHeader.GetExtension(TCachingHeaderExt::caching_header_ext);

        auto key = TChaosCacheKey(
            context->GetAuthenticationIdentity().User,
            replicationCardToken,
            requestCoordinators,
            requestProgress,
            requestHistory);

        YT_LOG_DEBUG("Serving request from cache (RequestId: %v, Key: %v)",
            requestId,
            key);

        auto expireAfterSuccessfulUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt.expire_after_successful_update_time());
        auto expireAfterFailedUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt.expire_after_failed_update_time());

        auto cookie = Cache_->BeginLookup(
            requestId,
            key,
            expireAfterSuccessfulUpdateTime,
            expireAfterFailedUpdateTime);

        asyncReplicationCard = cookie.GetValue().Apply(BIND([] (const TErrorOr<TChaosCacheEntryPtr>& entry) -> TErrorOr<TReplicationCardPtr> {
            if (entry.IsOK()) {
                return entry.Value()->GetReplicationCard();
            } else {
                return TError(entry);
            }
        }));

        if (cookie.IsActive()) {
            Client_->GetReplicationCard(replicationCardToken, options).Apply(
                BIND([=, this_ = MakeStrong(this), cookie = std::move(cookie)] (const TErrorOr<TReplicationCardPtr>& replicationCardOrError) mutable {
                    Cache_->EndLookup(
                        requestId,
                        std::move(cookie),
                        replicationCardOrError);
                }));
        }
    } else {
        asyncReplicationCard = Client_->GetReplicationCard(replicationCardToken, options);
    }

    context->ReplyFrom(asyncReplicationCard
        .Apply(BIND([=] (const TReplicationCardPtr& replicationCard) {
            ToProto(response->mutable_replication_card(), *replicationCard, requestCoordinators, requestProgress, requestHistory);
        })));
}

IServicePtr CreateChaosCacheService(
    IInvokerPtr invoker,
    IClientPtr masterClient,
    TChaosCachePtr cache)
{
    return New<TChaosCacheService>(
        std::move(invoker),
        std::move(masterClient),
        std::move(cache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache

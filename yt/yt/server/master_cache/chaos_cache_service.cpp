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
        IClientPtr client,
        TChaosCachePtr cache,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            std::move(invoker),
            TChaosNodeServiceProxy::GetDescriptor(),
            MasterCacheLogger,
            NullRealmId,
            std::move(authenticator))
        , Cache_(std::move(cache))
        , Client_(std::move(client))
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
    auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
    auto fetchOptions = FromProto<TReplicationCardFetchOptions>(request->fetch_options());
    auto refreshEra = request->has_refresh_era()
        ? request->refresh_era()
        : InvalidReplicationEra;

    context->SetRequestInfo("ReplicationCardId: %v, FetchOptions: %v, RefreshEra: %v",
        replicationCardId,
        fetchOptions,
        refreshEra);

    TGetReplicationCardOptions getCardOptions;
    getCardOptions.BypassCache = true;
    static_cast<TReplicationCardFetchOptions&>(getCardOptions) = fetchOptions;

    TFuture<TReplicationCardPtr> replicationCardFuture;
    const auto& requestHeader = context->GetRequestHeader();
    if (requestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
        const auto& cachingRequestHeaderExt = requestHeader.GetExtension(TCachingHeaderExt::caching_header_ext);

        auto key = TChaosCacheKey{
            .User = context->GetAuthenticationIdentity().User,
            .CardId = replicationCardId,
            .FetchOptions = fetchOptions
        };

        YT_LOG_DEBUG("Serving request from cache (RequestId: %v, Key: %v)",
            requestId,
            key);

        auto expireAfterSuccessfulUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt.expire_after_successful_update_time());
        auto expireAfterFailedUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt.expire_after_failed_update_time());

        auto cookie = Cache_->BeginLookup(
            requestId,
            key,
            expireAfterSuccessfulUpdateTime,
            expireAfterFailedUpdateTime,
            refreshEra);

        replicationCardFuture = cookie.GetValue().Apply(BIND([] (const TErrorOr<TChaosCacheEntryPtr>& entry) -> TErrorOr<TReplicationCardPtr> {
            if (entry.IsOK()) {
                return entry.Value()->GetReplicationCard();
            } else {
                return TError(entry);
            }
        }));

        if (cookie.IsActive()) {
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
        replicationCardFuture = Client_->GetReplicationCard(replicationCardId, getCardOptions);
    }

    context->ReplyFrom(replicationCardFuture
        .Apply(BIND([context, response, fetchOptions] (const TReplicationCardPtr& replicationCard) {
            ToProto(response->mutable_replication_card(), *replicationCard, fetchOptions);
        })));
}

IServicePtr CreateChaosCacheService(
    IInvokerPtr invoker,
    IClientPtr client,
    TChaosCachePtr cache,
    IAuthenticatorPtr authenticator)
{
    return New<TChaosCacheService>(
        std::move(invoker),
        std::move(client),
        std::move(cache),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache

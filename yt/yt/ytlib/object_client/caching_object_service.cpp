#include "caching_object_service.h"

#include "config.h"
#include "object_service_cache.h"
#include "object_service_proxy.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/throttling_channel.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <yt/yt/core/rpc/per_user_request_queue_provider.h>

namespace NYT::NObjectClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NSecurityClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TCachingObjectService
    : public ICachingObjectService
    , public TServiceBase
{
public:
    TCachingObjectService(
        TCachingObjectServiceConfigPtr config,
        IInvokerPtr invoker,
        IThrottlingChannelPtr cypressChannel,
        TObjectServiceCachePtr cache,
        TRealmId masterCellId,
        NLogging::TLogger logger,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            std::move(invoker),
            TObjectServiceProxy::GetDescriptor(),
            logger,
            masterCellId,
            std::move(authenticator))
        , Config_(config)
        , Cache_(std::move(cache))
        , CellId_(masterCellId)
        , CypressChannel_(std::move(cypressChannel))
        , Logger(logger.WithTag("RealmId: %v", masterCellId))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(10'000)
            .SetConcurrencyLimit(10'000)
            .SetRequestQueueProvider(ExecuteRequestQueueProvider_));

        DeclareServerFeature(EMasterFeature::Portals);
        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);

        Reconfigure(New<TCachingObjectServiceDynamicConfig>());
    }

    TCachingObjectService(
        TCachingObjectServiceConfigPtr config,
        IInvokerPtr invoker,
        const NApi::NNative::IConnectionPtr& connection,
        TObjectServiceCachePtr cache,
        TRealmId masterCellId,
        NLogging::TLogger logger,
        IAuthenticatorPtr authenticator)
        : TCachingObjectService(
            config,
            std::move(invoker),
            CreateCypressChannel(
                connection,
                CellTagFromId(masterCellId),
                config),
            std::move(cache),
            masterCellId,
            std::move(logger),
            std::move(authenticator))
        { }

    TCachingObjectService(
        TCachingObjectServiceConfigPtr config,
        IInvokerPtr invoker,
        IChannelPtr cacheChannel,
        TObjectServiceCachePtr cache,
        TRealmId masterCellId,
        NLogging::TLogger logger,
        IAuthenticatorPtr authenticator)
        : TCachingObjectService(
            config,
            std::move(invoker),
            CreateThrottlingChannel(config, std::move(cacheChannel)),
            std::move(cache),
            masterCellId,
            std::move(logger),
            std::move(authenticator))
        { }

    void Reconfigure(const TCachingObjectServiceDynamicConfigPtr& config) override
    {
        CypressChannel_->Reconfigure(config);
        CacheTtlRatio_.store(config->CacheTtlRatio);
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    const TCachingObjectServiceConfigPtr Config_;
    const TObjectServiceCachePtr Cache_;
    const TCellId CellId_;
    const IThrottlingChannelPtr CypressChannel_;
    const NLogging::TLogger Logger;

    const IRequestQueueProviderPtr ExecuteRequestQueueProvider_ = New<TPerUserRequestQueueProvider>();

    std::atomic<double> CacheTtlRatio_;

    std::atomic<bool> CachingEnabled_ = false;

    static IThrottlingChannelPtr CreateCypressChannel(
        const NApi::NNative::IConnectionPtr& connection,
        TCellTag cellTag,
        const TCachingObjectServiceConfigPtr& config)
    {
        // TODO(gritukan): Support Cypress Proxies here.
        auto channel = connection->GetMasterChannelOrThrow(
            NApi::EMasterChannelKind::Follower,
            cellTag);
        return CreateThrottlingChannel(config, std::move(channel));
    }
};

DEFINE_RPC_SERVICE_METHOD(TCachingObjectService, Execute)
{
    auto requestId = context->GetRequestId();

    context->SetRequestInfo("RequestCount: %v",
        request->part_counts_size());

    int attachmentIndex = 0;
    const auto& attachments = request->Attachments();

    std::vector<TFuture<TObjectServiceCacheEntryPtr>> cacheEntryFutures;

    for (int subrequestIndex = 0; subrequestIndex < request->part_counts_size(); ++subrequestIndex) {
        int partCount = request->part_counts(subrequestIndex);
        auto subrequestParts = std::vector<TSharedRef>(
            attachments.begin() + attachmentIndex,
            attachments.begin() + attachmentIndex + partCount);
        auto subrequestMessage = TSharedRefArray(std::move(subrequestParts), TSharedRefArray::TMoveParts{});
        attachmentIndex += partCount;

        if (subrequestMessage.Size() < 2) {
            THROW_ERROR_EXCEPTION("Malformed subrequest message: at least two parts are expected");
        }

        TRequestHeader subrequestHeader;
        if (!ParseRequestHeader(subrequestMessage, &subrequestHeader)) {
            THROW_ERROR_EXCEPTION("Malformed subrequest message: failed to parse header");
        }

        const auto& ypathExt = subrequestHeader.GetExtension(TYPathHeaderExt::ypath_header_ext);

        if (!subrequestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
            THROW_ERROR_EXCEPTION("Subrequest is lacking caching header");
        }
        auto* cachingRequestHeaderExt = subrequestHeader.MutableExtension(TCachingHeaderExt::caching_header_ext);

        auto refreshRevision = cachingRequestHeaderExt->refresh_revision();

        auto suppressUpstreamSync = request->suppress_upstream_sync();
        auto suppressTransactionCoordinatorSync = request->suppress_transaction_coordinator_sync();
        // COMPAT(aleksandra-zh)
        if (subrequestHeader.HasExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext)) {
            const auto& multicellSyncExt = subrequestHeader.GetExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
            suppressUpstreamSync |= multicellSyncExt.suppress_upstream_sync();
            suppressTransactionCoordinatorSync |= multicellSyncExt.suppress_transaction_coordinator_sync();
        }

        auto currentStickyGroupSize = request->has_current_sticky_group_size()
            ? std::make_optional(request->current_sticky_group_size())
            : std::nullopt;

        TObjectServiceCacheKey key(
            CellTagFromId(CellId_),
            cachingRequestHeaderExt->disable_per_user_cache() ? TString() : context->GetAuthenticationIdentity().User,
            ypathExt.target_path(),
            subrequestHeader.service(),
            subrequestHeader.method(),
            subrequestMessage[1],
            suppressUpstreamSync,
            suppressTransactionCoordinatorSync);

        if (ypathExt.mutating()) {
            THROW_ERROR_EXCEPTION("Cannot cache responses for mutating requests");
        }

        if (subrequestMessage.Size() > 2) {
            THROW_ERROR_EXCEPTION("Cannot cache responses for requests with attachments");
        }

        YT_LOG_DEBUG("Serving subrequest from cache (RequestId: %v, SubrequestIndex: %v, Key: %v, CurrentStickyGroupSize: %v)",
            requestId,
            subrequestIndex,
            key,
            currentStickyGroupSize);

        auto expireAfterSuccessfulUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt->expire_after_successful_update_time());
        auto expireAfterFailedUpdateTime = FromProto<TDuration>(cachingRequestHeaderExt->expire_after_failed_update_time());
        auto successStalenessBound = FromProto<TDuration>(cachingRequestHeaderExt->success_staleness_bound());

        auto cacheTtlRatio = CacheTtlRatio_.load();
        auto nodeExpireAfterSuccessfulUpdateTime = expireAfterSuccessfulUpdateTime * cacheTtlRatio;
        auto nodeExpireAfterFailedUpdateTime = expireAfterFailedUpdateTime * cacheTtlRatio;

        bool cachingEnabled = CachingEnabled_.load(std::memory_order::relaxed) && !cachingRequestHeaderExt->disable_second_level_cache();
        auto cookie = Cache_->BeginLookup(
            requestId,
            key,
            cachingEnabled ? nodeExpireAfterSuccessfulUpdateTime : expireAfterSuccessfulUpdateTime,
            cachingEnabled ? nodeExpireAfterFailedUpdateTime : expireAfterFailedUpdateTime,
            successStalenessBound,
            refreshRevision);

        if (cookie.ExpiredEntry()) {
            cacheEntryFutures.push_back(MakeFuture(cookie.ExpiredEntry()));
            // Since stale response was successfully found on this cache level,
            // we forbid stale responses on upper levels.
            cachingRequestHeaderExt->set_success_staleness_bound(ToProto<i64>(TDuration::Zero()));
        } else {
            cacheEntryFutures.push_back(cookie.GetValue());
        }

        if (cookie.IsActive()) {
            auto proxy = TObjectServiceProxy::FromDirectMasterChannel(CypressChannel_);
            auto req = proxy.Execute();
            SetCurrentAuthenticationIdentity(req);

            if (cachingEnabled) {
                auto* balancingHeaderExt = req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
                balancingHeaderExt->set_enable_stickiness(true);
                balancingHeaderExt->set_sticky_group_size(1);

                cachingRequestHeaderExt->set_expire_after_successful_update_time(ToProto<i64>(expireAfterSuccessfulUpdateTime - nodeExpireAfterSuccessfulUpdateTime));
                cachingRequestHeaderExt->set_expire_after_failed_update_time(ToProto<i64>(expireAfterFailedUpdateTime - nodeExpireAfterFailedUpdateTime));
            }

            auto* multicellSyncExt = req->Header().MutableExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
            multicellSyncExt->set_suppress_upstream_sync(suppressUpstreamSync);
            multicellSyncExt->set_suppress_transaction_coordinator_sync(suppressTransactionCoordinatorSync);

            subrequestMessage = SetRequestHeader(subrequestMessage, subrequestHeader);

            req->set_supports_portals(true);
            req->add_part_counts(subrequestMessage.Size());
            req->Attachments().insert(
                req->Attachments().end(),
                subrequestMessage.Begin(),
                subrequestMessage.End());

            YT_UNUSED_FUTURE(req->Invoke().Apply(
                BIND([this, this_ = MakeStrong(this), cookie = std::move(cookie), requestId] (
                    const TObjectServiceProxy::TErrorOrRspExecutePtr& rspOrError) mutable
                {
                    if (!rspOrError.IsOK()) {
                        YT_LOG_WARNING(rspOrError, "Cache population request failed (Key: %v)", cookie.GetKey());
                        cookie.Cancel(rspOrError);
                        return;
                    }

                    const auto& rsp = rspOrError.Value();
                    YT_VERIFY(
                        rsp->part_counts_size() == 1 ||
                        (rsp->subresponses_size() == 1 && rsp->subresponses(0).part_count() == 1));
                    auto responseMessage = TSharedRefArray(rsp->Attachments(), TSharedRefArray::TCopyParts{});

                    TResponseHeader responseHeader;
                    if (!TryParseResponseHeader(responseMessage, &responseHeader)) {
                        YT_LOG_WARNING("Error parsing cache population response header (Key: %v)", cookie.GetKey());
                        cookie.Cancel(TError(NRpc::EErrorCode::ProtocolError, "Error parsing response header"));
                        return;
                    }

                    auto responseError = FromProto<TError>(responseHeader.error());
                    auto revision = rsp->revisions_size() > 0 ? rsp->revisions(0) : NHydra::NullRevision;

                    bool cachingEnabled = rsp->caching_enabled();
                    if (CachingEnabled_.exchange(cachingEnabled) != cachingEnabled) {
                        YT_LOG_INFO("Changing next level object service cache mode (Enable: %v)", cachingEnabled);
                    }

                    Cache_->EndLookup(requestId, std::move(cookie), responseMessage, revision, responseError.IsOK());
                })));
        }
    }

    AllSucceeded(cacheEntryFutures)
        .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TObjectServiceCacheEntryPtr>>& cacheEntriesOrError) {
            if (!cacheEntriesOrError.IsOK()) {
                context->Reply(cacheEntriesOrError);
                return;
            }

            const auto& cacheEntries = cacheEntriesOrError.Value();

            auto& responseAttachments = response->Attachments();
            for (int subrequestIndex = 0; subrequestIndex < request->part_counts_size(); ++subrequestIndex) {
                const auto& cacheEntry = cacheEntries[subrequestIndex];
                if (request->has_current_sticky_group_size()) {
                    auto currentSize = request->current_sticky_group_size();
                    Cache_->UpdateAdvisedEntryStickyGroupSize(cacheEntry, currentSize);
                    int advisedSize = Cache_->GetAdvisedEntryStickyGroupSize(cacheEntry);
                    YT_LOG_DEBUG("Cache sticky group size advised (RequestId: %v, Key: %v, CurrentSize: %v, AdvisedSize: %v)",
                        requestId,
                        cacheEntry->GetKey(),
                        currentSize,
                        advisedSize);
                    response->add_advised_sticky_group_size(advisedSize);
                }
                const auto& responseMessage = cacheEntry->GetResponseMessage();
                response->add_part_counts(responseMessage.Size());
                responseAttachments.insert(
                    responseAttachments.end(),
                    responseMessage.Begin(),
                    responseMessage.End());
            }

            for (const auto& cacheEntry : cacheEntries) {
                if (cacheEntry->GetRevision() == NHydra::NullRevision) {
                    response->clear_revisions();
                    break;
                }
                response->add_revisions(cacheEntry->GetRevision());
            }

            response->set_caching_enabled(true);

            context->Reply();
        }));
}

////////////////////////////////////////////////////////////////////////////////

ICachingObjectServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    const NApi::NNative::IConnectionPtr& connection,
    TObjectServiceCachePtr cache,
    TRealmId masterCellId,
    NLogging::TLogger logger,
    IAuthenticatorPtr authenticator)
{
    return New<TCachingObjectService>(
        std::move(config),
        std::move(invoker),
        connection,
        std::move(cache),
        masterCellId,
        std::move(logger),
        std::move(authenticator));
}

ICachingObjectServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    IChannelPtr cacheChannel,
    TObjectServiceCachePtr cache,
    TRealmId masterCellId,
    NLogging::TLogger logger,
    IAuthenticatorPtr authenticator)
{
    return New<TCachingObjectService>(
        std::move(config),
        std::move(invoker),
        std::move(cacheChannel),
        std::move(cache),
        masterCellId,
        std::move(logger),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

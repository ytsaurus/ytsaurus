#include "caching_object_service.h"
#include "private.h"

#include <yt/ytlib/object_client/config.h>
#include <yt/ytlib/object_client/object_service_cache.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_slru_cache.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/checksum.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/throttling_channel.h>

#include <yt/core/ytree/proto/ypath.pb.h>

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

struct TSubrequestResponse
{
    TSubrequestResponse() = default;

    TSubrequestResponse(
        TSharedRefArray message,
        NHydra::TRevision revision,
        double byteRate)
        : Message(std::move(message))
        , Revision(revision)
        , ByteRate(byteRate)
    { }

    TSharedRefArray Message;
    NHydra::TRevision Revision;
    double ByteRate = 0.0;
};

class TCachingObjectService
    : public ICachingObjectService
    , public TServiceBase
{
public:
    TCachingObjectService(
        TCachingObjectServiceConfigPtr config,
        IInvokerPtr invoker,
        IChannelPtr masterChannel,
        TObjectServiceCachePtr cache,
        TRealmId masterCellId,
        NLogging::TLogger logger)
        : TServiceBase(
            std::move(invoker),
            TObjectServiceProxy::GetDescriptor(),
            logger,
            masterCellId)
        , Config_(config)
        , Cache_(std::move(cache))
        , CellId_(masterCellId)
        , MasterChannel_(CreateThrottlingChannel(
            config,
            masterChannel))
        , Logger(logger.WithTag("RealmId: %v", masterCellId))
        , CacheTtlRatio_(Config_->CacheTtlRatio)
        , EntryByteRateLimit_(Config_->EntryByteRateLimit)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(10000)
            .SetConcurrencyLimit(10000));
    }

    virtual void Reconfigure(const TCachingObjectServiceDynamicConfigPtr& config) override
    {
        MasterChannel_->Reconfigure(config);
        CacheTtlRatio_.store(config->CacheTtlRatio.value_or(Config_->CacheTtlRatio));
        EntryByteRateLimit_.store(config->EntryByteRateLimit.value_or(Config_->EntryByteRateLimit));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    class TMasterRequest
        : public TRefCounted
    {
    public:
        TMasterRequest(
            IChannelPtr channel,
            TCtxExecutePtr context,
            const NLogging::TLogger& logger)
            : Context_(std::move(context))
            , Logger(logger)
            , Proxy_(std::move(channel))
        {
            Request_ = Proxy_.Execute();
            SetAuthenticationIdentity(Request_, Context_->GetAuthenticationIdentity());
            MergeRequestHeaderExtensions(&Request_->Header(), Context_->RequestHeader());
        }

        TFuture<TSubrequestResponse> Add(TSharedRefArray subrequestMessage)
        {
            Request_->add_part_counts(subrequestMessage.Size());
            Request_->Attachments().insert(
                Request_->Attachments().end(),
                subrequestMessage.Begin(),
                subrequestMessage.End());

            auto promise = NewPromise<TSubrequestResponse>();
            Promises_.push_back(promise);
            return promise;
        }

        void Invoke()
        {
            YT_LOG_DEBUG("Running cache bypass request (RequestId: %v, SubrequestCount: %v)",
                Context_->GetRequestId(),
                Promises_.size());
            Request_->Invoke().Subscribe(BIND(&TMasterRequest::OnResponse, MakeStrong(this)));
        }

    private:
        const TCtxExecutePtr Context_;
        const NLogging::TLogger Logger;

        TObjectServiceProxy Proxy_;
        TObjectServiceProxy::TReqExecutePtr Request_;
        std::vector<TPromise<TSubrequestResponse>> Promises_;


        void OnResponse(const TObjectServiceProxy::TErrorOrRspExecutePtr& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                YT_LOG_DEBUG("Cache bypass request failed (RequestId: %v)",
                    Context_->GetRequestId());
                for (auto& promise : Promises_) {
                    promise.Set(rspOrError);
                }
                return;
            }

            YT_LOG_DEBUG("Cache bypass request succeeded (RequestId: %v)",
                Context_->GetRequestId());

            const auto& rsp = rspOrError.Value();
            YT_VERIFY(rsp->part_counts_size() == Promises_.size());

            int attachmentIndex = 0;
            const auto& attachments = rsp->Attachments();
            for (int subresponseIndex = 0; subresponseIndex < rsp->part_counts_size(); ++subresponseIndex) {
                int partCount = rsp->part_counts(subresponseIndex);
                auto parts = std::vector<TSharedRef>(
                    attachments.begin() + attachmentIndex,
                    attachments.begin() + attachmentIndex + partCount);
                Promises_[subresponseIndex].Set({
                    TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{}),
                    NHydra::NullRevision,
                    1.0
                });
                attachmentIndex += partCount;
            }
        }
    };

    const TCachingObjectServiceConfigPtr Config_;
    const TObjectServiceCachePtr Cache_;
    const TCellId CellId_;
    const IThrottlingChannelPtr MasterChannel_;
    const NLogging::TLogger Logger;

    std::atomic<double> CacheTtlRatio_;
    std::atomic<i64> EntryByteRateLimit_;

    std::atomic<bool> CachingEnabled_ = false;
};

DEFINE_RPC_SERVICE_METHOD(TCachingObjectService, Execute)
{
    auto requestId = context->GetRequestId();

    context->SetRequestInfo("RequestCount: %v",
        request->part_counts_size());

    int attachmentIndex = 0;
    const auto& attachments = request->Attachments();

    std::vector<TFuture<TSubrequestResponse>> asyncMasterResponseMessages;
    TIntrusivePtr<TMasterRequest> masterRequest;

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

        TObjectServiceCacheKey key(
            CellTagFromId(CellId_),
            context->GetAuthenticationIdentity().User,
            ypathExt.target_path(),
            subrequestHeader.service(),
            subrequestHeader.method(),
            subrequestMessage[1]);

        if (subrequestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
            const auto& cachingRequestHeaderExt = subrequestHeader.GetExtension(TCachingHeaderExt::caching_header_ext);
            auto refreshRevision = cachingRequestHeaderExt.refresh_revision();

            if (ypathExt.mutating()) {
                THROW_ERROR_EXCEPTION("Cannot cache responses for mutating requests");
            }

            if (subrequestMessage.Size() > 2) {
                THROW_ERROR_EXCEPTION("Cannot cache responses for requests with attachments");
            }

            YT_LOG_DEBUG("Serving subrequest from cache (RequestId: %v, SubrequestIndex: %v, Key: %v)",
                requestId,
                subrequestIndex,
                key);

            auto successExpirationTime = FromProto<TDuration>(cachingRequestHeaderExt.success_expiration_time());
            auto failureExpirationTime = FromProto<TDuration>(cachingRequestHeaderExt.failure_expiration_time());

            auto cacheTtlRatio = CacheTtlRatio_.load();
            auto nodeSuccessExpirationTime = successExpirationTime * cacheTtlRatio;
            auto nodeFailureExpirationTime = failureExpirationTime * cacheTtlRatio;

            bool cachingEnabled = CachingEnabled_.load(std::memory_order_relaxed);
            auto cookie = Cache_->BeginLookup(
                requestId,
                key,
                cachingEnabled ? nodeSuccessExpirationTime : successExpirationTime,
                cachingEnabled ? nodeFailureExpirationTime : failureExpirationTime,
                refreshRevision);

            asyncMasterResponseMessages.push_back(
                cookie.GetValue().Apply(BIND([] (const TObjectServiceCacheEntryPtr& entry) -> TSubrequestResponse {
                    return {entry->GetResponseMessage(), entry->GetRevision(), entry->GetByteRate()};
                })));

            if (cookie.IsActive()) {
                TObjectServiceProxy proxy(MasterChannel_);
                auto req = proxy.Execute();
                req->set_suppress_upstream_sync(true);
                req->add_part_counts(subrequestMessage.Size());
                req->Attachments().insert(
                    req->Attachments().end(),
                    subrequestMessage.Begin(),
                    subrequestMessage.End());
                SetCurrentAuthenticationIdentity(req);

                if (cachingEnabled) {
                    auto* balancingHeaderExt = req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
                    balancingHeaderExt->set_enable_stickiness(true);
                    balancingHeaderExt->set_sticky_group_size(1);

                    auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
                    cachingHeaderExt->set_success_expiration_time(ToProto<i64>(successExpirationTime - nodeSuccessExpirationTime));
                    cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(failureExpirationTime - nodeFailureExpirationTime));
                } else {
                    req->Header().ClearExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
                    req->Header().ClearExtension(NRpc::NProto::TBalancingExt::balancing_ext);
                }

                req->Invoke().Apply(
                    BIND([this, this_ = MakeStrong(this), cookie = std::move(cookie), requestId] (
                        const TObjectServiceProxy::TErrorOrRspExecutePtr& rspOrError) mutable
                    {
                        if (!rspOrError.IsOK()) {
                            YT_LOG_WARNING(rspOrError, "Cache population request failed (Key: %v)", cookie.GetKey());
                            cookie.Cancel(rspOrError);
                            return;
                        }

                        const auto& rsp = rspOrError.Value();
                        YT_VERIFY(rsp->part_counts_size() == 1);
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
                    }));
            }
        } else {
            YT_LOG_DEBUG("Subrequest does not support caching, bypassing cache (RequestId: %v, SubrequestIndex: %v, Key: %v)",
                requestId,
                subrequestIndex,
                key);

            if (!masterRequest) {
                masterRequest = New<TMasterRequest>(MasterChannel_, context, Logger);
            }

            asyncMasterResponseMessages.push_back(masterRequest->Add(subrequestMessage));
        }
    }

    if (masterRequest) {
        masterRequest->Invoke();
    }

    AllSucceeded(asyncMasterResponseMessages)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TSubrequestResponse>>& masterResponseMessagesOrError) {
            if (!masterResponseMessagesOrError.IsOK()) {
                context->Reply(masterResponseMessagesOrError);
                return;
            }

            const auto& masterResponseMessages = masterResponseMessagesOrError.Value();

            auto& responseAttachments = response->Attachments();
            for (int subrequestIndex = 0; subrequestIndex < request->part_counts_size(); ++subrequestIndex) {
                const auto& subrequestResponse = masterResponseMessages[subrequestIndex];
                if (request->has_current_sticky_group_size()) {
                    auto currentStickyGroupSize = request->current_sticky_group_size();
                    auto totalSize = subrequestResponse.ByteRate * currentStickyGroupSize;
                    auto advisedStickyGroupSize = 1 + static_cast<int>(totalSize / EntryByteRateLimit_.load());
                    response->add_advised_sticky_group_size(advisedStickyGroupSize);
                }
                const auto& masterResponseMessage = subrequestResponse.Message;
                response->add_part_counts(masterResponseMessage.Size());
                responseAttachments.insert(
                    responseAttachments.end(),
                    masterResponseMessage.Begin(),
                    masterResponseMessage.End());
            }

            for (const auto& [message, revision, rate] : masterResponseMessages) {
                if (revision == NHydra::NullRevision) {
                    response->clear_revisions();
                    break;
                }
                response->add_revisions(revision);
            }

            response->set_caching_enabled(true);

            context->Reply();
        }));
}

ICachingObjectServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    IChannelPtr masterChannel,
    TObjectServiceCachePtr cache,
    TRealmId masterCellId,
    NLogging::TLogger logger)
{
    return New<TCachingObjectService>(
        std::move(config),
        std::move(invoker),
        std::move(masterChannel),
        std::move(cache),
        masterCellId,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

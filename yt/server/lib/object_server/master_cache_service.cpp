#include "config.h"
#include "master_cache_service.h"
#include "object_service_cache.h"
#include "private.h"

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/checksum.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/throttling_channel.h>

#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NObjectServer {

using namespace NConcurrency;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

struct TSubrequestResponse
{
    TSharedRefArray Message;
    NHydra::TRevision Revision;
};

class TMasterCacheService
    : public TServiceBase
{
public:
    TMasterCacheService(
        TMasterCacheServiceConfigPtr config,
        IInvokerPtr invoker,
        IChannelPtr masterChannel,
        TObjectServiceCachePtr cache,
        TRealmId masterCellId)
        : TServiceBase(
            std::move(invoker),
            TObjectServiceProxy::GetDescriptor(),
            ObjectServerLogger,
            masterCellId)
        , Config_(config)
        , Cache_(std::move(cache))
        , CellId_(masterCellId)
        , MasterChannel_(CreateThrottlingChannel(
            config,
            masterChannel))
        , Logger(NLogging::TLogger(ObjectServerLogger)
            .AddTag("RealmId: %v", masterCellId))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    class TMasterRequest
        : public TIntrinsicRefCounted
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
            Request_->SetUser(Context_->GetUser());
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
                    NHydra::NullRevision});
                attachmentIndex += partCount;
            }
        }
    };

    const TMasterCacheServiceConfigPtr Config_;
    const TObjectServiceCachePtr Cache_;
    const TCellId CellId_;
    const IChannelPtr MasterChannel_;
    const NLogging::TLogger Logger;

    std::atomic<bool> EnableTwoLevelObjectServiceCache_ = {false};
};

DEFINE_RPC_SERVICE_METHOD(TMasterCacheService, Execute)
{
    auto requestId = context->GetRequestId();

    context->SetRequestInfo("RequestCount: %v",
        request->part_counts_size());

    const auto& user = context->GetUser();

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
            user,
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

            auto nodeSuccessExpirationTime = successExpirationTime * Config_->NodeCacheTtlRatio;
            auto nodeFailureExpirationTime = failureExpirationTime * Config_->NodeCacheTtlRatio;

            bool enableTwoLevelObjectServiceCache = EnableTwoLevelObjectServiceCache_.load(std::memory_order_relaxed);
            auto cookie = Cache_->BeginLookup(
                requestId,
                key,
                enableTwoLevelObjectServiceCache ? nodeSuccessExpirationTime : successExpirationTime,
                enableTwoLevelObjectServiceCache ? nodeFailureExpirationTime : failureExpirationTime,
                refreshRevision);

            asyncMasterResponseMessages.push_back(
                cookie.GetValue().Apply(BIND([] (const TObjectServiceCacheEntryPtr& entry) -> TSubrequestResponse {
                    return {entry->GetResponseMessage(), entry->GetRevision()};
                })));

            if (cookie.IsActive()) {
                TObjectServiceProxy proxy(MasterChannel_);
                auto req = proxy.Execute();
                req->SetUser(key.User);
                req->add_part_counts(subrequestMessage.Size());
                req->Attachments().insert(
                    req->Attachments().end(),
                    subrequestMessage.Begin(),
                    subrequestMessage.End());

                if (enableTwoLevelObjectServiceCache) {
                    auto* balancingHeaderExt = req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
                    balancingHeaderExt->set_enable_stickness(true);
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
                        YT_VERIFY(ParseResponseHeader(responseMessage, &responseHeader));
                        auto responseError = FromProto<TError>(responseHeader.error());
                        auto revision = rsp->revisions_size() > 0 ? rsp->revisions(0) : NHydra::NullRevision;

                        bool enableTwoLevelCache = rsp->two_level_cache_enabled();
                        if (EnableTwoLevelObjectServiceCache_.exchange(enableTwoLevelCache) != enableTwoLevelCache) {
                            YT_LOG_INFO("Changing two-level object service cache mode (Enable: %v)", enableTwoLevelCache);
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

    auto masterResponseMessages = WaitFor(Combine(asyncMasterResponseMessages))
        .ValueOrThrow();

    auto& responseAttachments = response->Attachments();
    for (const auto& subrequestResponse : masterResponseMessages) {
        const auto& masterResponseMessage = subrequestResponse.Message;
        response->add_part_counts(masterResponseMessage.Size());
        responseAttachments.insert(
            responseAttachments.end(),
            masterResponseMessage.Begin(),
            masterResponseMessage.End());
    }

    for (const auto& [message, revision] : masterResponseMessages) {
        if (revision == NHydra::NullRevision) {
            response->clear_revisions();
            break;
        }
        response->add_revisions(revision);
    }

    context->Reply();
}

IServicePtr CreateMasterCacheService(
    TMasterCacheServiceConfigPtr config,
    IInvokerPtr invoker,
    IChannelPtr masterChannel,
    TObjectServiceCachePtr cache,
    TRealmId masterCellId)
{
    return New<TMasterCacheService>(
        std::move(config),
        std::move(invoker),
        std::move(masterChannel),
        std::move(cache),
        masterCellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

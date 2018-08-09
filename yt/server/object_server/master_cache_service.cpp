#include "master_cache_service.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/checksum.h>

#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/throttling_channel.h>

#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT {
namespace NObjectServer {

using namespace NConcurrency;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NObjectClient;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheService
    : public TServiceBase
{
public:
    TMasterCacheService(
        TMasterCacheServiceConfigPtr config,
        IChannelPtr masterChannel,
        const TRealmId& masterCellId)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            TObjectServiceProxy::GetDescriptor(),
            ObjectServerLogger,
            masterCellId)
        , Config_(config)
        , MasterChannel_(CreateThrottlingChannel(
            config,
            masterChannel))
        , Logger(NLogging::TLogger(ObjectServerLogger)
            .AddTag("RealmId: %v", masterCellId))
        , Cache_(New<TCache>(this))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    struct TKey
    {
        TString User;
        TYPath Path;
        TString Service;
        TString Method;
        TSharedRef RequestBody;
        size_t RequestBodyHash;

        TKey(
            const TString& user,
            const TYPath& path,
            const TString& service,
            const TString& method,
            const TSharedRef& requestBody)
            : User(user)
            , Path(path)
            , Method(method)
            , RequestBody(requestBody)
            , RequestBodyHash(GetChecksum(RequestBody))
        { }

        operator size_t() const
        {
            size_t result = 0;
            HashCombine(result, User);
            HashCombine(result, Path);
            HashCombine(result, Service);
            HashCombine(result, Method);
            HashCombine(result, RequestBodyHash);
            return result;
        }

        bool operator == (const TKey& other) const
        {
            return
                User == other.User &&
                Path == other.Path &&
                Service == other.Service &&
                Method == other.Method &&
                RequestBodyHash == other.RequestBodyHash &&
                TRef::AreBitwiseEqual(RequestBody, other.RequestBody);
        }

        friend TString ToString(const TKey& key)
        {
            return Format("{%v %v:%v %v %x}",
                key.User,
                key.Service,
                key.Method,
                key.Path,
                key.RequestBodyHash);
        }
    };

    class TEntry
        : public TAsyncCacheValueBase<TKey, TEntry>
    {
    public:
        TEntry(
            const TKey& key,
            bool success,
            TInstant timestamp,
            TSharedRefArray responseMessage)
            : TAsyncCacheValueBase(key)
            , Success_(success)
            , ResponseMessage_(std::move(responseMessage))
            , TotalSpace_(GetByteSize(ResponseMessage_))
            , Timestamp_(timestamp)
        { }

        DEFINE_BYVAL_RO_PROPERTY(bool, Success);
        DEFINE_BYVAL_RO_PROPERTY(TSharedRefArray, ResponseMessage);
        DEFINE_BYVAL_RO_PROPERTY(i64, TotalSpace);
        DEFINE_BYVAL_RO_PROPERTY(TInstant, Timestamp);
    };

    typedef TIntrusivePtr<TEntry> TEntryPtr;

    class TCache
        : public TAsyncSlruCacheBase<TKey, TEntry>
    {
    public:
        explicit TCache(TMasterCacheService* owner)
            : TAsyncSlruCacheBase(
                owner->Config_,
                NProfiling::TProfiler(ObjectServerProfiler.GetPathPrefix() + "/master_cache"))
            , Owner_(owner)
            , Logger(owner->Logger)
        { }

        TFuture<TSharedRefArray> Lookup(
            const TKey& key,
            TSharedRefArray requestMessage,
            TDuration successExpirationTime,
            TDuration failureExpirationTime)
        {
            auto entry = Find(key);
            if (entry) {
                if (!IsExpired(entry, successExpirationTime, failureExpirationTime)) {
                    LOG_DEBUG("Cache hit (Key: %v, Success: %v, SuccessExpirationTime: %v, FailureExpirationTime: %v)",
                        key,
                        entry->GetSuccess(),
                        successExpirationTime,
                        failureExpirationTime);
                    return MakeFuture(TErrorOr<TSharedRefArray>(entry->GetResponseMessage()));
                }

                LOG_DEBUG("Cache entry expired (Key: %v, Success: %v, SuccessExpirationTime: %v, FailureExpirationTime: %v)",
                    key,
                    entry->GetSuccess(),
                    successExpirationTime,
                    failureExpirationTime);

                TryRemove(entry);
            }

            auto cookie = BeginInsert(key);
            auto result = cookie.GetValue();
            if (cookie.IsActive()) {
                LOG_DEBUG("Populating cache (Key: %v)",
                    key);

                TObjectServiceProxy proxy(Owner_->MasterChannel_);
                auto req = proxy.Execute();
                req->SetUser(key.User);
                req->add_part_counts(requestMessage.Size());
                req->Attachments().insert(
                    req->Attachments().end(),
                    requestMessage.Begin(),
                    requestMessage.End());

                req->Invoke().Subscribe(BIND(
                    &TCache::OnResponse,
                    MakeStrong(this),
                    Passed(std::move(cookie))));
            }

            return result.Apply(BIND([] (const TEntryPtr& entry) -> TSharedRefArray {
                return entry->GetResponseMessage();
            }));
        }

    private:
        TMasterCacheService* const Owner_;
        const NLogging::TLogger Logger;


        virtual void OnAdded(const TEntryPtr& entry) override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TAsyncSlruCacheBase::OnAdded(entry);

            const auto& key = entry->GetKey();
            LOG_DEBUG("Cache entry added (Key: %v, Success: %v, TotalSpace: %v)",
                key,
                entry->GetSuccess(),
                entry->GetTotalSpace());
        }

        virtual void OnRemoved(const TEntryPtr& entry) override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TAsyncSlruCacheBase::OnRemoved(entry);

            const auto& key = entry->GetKey();
            LOG_DEBUG("Cache entry removed (Key: %v, Success: %v, TotalSpace: %v)",
                key,
                entry->GetSuccess(),
                entry->GetTotalSpace());
        }

        virtual i64 GetWeight(const TEntryPtr& entry) const override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            return entry->GetTotalSpace();
        }


        static bool IsExpired(
            TEntryPtr entry,
            TDuration successExpirationTime,
            TDuration failureExpirationTime)
        {
            return
                TInstant::Now() > entry->GetTimestamp() +
                (entry->GetSuccess() ? successExpirationTime : failureExpirationTime);
        }


        void OnResponse(
            TInsertCookie cookie,
            const TObjectServiceProxy::TErrorOrRspExecutePtr& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                LOG_WARNING(rspOrError, "Cache population request failed");
                cookie.Cancel(rspOrError);
                return;
            }

            const auto& rsp = rspOrError.Value();
            const auto& key = cookie.GetKey();

            YCHECK(rsp->part_counts_size() == 1);
            auto responseMessage = TSharedRefArray(rsp->Attachments());

            TResponseHeader responseHeader;
            YCHECK(ParseResponseHeader(responseMessage, &responseHeader));
            auto responseError = FromProto<TError>(responseHeader.error());

            LOG_DEBUG("Cache population request succeeded (Key: %v, Error: %v)",
                key,
                responseError);

            auto entry = New<TEntry>(
                key,
                responseError.IsOK(),
                TInstant::Now(),
                responseMessage);
            cookie.EndInsert(entry);
        }
    };

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

        TFuture<TSharedRefArray> Add(TSharedRefArray subrequestMessage)
        {
            Request_->add_part_counts(subrequestMessage.Size());
            Request_->Attachments().insert(
                Request_->Attachments().end(),
                subrequestMessage.Begin(),
                subrequestMessage.End());

            auto promise = NewPromise<TSharedRefArray>();
            Promises_.push_back(promise);
            return promise;
        }

        void Invoke()
        {
            LOG_DEBUG("Running cache bypass request (RequestId: %v, SubrequestCount: %v)",
                Context_->GetRequestId(),
                Promises_.size());
            Request_->Invoke().Subscribe(BIND(&TMasterRequest::OnResponse, MakeStrong(this)));
        }

    private:
        const TCtxExecutePtr Context_;
        const NLogging::TLogger Logger;

        TObjectServiceProxy Proxy_;
        TObjectServiceProxy::TReqExecutePtr Request_;
        std::vector<TPromise<TSharedRefArray>> Promises_;


        void OnResponse(const TObjectServiceProxy::TErrorOrRspExecutePtr& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                LOG_DEBUG("Cache bypass request failed (RequestId: %v)",
                    Context_->GetRequestId());
                for (auto& promise : Promises_) {
                    promise.Set(rspOrError);
                }
                return;
            }

            LOG_DEBUG("Cache bypass request succeeded (RequestId: %v)",
                Context_->GetRequestId());

            const auto& rsp = rspOrError.Value();
            YCHECK(rsp->part_counts_size() == Promises_.size());

            int attachmentIndex = 0;
            const auto& attachments = rsp->Attachments();
            for (int subresponseIndex = 0; subresponseIndex < rsp->part_counts_size(); ++subresponseIndex) {
                int partCount = rsp->part_counts(subresponseIndex);
                auto parts = std::vector<TSharedRef>(
                    attachments.begin() + attachmentIndex,
                    attachments.begin() + attachmentIndex + partCount);
                Promises_[subresponseIndex].Set(TSharedRefArray(std::move(parts)));
                attachmentIndex += partCount;
            }
        }
    };

    const TMasterCacheServiceConfigPtr Config_;
    const IChannelPtr MasterChannel_;
    const NLogging::TLogger Logger;
    const TIntrusivePtr<TCache> Cache_;
};

DEFINE_RPC_SERVICE_METHOD(TMasterCacheService, Execute)
{
    const auto& requestId = context->GetRequestId();

    context->SetRequestInfo("RequestCount: %v",
        request->part_counts_size());

    const auto& user = context->GetUser();

    int attachmentIndex = 0;
    const auto& attachments = request->Attachments();

    std::vector<TFuture<TSharedRefArray>> asyncMasterResponseMessages;
    TIntrusivePtr<TMasterRequest> masterRequest;

    for (int subrequestIndex = 0; subrequestIndex < request->part_counts_size(); ++subrequestIndex) {
        int partCount = request->part_counts(subrequestIndex);
        auto subrequestParts = std::vector<TSharedRef>(
            attachments.begin() + attachmentIndex,
            attachments.begin() + attachmentIndex + partCount);
        auto subrequestMessage = TSharedRefArray(std::move(subrequestParts));
        attachmentIndex += partCount;

        if (subrequestMessage.Size() < 2) {
            THROW_ERROR_EXCEPTION("Malformed subrequest message: at least two parts are expected");
        }

        TRequestHeader subrequestHeader;
        if (!ParseRequestHeader(subrequestMessage, &subrequestHeader)) {
            THROW_ERROR_EXCEPTION("Malformed subrequest message: failed to parse header");
        }

        const auto& ypathExt = subrequestHeader.GetExtension(TYPathHeaderExt::ypath_header_ext);

        TKey key(
            user,
            ypathExt.path(),
            subrequestHeader.service(),
            subrequestHeader.method(),
            subrequestMessage[1]);

        if (subrequestHeader.HasExtension(TCachingHeaderExt::caching_header_ext)) {
            const auto& cachingRequestHeaderExt = subrequestHeader.GetExtension(TCachingHeaderExt::caching_header_ext);

            if (ypathExt.mutating()) {
                THROW_ERROR_EXCEPTION("Cannot cache responses for mutating requests");
            }

            if (subrequestMessage.Size() > 2) {
                THROW_ERROR_EXCEPTION("Cannot cache responses for requests with attachments");
            }

            LOG_DEBUG("Serving subrequest from cache (RequestId: %v, SubrequestIndex: %v, Key: %v)",
                requestId,
                subrequestIndex,
                key);

            asyncMasterResponseMessages.push_back(Cache_->Lookup(
                key,
                std::move(subrequestMessage),
                FromProto<TDuration>(cachingRequestHeaderExt.success_expiration_time()),
                FromProto<TDuration>(cachingRequestHeaderExt.failure_expiration_time())));
        } else {
            LOG_DEBUG("Subrequest does not support caching, bypassing cache (RequestId: %v, SubrequestIndex: %v, Key: %v)",
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
    for (const auto& masterResponseMessage : masterResponseMessages) {
        response->add_part_counts(masterResponseMessage.Size());
        responseAttachments.insert(
            responseAttachments.end(),
            masterResponseMessage.Begin(),
            masterResponseMessage.End());
    }

    context->Reply();
}

IServicePtr CreateMasterCacheService(
    TMasterCacheServiceConfigPtr config,
    IChannelPtr masterChannel,
    const TRealmId& masterCellId)
{
    return New<TMasterCacheService>(
        std::move(config),
        std::move(masterChannel),
        masterCellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

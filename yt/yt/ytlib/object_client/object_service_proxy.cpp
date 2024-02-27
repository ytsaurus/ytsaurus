#include "object_service_proxy.h"

#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/helpers.h>

#include <utility>

namespace NYT::NObjectClient {

using namespace NYTree;
using namespace NObjectClient;
using namespace NRpc;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectClientLogger;

static const auto ExecuteMethodDescriptor = TMethodDescriptor("Execute");

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TObjectServiceProxy(
    IClientPtr client,
    NApi::EMasterChannelKind masterChannelKind,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TProxyBase(
        client->GetCypressChannelOrThrow(masterChannelKind, cellTag),
        GetDescriptor())
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , CellTag_(cellTag)
    , ChannelKind_(masterChannelKind)
{ }

TObjectServiceProxy::TObjectServiceProxy(
    IConnectionPtr connection,
    NApi::EMasterChannelKind masterChannelKind,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
    : TProxyBase(
        connection->GetCypressChannelOrThrow(masterChannelKind, cellTag),
        GetDescriptor())
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , CellTag_(cellTag)
    , ChannelKind_(masterChannelKind)
{ }

TObjectServiceProxy::TObjectServiceProxy(
    IConnectionPtr connection,
    NApi::EMasterChannelKind masterChannelKind,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    TAuthenticationIdentity identity)
    : TProxyBase(
        CreateAuthenticatedChannel(
            connection->GetCypressChannelOrThrow(masterChannelKind, cellTag),
            identity),
        GetDescriptor())
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , CellTag_(cellTag)
    , ChannelKind_(masterChannelKind)
{ }

TObjectServiceProxy TObjectServiceProxy::FromDirectMasterChannel(IChannelPtr channel)
{
    return TObjectServiceProxy(std::move(channel));
}

TStickyGroupSizeCache::TKey TObjectServiceProxy::TReqExecuteSubbatch::TInnerRequestDescriptor::GetKey() const
{
    return {Key, Message};
}

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(
    IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    std::optional<TCellTag> cellTag,
    std::optional<NApi::EMasterChannelKind> channelKind)
    : TClientRequest(
        std::move(channel),
        TObjectServiceProxy::GetDescriptor(),
        ExecuteMethodDescriptor)
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , SubbatchSize_(subbatchSize)
    , CellTag_(cellTag)
    , ChannelKind_(channelKind)
{
    SetResponseHeavy(true);
}

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(
    const TReqExecuteSubbatch& other,
    std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors)
    : TClientRequest(other)
    , StickyGroupSizeCache_(other.StickyGroupSizeCache_)
    , SubbatchSize_(other.SubbatchSize_)
    , CellTag_(other.CellTag_)
    , ChannelKind_(other.ChannelKind_)
    , InnerRequestDescriptors_(std::move(innerRequestDescriptors))
    , OriginalRequestId_(other.OriginalRequestId_)
    , SuppressUpstreamSync_(other.SuppressUpstreamSync_)
    , SuppressTransactionCoordinatorSync_(other.SuppressTransactionCoordinatorSync_)
{
    // Undo some work done by the base class's copy ctor and make some tweaks.
    // XXX(babenko): refactor this, maybe avoid TClientRequest copying.
    Attachments().clear();
    ToProto(Header().mutable_request_id(), TRequestId::Create());
}

int TObjectServiceProxy::TReqExecuteSubbatch::GetSize() const
{
    return static_cast<int>(InnerRequestDescriptors_.size());
}

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteSubbatch::DoInvoke()
{
    // Prepare attachments.
    for (const auto& descriptor : InnerRequestDescriptors_) {
        const auto& message = descriptor.Message;
        if (message) {
            Attachments_.insert(Attachments_.end(), message.Begin(), message.End());
        }
    }

    auto batchRsp = New<TRspExecuteBatch>(CreateClientContext(), InnerRequestDescriptors_, StickyGroupSizeCache_);
    auto promise = batchRsp->GetPromise();
    if (GetSize() == 0) {
        batchRsp->SetEmpty();
    } else {
        auto requestControl = Send(batchRsp);
        promise.OnCanceled(BIND([=] (const TError& /*error*/) {
            requestControl->Cancel();
        }));
    }
    return promise;
}

TSharedRefArray TObjectServiceProxy::TReqExecuteSubbatch::SerializeHeaderless() const
{
    NProto::TReqExecute req;
    if (OriginalRequestId_) {
        ToProto(req.mutable_original_request_id(), OriginalRequestId_);
    }

    // COMPAT(shakurov): remove in favor of proto ext (see SetMulticellSyncHeader).
    req.set_suppress_upstream_sync(SuppressUpstreamSync_);
    req.set_suppress_transaction_coordinator_sync(SuppressTransactionCoordinatorSync_);

    req.set_allow_backoff(true);
    req.set_supports_portals(true);

    if (CellTag_) {
        req.set_cell_tag(ToProto<int>(*CellTag_));
    }
    if (ChannelKind_) {
        req.set_peer_kind(ToProto<int>(*ChannelKind_));
    }

    if (Header().HasExtension(NRpc::NProto::TBalancingExt::balancing_ext)) {
        auto currentStickyGroupSize = Header().GetExtension(NRpc::NProto::TBalancingExt::balancing_ext).sticky_group_size();
        req.set_current_sticky_group_size(currentStickyGroupSize);
    }

    for (const auto& descriptor : InnerRequestDescriptors_) {
        req.add_part_counts(descriptor.Message.Size());
    }

    // COMPAT(danilalexeev): legacy RPC codecs.
    auto body = EnableLegacyRpcCodecs_
        ? SerializeProtoToRefWithEnvelope(req, RequestCodec_)
        : SerializeProtoToRefWithCompression(req, RequestCodec_);

    std::vector<TSharedRef> data;
    data.reserve(Attachments_.size() + 1);
    data.push_back(std::move(body));
    data.insert(data.end(), Attachments_.begin(), Attachments_.end());

    return TSharedRefArray(std::move(data), TSharedRefArray::TMoveParts{});
}

size_t TObjectServiceProxy::TReqExecuteSubbatch::ComputeHash() const
{
    size_t hash = 0;
    HashCombine(hash, SuppressUpstreamSync_);
    HashCombine(hash, SuppressTransactionCoordinatorSync_);
    for (const auto& descriptor : InnerRequestDescriptors_) {
        if (descriptor.Hash) {
            HashCombine(hash, descriptor.Hash);
        } else {
            for (auto* it = descriptor.Message.Begin(); it != descriptor.Message.End(); ++it) {
                HashCombine(hash, GetChecksum(*it));
            }
        }
    }
    return hash;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchBase::TReqExecuteBatchBase(
    IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    std::optional<TCellTag> cellTag,
    std::optional<NApi::EMasterChannelKind> channelKind)
    : TReqExecuteSubbatch(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache),
        cellTag,
        channelKind)
{ }

TObjectServiceProxy::TReqExecuteBatchBase::TReqExecuteBatchBase(
    const TReqExecuteBatchBase& other,
    std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors)
    : TReqExecuteSubbatch(other, std::move(innerRequestDescriptors))
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatchBase::Invoke()
{
    PushDownPrerequisites();
    return DoInvoke();
}

void TObjectServiceProxy::TReqExecuteBatchBase::SetOriginalRequestId(TRequestId originalRequestId)
{
    OriginalRequestId_ = originalRequestId;
}

void TObjectServiceProxy::TReqExecuteBatchBase::SetSuppressUpstreamSync(bool value)
{
    SuppressUpstreamSync_ = value;
}

void TObjectServiceProxy::TReqExecuteBatchBase::SetSuppressTransactionCoordinatorSync(bool value)
{
    SuppressTransactionCoordinatorSync_ = value;
}

void TObjectServiceProxy::TReqExecuteBatchBase::AddRequest(
    const TYPathRequestPtr& innerRequest,
    std::optional<TString> key,
    std::optional<size_t> hash)
{
    InnerRequestDescriptors_.push_back({
        std::move(key),
        innerRequest->Tag(),
        innerRequest->Serialize(),
        hash
    });
}

void TObjectServiceProxy::TReqExecuteBatchBase::AddRequestMessage(
    TSharedRefArray innerRequestMessage,
    std::optional<TString> key,
    std::any tag,
    std::optional<size_t> hash)
{
    InnerRequestDescriptors_.push_back({
        std::move(key),
        std::move(tag),
        std::move(innerRequestMessage),
        hash
    });
}

void TObjectServiceProxy::TReqExecuteBatchBase::PushDownPrerequisites()
{
    // Push TPrerequisitesExt down to individual requests.
    if (Header().HasExtension(NProto::TPrerequisitesExt::prerequisites_ext)) {
        const auto& batchPrerequisitesExt = Header().GetExtension(NProto::TPrerequisitesExt::prerequisites_ext);
        for (auto& descriptor : InnerRequestDescriptors_) {
            NRpc::NProto::TRequestHeader requestHeader;
            YT_VERIFY(ParseRequestHeader(descriptor.Message, &requestHeader));

            auto* prerequisitesExt = requestHeader.MutableExtension(NProto::TPrerequisitesExt::prerequisites_ext);
            prerequisitesExt->mutable_transactions()->MergeFrom(batchPrerequisitesExt.transactions());
            prerequisitesExt->mutable_revisions()->MergeFrom(batchPrerequisitesExt.revisions());
            descriptor.Message = SetRequestHeader(descriptor.Message, requestHeader);
        }
        Header().ClearExtension(NProto::TPrerequisitesExt::prerequisites_ext);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray TObjectServiceProxy::TReqExecuteBatch::PatchForRetry(const TSharedRefArray& message)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(ParseRequestHeader(message, &header));
    if (header.retry()) {
        // Already patched.
        return message;
    } else {
        header.set_retry(true);
        return SetRequestHeader(message, header);
    }
}

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatch::Invoke()
{
    SetBalancingHeader();
    FullResponsePromise_ = NewPromise<TRspExecuteBatchPtr>();
    PushDownPrerequisites();
    InvokeNextBatch();
    return FullResponsePromise_;
}

void TObjectServiceProxy::TReqExecuteBatch::SetStickyGroupSize(int value)
{
    StickyGroupSize_ = value;
}

void TObjectServiceProxy::TReqExecuteBatch::SetEnableClientStickiness(bool value)
{
    EnableClientStickiness_ = value;
}

TObjectServiceProxy::TObjectServiceProxy(IChannelPtr channel)
    : TProxyBase(std::move(channel), GetDescriptor())
{ }

TObjectServiceProxy::TReqExecuteBatch::TReqExecuteBatch(
    const TReqExecuteBatchBase& other,
    std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors)
    : TReqExecuteBatchBase(other, std::move(innerRequestDescriptors))
{ }

TObjectServiceProxy::TReqExecuteBatch::TReqExecuteBatch(
    IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    std::optional<TCellTag> cellTag,
    std::optional<NApi::EMasterChannelKind> channelKind)
    : TReqExecuteBatchBase(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache),
        cellTag,
        channelKind)
{ }

TObjectServiceProxy::TReqExecuteSubbatchPtr TObjectServiceProxy::TReqExecuteBatch::FormNextBatch()
{
    std::vector<TInnerRequestDescriptor> innerRequestDescriptors;
    innerRequestDescriptors.reserve(SubbatchSize_);

    for (auto i = GetFirstUnreceivedSubresponseIndex(); i < GetTotalSubrequestCount(); ++i) {
        if (IsSubresponseReceived(i)) {
            continue;
        }

        auto& descriptor = InnerRequestDescriptors_[i];
        if (IsSubresponseUncertain(i)) {
            descriptor.Message = PatchForRetry(descriptor.Message);
        }

        innerRequestDescriptors.push_back(descriptor);

        if (std::ssize(innerRequestDescriptors) == SubbatchSize_) {
            break;
        }
    }

    return New<TReqExecuteSubbatch>(*this, std::move(innerRequestDescriptors));
}

void TObjectServiceProxy::TReqExecuteBatch::InvokeNextBatch()
{
    // Optimization for the typical case of a small batch.
    if (IsFirstBatch_ && GetTotalSubrequestCount() <= SubbatchSize_) {
        CurrentReqFuture_ = DoInvoke();
    } else {
        auto subbatchReq = FormNextBatch();
        CurrentReqFuture_ = subbatchReq->DoInvoke();
        YT_LOG_DEBUG("Subbatch request invoked (BatchRequestId: %v, SubbatchRequestId: %v, SubbatchSize: %v)",
            GetRequestId(),
            subbatchReq->GetRequestId(),
            subbatchReq->GetSize());
    }

    CurrentReqFuture_.Subscribe(BIND(&TObjectServiceProxy::TReqExecuteBatch::OnSubbatchResponse, MakeStrong(this)));
}

TObjectServiceProxy::TRspExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatch::GetFullResponse()
{
    if (!FullResponse_) {
        // Make sure the full response uses the promise we've returned to the caller.
        FullResponse_ = New<TRspExecuteBatch>(
            CreateClientContext(),
            InnerRequestDescriptors_,
            StickyGroupSizeCache_,
            FullResponsePromise_);
    }
    return FullResponse_;
}

void TObjectServiceProxy::TReqExecuteBatch::OnSubbatchResponse(const TErrorOr<TRspExecuteBatchPtr>& rspOrErr)
{
    auto isFirstBatch = IsFirstBatch_;
    IsFirstBatch_ = false;

    if (!rspOrErr.IsOK()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    const auto& rsp = rspOrErr.Value();

    // Optimization for the typical case of a small batch.
    if (isFirstBatch && rsp->GetResponseCount() == GetTotalSubrequestCount()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    YT_LOG_DEBUG("Subbatch response received (BatchRequestId: %v, SubbatchRequestId: %v, SubbatchSubresponseCount: %v)",
        GetRequestId(),
        rsp->GetRequestId(),
        rsp->GetResponseCount());

    // The remote side shouldn't backoff until there's at least one subresponse.
    YT_VERIFY(rsp->GetResponseCount() > 0 || GetTotalSubrequestCount() == 0);

    auto fullResponse = GetFullResponse();
    auto globalIndex = fullResponse->GetFirstUnreceivedResponseIndex();
    for (auto i = 0; i < rsp->GetSize(); ++i) {
        YT_ASSERT(!fullResponse->IsResponseReceived(globalIndex));

        if (rsp->IsResponseReceived(i)) {
            auto revision = rsp->GetRevision(i);
            auto attachmentRange = rsp->GetResponseAttachmentRange(i);
            fullResponse->SetResponseReceived(globalIndex, revision, attachmentRange);
        } else if (rsp->IsResponseUncertain(i)) {
            fullResponse->SetResponseUncertain(globalIndex);
        }

        do {
            ++globalIndex;
        } while (globalIndex < GetTotalSubrequestCount() && fullResponse->IsResponseReceived(globalIndex));
    }

    if (GetFirstUnreceivedSubresponseIndex() == GetTotalSubrequestCount()) {
        GetFullResponse()->SetPromise({});
        return;
    }

    InvokeNextBatch();
}

int TObjectServiceProxy::TReqExecuteBatch::GetTotalSubrequestCount() const
{
    return GetSize();
}

int TObjectServiceProxy::TReqExecuteBatch::GetFirstUnreceivedSubresponseIndex() const
{
    return FullResponse_ ? FullResponse_->GetFirstUnreceivedResponseIndex() : 0;
}

bool TObjectServiceProxy::TReqExecuteBatch::IsSubresponseUncertain(int index) const
{
    return FullResponse_ ? FullResponse_->IsResponseUncertain(index) : false;
}

bool TObjectServiceProxy::TReqExecuteBatch::IsSubresponseReceived(int index) const
{
    return FullResponse_ ? FullResponse_->IsResponseReceived(index) : false;
}

void TObjectServiceProxy::TReqExecuteBatch::SetBalancingHeader()
{
    if (!StickyGroupSize_) {
        return;
    }

    auto* balancingHeaderExt = Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
    balancingHeaderExt->set_enable_stickiness(true);
    balancingHeaderExt->set_enable_client_stickiness(EnableClientStickiness_);

    auto stickyGroupSize = *StickyGroupSize_;
    if (auto advisedStickyGroupSize = GetAdvisedStickyGroupSize()) {
        stickyGroupSize = std::max(stickyGroupSize, *advisedStickyGroupSize);
        YT_LOG_DEBUG("Using advised sticky group size (RequestId: %v, ProvidedSize: %v, AdvisedSize: %v)",
            GetRequestId(),
            stickyGroupSize,
            *advisedStickyGroupSize);
    } else {
        YT_LOG_DEBUG("Not using advised sticky group size (RequestId: %v, ProvidedSize: %v)",
            GetRequestId(),
            stickyGroupSize);
    }
    balancingHeaderExt->set_sticky_group_size(stickyGroupSize);
}

void TObjectServiceProxy::TReqExecuteBatch::SetMulticellSyncHeader()
{
    NObjectClient::SetSuppressUpstreamSync(&Header(), SuppressUpstreamSync_);
    NObjectClient::SetSuppressTransactionCoordinatorSync(&Header(), SuppressTransactionCoordinatorSync_);
}

std::optional<int> TObjectServiceProxy::TReqExecuteBatch::GetAdvisedStickyGroupSize() const
{
    if (!StickyGroupSizeCache_) {
        return std::nullopt;
    }

    std::optional<int> advisedStickyGroupSize;

    for (const auto& descriptor : InnerRequestDescriptors_) {
        auto key = descriptor.GetKey();
        auto subrequestAdvisedStickyGroupSize = StickyGroupSizeCache_->GetAdvisedStickyGroupSize(key);
        if (!advisedStickyGroupSize) {
            advisedStickyGroupSize = subrequestAdvisedStickyGroupSize;
        } else if (subrequestAdvisedStickyGroupSize) {
            advisedStickyGroupSize = std::max(subrequestAdvisedStickyGroupSize, advisedStickyGroupSize);
        }
    }

    return advisedStickyGroupSize;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchWithRetries::TReqExecuteBatchWithRetries(
    IChannelPtr channel,
    TReqExecuteBatchWithRetriesConfigPtr config,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    std::optional<TCellTag> cellTag,
    std::optional<NApi::EMasterChannelKind> channelKind,
    TCallback<bool(int, const TError&)> needRetry,
    int subbatchSize)
    : TReqExecuteBatchBase(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache),
        cellTag,
        channelKind)
    , Config_(std::move(config))
    , NeedRetry_(BIND(std::move(needRetry), std::cref(CurrentRetry_)))
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatchWithRetries::Invoke()
{
    Initialize();
    InvokeNextBatch();
    return FullResponsePromise_;
}

void TObjectServiceProxy::TReqExecuteBatchWithRetries::Initialize()
{
    FullResponsePromise_ = NewPromise<TRspExecuteBatchPtr>();
    FullResponse_ = New<TRspExecuteBatch>(
        CreateClientContext(),
        InnerRequestDescriptors_,
        StickyGroupSizeCache_,
        FullResponsePromise_);

    // First batch contains all requests so fill in all the indexes.
    PendingIndexes_.resize(InnerRequestDescriptors_.size());
    std::iota(PendingIndexes_.begin(), PendingIndexes_.end(), 0);

    PushDownPrerequisites();
}

void TObjectServiceProxy::TReqExecuteBatchWithRetries::InvokeNextBatch()
{
    std::vector<TInnerRequestDescriptor> innerRequestDescriptors;
    innerRequestDescriptors.reserve(PendingIndexes_.size());

    for (int index : PendingIndexes_) {
        auto& descriptor = InnerRequestDescriptors_[index];
        descriptor.Message = PatchMutationId(descriptor.Message);
        innerRequestDescriptors.push_back(descriptor);
    }

    auto batchRequest = New<TReqExecuteBatch>(*this, std::move(innerRequestDescriptors));
    CurrentReqFuture_ = batchRequest->Invoke();
    YT_LOG_DEBUG("Batch attempt invoked (BatchRequestId: %v, AttemptRequestId: %v, RequestCount: %v)",
        GetRequestId(),
        batchRequest->GetRequestId(),
        batchRequest->GetSize());
    YT_UNUSED_FUTURE(CurrentReqFuture_.Apply(BIND(&TObjectServiceProxy::TReqExecuteBatchWithRetries::OnBatchResponse, MakeStrong(this))));
}

void TObjectServiceProxy::TReqExecuteBatchWithRetries::OnBatchResponse(const TErrorOr<TRspExecuteBatchPtr>& batchRspOrErr)
{
    if (!batchRspOrErr.IsOK()) {
        FullResponsePromise_.Set(batchRspOrErr);
        return;
    }

    const auto batchRsp = batchRspOrErr.Value();
    YT_VERIFY(batchRsp->GetResponseCount() == std::ssize(PendingIndexes_));
    YT_VERIFY(batchRsp->GetResponseCount() == batchRsp->GetSize());

    int retryCount = 0;
    for (int i = 0; i < batchRsp->GetSize(); ++i) {
        const auto& rspOrErr = batchRsp->GetResponse(i);
        if (CurrentRetry_ < Config_->RetryCount && NeedRetry_(rspOrErr)) {
            // Building new indexes vector in-place to avoid new allocations.
            PendingIndexes_[retryCount++] = PendingIndexes_[i];
        } else {
            auto revision = batchRsp->GetRevision(i);
            auto attachmentRange = batchRsp->GetResponseAttachmentRange(i);
            FullResponse_->SetResponseReceived(PendingIndexes_[i], revision, attachmentRange);
        }
    }

    if (retryCount == 0) {
        FullResponse_->SetPromise({});
        return;
    }

    PendingIndexes_.resize(retryCount);

    NConcurrency::TDelayedExecutor::Submit(
        BIND(&TObjectServiceProxy::TReqExecuteBatchWithRetries::OnRetryDelayFinished, MakeStrong(this)),
        GetCurrentDelay());
}

void TObjectServiceProxy::TReqExecuteBatchWithRetries::OnRetryDelayFinished()
{
    ++CurrentRetry_;
    InvokeNextBatch();
}

TSharedRefArray TObjectServiceProxy::TReqExecuteBatchWithRetries::PatchMutationId(const TSharedRefArray& message)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(ParseRequestHeader(message, &header));
    NRpc::SetMutationId(&header, GenerateMutationId(), false);
    return SetRequestHeader(message, header);
}

TDuration TObjectServiceProxy::TReqExecuteBatchWithRetries::GetCurrentDelay()
{
    YT_VERIFY(CurrentRetry_ < Config_->RetryCount);

    double backoffMultiplier = std::pow(Config_->BackoffMultiplier, CurrentRetry_);
    return std::min(Config_->StartBackoff * backoffMultiplier, Config_->MaxBackoff);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::vector<TReqExecuteSubbatch::TInnerRequestDescriptor>& innerRequestDescriptors,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    TPromise<TRspExecuteBatchPtr> promise)
    : TClientResponse(std::move(clientContext))
    , StickyGroupSizeCache_(std::move(stickyGroupSizeCache))
    , InnerResponseDescriptors_(innerRequestDescriptors.size())
    , InnerRequestDescriptors_(std::move(innerRequestDescriptors))
    , Promise_(promise ? std::move(promise) : NewPromise<TRspExecuteBatchPtr>())
{ }

TPromise<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TRspExecuteBatch::GetPromise()
{
    return Promise_;
}

void TObjectServiceProxy::TRspExecuteBatch::SetEmpty()
{
    NProto::TRspExecute body;
    auto message = CreateResponseMessage(body);
    static_cast<IClientResponseHandler*>(this)->HandleResponse(
        std::move(message),
        /*address*/ TString());
}

void TObjectServiceProxy::TRspExecuteBatch::SetPromise(const TError& error)
{
    if (error.IsOK()) {
        Promise_.Set(this);
    } else {
        Promise_.Set(error);
    }
    Promise_.Reset();
}

bool TObjectServiceProxy::TRspExecuteBatch::TryDeserializeBody(
    TRef data,
    std::optional<NCompression::ECodec> codecId)
{
    NProto::TRspExecute body;
    auto deserializeResult = codecId
        ? TryDeserializeProtoWithCompression(&body, data, *codecId)
        : TryDeserializeProtoWithEnvelope(&body, data);
    if (!deserializeResult) {
        return false;
    }

    if (body.subresponses_size() != 0) { // new format
        YT_VERIFY(std::ssize(InnerResponseDescriptors_) >= body.subresponses_size());

        auto partIndex = 0;
        for (const auto& subresponse : body.subresponses()) {
            auto subrequestIndex = subresponse.index();
            auto partCount = subresponse.part_count();
            auto revision = subresponse.revision();
            if (subresponse.has_advised_sticky_group_size() && StickyGroupSizeCache_) {
                auto advisedStickyGroupSize = subresponse.advised_sticky_group_size();
                auto key = InnerRequestDescriptors_[subrequestIndex].GetKey();
                auto oldAdvisedStickyGroupSize = StickyGroupSizeCache_->UpdateAdvisedStickyGroupSize(key, advisedStickyGroupSize);
                YT_LOG_DEBUG_IF(
                    advisedStickyGroupSize != oldAdvisedStickyGroupSize,
                    "Cached sticky group size updated (RequestId: %v, SubrequestIndex: %v, Size: %v -> %v)",
                    GetRequestId(),
                    subrequestIndex,
                    oldAdvisedStickyGroupSize,
                    advisedStickyGroupSize);
            }
            InnerResponseDescriptors_[subrequestIndex].Meta = {{partIndex, partIndex + partCount}, revision};
            partIndex += partCount;
        }
        ResponseCount_ = body.subresponses_size();

        for (const auto& uncertainSubrequestIndex : body.uncertain_subrequest_indexes()) {
            InnerResponseDescriptors_[uncertainSubrequestIndex].Uncertain = true;
        }
    } else { // old format
        // COMPAT(shakurov)

        YT_VERIFY(std::ssize(InnerResponseDescriptors_) >= body.part_counts_size());
        YT_VERIFY(body.revisions_size() == body.part_counts_size() || body.revisions_size() == 0);
        YT_VERIFY(body.advised_sticky_group_size_size() == body.part_counts_size() || body.advised_sticky_group_size_size() == 0);

        auto revisions = body.revisions_size() == 0
            ? std::vector<ui64>(body.part_counts_size())
            : FromProto<std::vector<NHydra::TRevision>>(body.revisions());

        auto partIndex = 0;
        for (auto subrequestIndex = 0; subrequestIndex < body.part_counts_size(); ++subrequestIndex) {
            auto partCount = body.part_counts(subrequestIndex);
            InnerResponseDescriptors_[subrequestIndex].Meta = {{partIndex, partIndex + partCount}, revisions[subrequestIndex]};
            partIndex += partCount;

            if (body.advised_sticky_group_size_size() > 0 && StickyGroupSizeCache_) {
                auto advisedStickyGroupSize = body.advised_sticky_group_size(subrequestIndex);
                auto key = InnerRequestDescriptors_[subrequestIndex].GetKey();
                auto oldAdvisedStickyGroupSize = StickyGroupSizeCache_->UpdateAdvisedStickyGroupSize(key, advisedStickyGroupSize);
                YT_LOG_DEBUG_IF(
                    advisedStickyGroupSize != oldAdvisedStickyGroupSize,
                    "Cached sticky group size updated (RequestId: %v, SubrequestIndex: %v, Size: %v -> %v)",
                    GetRequestId(),
                    subrequestIndex,
                    oldAdvisedStickyGroupSize,
                    advisedStickyGroupSize);
            }
        }

        ResponseCount_ = body.part_counts_size();
    }

    return true;
}

void TObjectServiceProxy::TRspExecuteBatch::SetResponseReceived(
    int index,
    NHydra::TRevision revision,
    TAttachmentRange attachments)
{
    YT_VERIFY(0 <= index && index <= std::ssize(InnerResponseDescriptors_));

    auto& descriptor = InnerResponseDescriptors_[index];
    YT_VERIFY(!descriptor.Meta);

    const auto attachmentCount = static_cast<int>(Attachments_.size());
    const auto rangeSize = std::distance(attachments.Begin, attachments.End);

    descriptor.Uncertain = false;
    descriptor.Meta = {{attachmentCount, attachmentCount + rangeSize}, revision};
    ++ResponseCount_;

    Attachments_.insert(Attachments_.end(), attachments.Begin, attachments.End);

    if (index == FirstUnreceivedResponseIndex_) {
        for (; FirstUnreceivedResponseIndex_ < std::ssize(InnerRequestDescriptors_); ++FirstUnreceivedResponseIndex_) {
            if (!IsResponseReceived(FirstUnreceivedResponseIndex_)) {
                break;
            }
        }
    }
}

void TObjectServiceProxy::TRspExecuteBatch::SetResponseUncertain(int index)
{
    YT_VERIFY(0 <= index && index <= std::ssize(InnerResponseDescriptors_));
    YT_VERIFY(!InnerResponseDescriptors_[index].Meta);
    InnerResponseDescriptors_[index].Uncertain = true;
}

int TObjectServiceProxy::TRspExecuteBatch::GetSize() const
{
    return InnerResponseDescriptors_.size();
}

int TObjectServiceProxy::TRspExecuteBatch::GetResponseCount() const
{
    return ResponseCount_;
}

std::vector<int> TObjectServiceProxy::TRspExecuteBatch::GetUncertainRequestIndexes() const
{
    std::vector<int> result;
    result.reserve(InnerResponseDescriptors_.size());

    for (auto i = 0; i < std::ssize(InnerResponseDescriptors_); ++i) {
        if (IsResponseUncertain(i)) {
            result.push_back(i);
        }
    }

    return result;
}

TGuid TObjectServiceProxy::TRspExecuteBatch::GetRequestId() const
{
    return FromProto<TGuid>(Header().request_id());
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

std::optional<TErrorOr<TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const TString& key) const
{
    return FindResponse<TYPathResponse>(key);
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const TString& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<TErrorOr<TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const std::optional<TString>& key) const
{
    return GetResponses<TYPathResponse>(key);
}

TSharedRefArray TObjectServiceProxy::TRspExecuteBatch::GetResponseMessage(int index) const
{
    YT_VERIFY(index >= 0 && index < std::ssize(InnerRequestDescriptors_));

    const auto& responseMeta = InnerResponseDescriptors_[index].Meta;

    if (!responseMeta) {
        return TSharedRefArray();
    }

    auto [beginIndex, endIndex] = responseMeta->PartRange;
    if (beginIndex == endIndex) {
        // This is an empty response.
        return TSharedRefArray();
    }
    return TSharedRefArray(
        MakeRange(
            Attachments_.data() + beginIndex,
            Attachments_.data() + endIndex),
        TSharedRefArray::TCopyParts{});
}

bool TObjectServiceProxy::TRspExecuteBatch::IsResponseReceived(int index) const
{
    YT_VERIFY(index >= 0 && index < std::ssize(InnerRequestDescriptors_));
    return InnerResponseDescriptors_[index].Meta.has_value();
}

bool TObjectServiceProxy::TRspExecuteBatch::IsResponseUncertain(int index) const
{
    YT_VERIFY(index >= 0 && index < std::ssize(InnerRequestDescriptors_));
    return InnerResponseDescriptors_[index].Uncertain;
}

int TObjectServiceProxy::TRspExecuteBatch::GetFirstUnreceivedResponseIndex() const
{
    return FirstUnreceivedResponseIndex_;
}

TObjectServiceProxy::TRspExecuteBatch::TAttachmentRange TObjectServiceProxy::TRspExecuteBatch::GetResponseAttachmentRange(int index) const
{
    const auto& meta = InnerResponseDescriptors_[index].Meta;
    YT_VERIFY(meta);
    return {
        Attachments_.begin() + meta->PartRange.first,
        Attachments_.begin() + meta->PartRange.second
    };
}

NHydra::TRevision TObjectServiceProxy::TRspExecuteBatch::GetRevision(int index) const
{
    if (InnerResponseDescriptors_.empty()) {
        return NHydra::NullRevision;
    }

    YT_VERIFY(index >= 0 && index <= std::ssize(InnerRequestDescriptors_));

    const auto& meta = InnerResponseDescriptors_[index].Meta;
    YT_VERIFY(meta);
    return meta->Revision;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::ExecuteBatch(int subbatchSize)
{
    auto batchReq = New<TReqExecuteBatch>(
        Channel_,
        subbatchSize,
        StickyGroupSizeCache_,
        CellTag_,
        ChannelKind_);
    PrepareBatchRequest(batchReq);
    return batchReq;
}

TObjectServiceProxy::TReqExecuteBatchBasePtr
TObjectServiceProxy::ExecuteBatchNoBackoffRetries(int subbatchSize)
{
    auto batchReq = New<TReqExecuteBatchBase>(
        Channel_,
        subbatchSize,
        StickyGroupSizeCache_,
        CellTag_,
        ChannelKind_);
    PrepareBatchRequest(batchReq);
    return batchReq;
}

TObjectServiceProxy::TReqExecuteBatchWithRetriesPtr
TObjectServiceProxy::ExecuteBatchWithRetries(
    TReqExecuteBatchWithRetriesConfigPtr config,
    TCallback<bool(int, const TError&)> needRetry,
    int subbatchSize)
{
    auto batchReq = New<TReqExecuteBatchWithRetries>(
        Channel_,
        std::move(config),
        StickyGroupSizeCache_,
        CellTag_,
        ChannelKind_,
        std::move(needRetry),
        subbatchSize);
    PrepareBatchRequest(batchReq);
    return batchReq;
}

TObjectServiceProxy::TReqExecuteBatchWithRetriesInParallelPtr
TObjectServiceProxy::ExecuteBatchWithRetriesInParallel(
    TReqExecuteBatchWithRetriesConfigPtr config,
    TCallback<bool(int, const TError&)> needRetry,
    int subbatchSize,
    int maxParallelSubbatchCount)
{
    YT_VERIFY(maxParallelSubbatchCount > 0);
    std::vector<TReqExecuteBatchWithRetriesPtr> parallelReqs;
    for (int i = 0; i < maxParallelSubbatchCount; ++i) {
        auto batchReq = ExecuteBatchWithRetries(
            config,
            needRetry,
            subbatchSize);
        PrepareBatchRequest(batchReq);
        parallelReqs.push_back(std::move(batchReq));
    }
    return New<TReqExecuteBatchWithRetriesInParallel>(
        Channel_,
        subbatchSize,
        StickyGroupSizeCache_,
        CellTag_,
        ChannelKind_,
        std::move(parallelReqs));
}

TObjectServiceProxy::TReqExecuteBatchWithRetriesInParallel::TReqExecuteBatchWithRetriesInParallel(
    NRpc::IChannelPtr channel,
    int subbatchSize,
    TStickyGroupSizeCachePtr stickyGroupSizeCache,
    std::optional<TCellTag> cellTag,
    std::optional<NApi::EMasterChannelKind> channelKind,
    std::vector<TReqExecuteBatchWithRetriesPtr> parallelReqs)
    : TReqExecuteBatchBase(
        std::move(channel),
        subbatchSize,
        std::move(stickyGroupSizeCache),
        cellTag,
        channelKind)
    , ParallelReqs_(std::move(parallelReqs))
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteBatchWithRetriesInParallel::Invoke()
{
    auto needsRoundUp = std::ssize(InnerRequestDescriptors_) % std::ssize(ParallelReqs_) != 0;
    auto reqsPerParallelReq = std::ssize(InnerRequestDescriptors_) / std::ssize(ParallelReqs_) + needsRoundUp;
    for (int i = 0; i < std::ssize(InnerRequestDescriptors_); ++i) {
        const auto& req = InnerRequestDescriptors_[i];
        auto parallelReqIndex = i / reqsPerParallelReq;
        YT_VERIFY(parallelReqIndex < std::ssize(ParallelReqs_));
        ParallelReqs_[parallelReqIndex]->AddRequestMessage(
            req.Message,
            req.Key,
            req.Tag,
            req.Hash);
    }

    std::vector<TFuture<TRspExecuteBatchPtr>> parallelReqFutures;
    for (auto& parallelReq : ParallelReqs_) {
        parallelReqFutures.push_back(parallelReq->Invoke());
    }

    return AllSucceeded(std::move(parallelReqFutures)).Apply(
        BIND(&TReqExecuteBatchWithRetriesInParallel::OnParallelResponses, MakeStrong(this)));
}

TObjectServiceProxy::TRspExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatchWithRetriesInParallel::OnParallelResponses(
    const TErrorOr<std::vector<TRspExecuteBatchPtr>>& parallelRspsOrError)
{
    auto fullResponse = New<TRspExecuteBatch>(
        CreateClientContext(),
        InnerRequestDescriptors_,
        StickyGroupSizeCache_);

    if (!parallelRspsOrError.IsOK()) {
        fullResponse->GetPromise().Set(parallelRspsOrError);
        return fullResponse;
    }

    const auto& parallelRsps = parallelRspsOrError.Value();

    auto globalIndex = 0;
    for (const auto& parallelRsp : parallelRsps) {
        for (auto i = 0; i < parallelRsp->GetSize(); ++i) {
            if (parallelRsp->IsResponseReceived(i)) {
                auto revision = parallelRsp->GetRevision(i);
                auto attachmentRange = parallelRsp->GetResponseAttachmentRange(i);
                fullResponse->SetResponseReceived(globalIndex, revision, attachmentRange);
            } else if (parallelRsp->IsResponseUncertain(i)) {
                fullResponse->SetResponseUncertain(globalIndex);
            }
            ++globalIndex;
        }
    }

    fullResponse->SetPromise({});
    return fullResponse;
}

template <class TBatchReqPtr>
void TObjectServiceProxy::PrepareBatchRequest(const TBatchReqPtr& batchReq)
{
    batchReq->SetTimeout(DefaultTimeout_);
    batchReq->SetAcknowledgementTimeout(DefaultAcknowledgementTimeout_);
}

const TServiceDescriptor& TObjectServiceProxy::GetDescriptor()
{
    static auto serviceDescriptor = TServiceDescriptor("ObjectService")
        .SetProtocolVersion(12)
        .SetAcceptsBaggage(false)
        .SetFeaturesType<EMasterFeature>();
    return serviceDescriptor;
}

////////////////////////////////////////////////////////////////////////////////

TError GetCumulativeError(
    const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError,
    const std::optional<TString>& key)
{
    if (!batchRspOrError.IsOK()) {
        return batchRspOrError;
    }

    return GetCumulativeError(batchRspOrError.Value(), key);
}

TError GetCumulativeError(
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp,
    const std::optional<TString>& key)
{
    TError cumulativeError("Error communicating with master");
    for (const auto& rspOrError : batchRsp->GetResponses(key)) {
        if (!rspOrError.IsOK()) {
            cumulativeError.MutableInnerErrors()->push_back(rspOrError);
        }
    }
    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

void ThrowCumulativeErrorIfFailed(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
    auto cumulativeError = GetCumulativeError(batchRspOrError);
    THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy CreateObjectServiceReadProxy(
    IClientPtr client,
    NApi::EMasterChannelKind readFrom,
    TCellTag cellTag,
    TStickyGroupSizeCachePtr stickyGroupSizeCache)
{
    return TObjectServiceProxy(
        std::move(client),
        readFrom,
        cellTag,
        std::move(stickyGroupSizeCache));
}

TObjectServiceProxy CreateObjectServiceWriteProxy(
    NApi::NNative::IClientPtr client,
    TCellTag cellTag)
{
    return TObjectServiceProxy(
        std::move(client),
        NApi::EMasterChannelKind::Leader,
        cellTag,
        /*stickyGroupSizeCache*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

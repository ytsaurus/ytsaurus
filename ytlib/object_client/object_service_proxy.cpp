#include "object_service_proxy.h"

#include <yt/ytlib/object_client/object_ypath.pb.h>

#include <yt/core/rpc/message.h>

namespace NYT {
namespace NObjectClient {

using namespace NYTree;
using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(IChannelPtr channel)
    : TClientRequest(
        std::move(channel),
        TObjectServiceProxy::GetDescriptor().ServiceName,
        "Execute",
        TObjectServiceProxy::GetDescriptor().ProtocolVersion)
{ }

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(
    const TReqExecuteBatch& other,
    int beginPos,
    int retriesEndPos,
    int endPos)
    : TClientRequest(other)
    , SuppressUpstreamSync_(other.SuppressUpstreamSync_)
{
    // Undo some work done by the base class's copy ctor and make some tweaks.
    Attachments_.clear();
    ToProto(Header_.mutable_request_id(), TRequestId::Create());
    SerializedBody_.Reset();
    Hash_.Reset();
    FirstTimeSerialization_ = true;

    const auto otherBegin = other.InnerRequestDescriptors_.begin();

    InnerRequestDescriptors_.reserve(endPos - beginPos);

    InnerRequestDescriptors_.insert(InnerRequestDescriptors_.end(), otherBegin + beginPos, otherBegin + retriesEndPos);
    // Patch 'retry' flags.
    for (auto& descriptor : InnerRequestDescriptors_) {
        if (descriptor.NeedsPatchingForRetry) {
            descriptor.Message = PatchForRetry(descriptor.Message);
            descriptor.NeedsPatchingForRetry = false;
        }
    }

    InnerRequestDescriptors_.insert(InnerRequestDescriptors_.end(), otherBegin + retriesEndPos, otherBegin + endPos);
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

    auto batchRsp = CreateResponse();
    auto promise = batchRsp->GetPromise();
    if (GetSize() == 0) {
        batchRsp->SetEmpty();
    } else {
        auto requestControl = Send(batchRsp);
        promise.OnCanceled(BIND([=] () {
            requestControl->Cancel();
        }));
    }
    return promise;
}

TObjectServiceProxy::TRspExecuteBatchPtr TObjectServiceProxy::TReqExecuteSubbatch::CreateResponse()
{
    auto clientContext = CreateClientContext();
    return New<TRspExecuteBatch>(clientContext, std::multimap<TString, int>());
}

TSharedRefArray TObjectServiceProxy::TReqExecuteSubbatch::PatchForRetry(const TSharedRefArray& message)
{
    NRpc::NProto::TRequestHeader header;
    YCHECK(ParseRequestHeader(message, &header));
    YCHECK(!header.retry());
    header.set_retry(true);
    return SetRequestHeader(message, header);
}

TSharedRef TObjectServiceProxy::TReqExecuteSubbatch::SerializeBody() const
{
    NProto::TReqExecute req;
    req.set_suppress_upstream_sync(SuppressUpstreamSync_);
    req.set_allow_backoff(true);
    for (const auto& descriptor : InnerRequestDescriptors_) {
        if (descriptor.Message) {
            req.add_part_counts(descriptor.Message.Size());
        } else {
            req.add_part_counts(0);
        }
    }
    return SerializeProtoToRefWithEnvelope(req);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatch::TReqExecuteBatch(IChannelPtr channel)
    : TReqExecuteSubbatch(std::move(channel))
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> TObjectServiceProxy::TReqExecuteBatch::Invoke()
{
    FullResponsePromise_ = NewPromise<TRspExecuteBatchPtr>();
    PushDownPrerequisites();
    InvokeNextBatch();
    return FullResponsePromise_;
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequest(
    TYPathRequestPtr innerRequest,
    const TString& key)
{
    TSharedRefArray innerRequestMessage;
    auto needsPatchingForRetry = false;
    if (innerRequest) {
        innerRequestMessage = innerRequest->Serialize();
        auto mutationId = innerRequest->GetMutationId();
        auto isRetry = innerRequest->GetRetry();
        if (mutationId && !isRetry) {
            needsPatchingForRetry = true;
        }
    }

    return AddRequestMessage(innerRequestMessage, needsPatchingForRetry, key);
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequestMessage(
    TSharedRefArray innerRequestMessage,
    bool needsPatchingForRetry,
    const TString& key)
{
    if (!key.empty()) {
        int index = static_cast<int>(InnerRequestDescriptors_.size());
        KeyToIndexes_.insert(std::make_pair(key, index));
    }

    InnerRequestDescriptors_.emplace_back(TInnerRequestDescriptor{innerRequestMessage, needsPatchingForRetry});

    return this;
}

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatch::SetTimeout(
    TNullable<TDuration> timeout)
{
    TClientRequest::SetTimeout(timeout);
    return this;
}

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatch::SetSuppressUpstreamSync(
    bool value)
{
    SuppressUpstreamSync_ = value;
    return this;
}

TObjectServiceProxy::TReqExecuteSubbatchPtr TObjectServiceProxy::TReqExecuteBatch::Slice(int beginPos, int retriesEndPos, int endPos)
{
    return New<TReqExecuteSubbatch>(*this, beginPos, retriesEndPos, endPos);
}

void TObjectServiceProxy::TReqExecuteBatch::PushDownPrerequisites()
{
    // Push TPrerequisitesExt down to individual requests.
    if (Header_.HasExtension(NProto::TPrerequisitesExt::prerequisites_ext)) {
        const auto& batchPrerequisitesExt = Header_.GetExtension(NProto::TPrerequisitesExt::prerequisites_ext);
        for (auto& descriptor : InnerRequestDescriptors_) {
            NRpc::NProto::TRequestHeader requestHeader;
            YCHECK(ParseRequestHeader(descriptor.Message, &requestHeader));
            auto* prerequisitesExt = requestHeader.MutableExtension(NProto::TPrerequisitesExt::prerequisites_ext);
            prerequisitesExt->mutable_transactions()->MergeFrom(batchPrerequisitesExt.transactions());
            prerequisitesExt->mutable_revisions()->MergeFrom(batchPrerequisitesExt.revisions());
            descriptor.Message = SetRequestHeader(descriptor.Message, requestHeader);
        }
        Header_.ClearExtension(NProto::TPrerequisitesExt::prerequisites_ext);
    }
}

TObjectServiceProxy::TRspExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatch::CreateResponse()
{
    auto clientContext = CreateClientContext();
    return New<TRspExecuteBatch>(clientContext, KeyToIndexes_);
}

void TObjectServiceProxy::TReqExecuteBatch::InvokeNextBatch()
{
    CurBatchBegin_ = GetTotalSubresponseCount();
    auto lastBatchEnd = CurBatchEnd_;
    CurBatchEnd_ = std::min(CurBatchBegin_ + MaxSingleSubbatchSize, GetTotalSubrequestCount());

    YCHECK(CurBatchBegin_ < CurBatchEnd_ || GetTotalSubrequestCount() == 0);

    // Optimization for the typical case of a small batch.
    if (CurBatchBegin_ == 0 && CurBatchEnd_ == GetTotalSubrequestCount()) {
        CurReqFuture_ = DoInvoke();
    } else {
        // If the last batch was backed off, we may not have received all the
        // subresponses. We must mark relevant subrequests as retries to avoid
        // them being rejected by the response keeper.
        auto subbatchReq = Slice(CurBatchBegin_, lastBatchEnd, CurBatchEnd_);
        CurReqFuture_ = subbatchReq->DoInvoke();
    }

    CurReqFuture_.Apply(BIND(&TObjectServiceProxy::TReqExecuteBatch::OnSubbatchResponse, MakeStrong(this)));
}

void TObjectServiceProxy::TReqExecuteBatch::OnSubbatchResponse(const TErrorOr<TRspExecuteBatchPtr>& rspOrErr)
{
    if (!rspOrErr.IsOK()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    const auto& rsp = rspOrErr.Value();

    // Optimization for the typical case of a small batch.
    if (CurBatchBegin_ == 0 && rsp->GetSize() == GetTotalSubrequestCount()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    // The remote side shouldn't backoff until there's at least one subresponse.
    YCHECK(rsp->GetSize() > 0 || GetTotalSubrequestCount() == 0);

    FullResponse()->Append(rsp);

    if (GetTotalSubresponseCount() == GetTotalSubrequestCount()) {
        FullResponse()->SetPromise({});
        return;
    }

    InvokeNextBatch();
}

int TObjectServiceProxy::TReqExecuteBatch::GetTotalSubrequestCount() const
{
    return GetSize();
}

int TObjectServiceProxy::TReqExecuteBatch::GetTotalSubresponseCount() const
{
    return FullResponse_ ? FullResponse_->GetSize() : 0;
}

TObjectServiceProxy::TRspExecuteBatchPtr& TObjectServiceProxy::TReqExecuteBatch::FullResponse()
{
    if (!FullResponse_) {
        // Make sure the full response uses the promise we've returned to the caller.
        FullResponse_ = New<TRspExecuteBatch>(
            CreateClientContext(),
            KeyToIndexes_,
            FullResponsePromise_);
    }
    return FullResponse_;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::multimap<TString, int>& keyToIndexes)
    : TClientResponse(std::move(clientContext))
    , KeyToIndexes_(keyToIndexes)
{ }

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::multimap<TString, int>& keyToIndexes,
    TPromise<TRspExecuteBatchPtr> promise)
    : TClientResponse(std::move(clientContext))
    , KeyToIndexes_(keyToIndexes)
    , Promise_(std::move(promise))
{ }

auto TObjectServiceProxy::TRspExecuteBatch::GetPromise() -> TPromise<TRspExecuteBatchPtr>
{
    return Promise_;
}

void TObjectServiceProxy::TRspExecuteBatch::SetEmpty()
{
    NProto::TRspExecute body;
    auto message = CreateResponseMessage(body);
    static_cast<IClientResponseHandler*>(this)->HandleResponse(std::move(message));
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

void TObjectServiceProxy::TRspExecuteBatch::DeserializeBody(const TRef& data)
{
    NProto::TRspExecute body;
    DeserializeProtoWithEnvelope(&body, data);

    int currentIndex = 0;
    PartRanges_.reserve(body.part_counts_size());
    for (int partCount : body.part_counts()) {
        PartRanges_.push_back(std::make_pair(currentIndex, currentIndex + partCount));
        currentIndex += partCount;
    }
}

void TObjectServiceProxy::TRspExecuteBatch::Append(const TRspExecuteBatchPtr& subbatchResponse)
{
    YCHECK(
        PartRanges_.empty() && Attachments_.empty() ||
        PartRanges_.back().second == Attachments_.size());

    int rangeIndexShift = Attachments_.size();
    PartRanges_.reserve(PartRanges_.size() + subbatchResponse->PartRanges_.size());
    for (const auto& range : subbatchResponse->PartRanges_) {
        PartRanges_.emplace_back(range.first + rangeIndexShift, range.second + rangeIndexShift);
    }

    Attachments_.insert(
        Attachments_.end(),
        subbatchResponse->Attachments_.begin(),
        subbatchResponse->Attachments_.end());
}

int TObjectServiceProxy::TRspExecuteBatch::GetSize() const
{
    return PartRanges_.size();
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

TNullable<TErrorOr<TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const TString& key) const
{
    return FindResponse<TYPathResponse>(key);
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const TString& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<TErrorOr<NYTree::TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const TString& key) const
{
    return GetResponses<TYPathResponse>(key);
}

TSharedRefArray TObjectServiceProxy::TRspExecuteBatch::GetResponseMessage(int index) const
{
    YCHECK(index >= 0 && index < GetSize());
    int beginIndex = PartRanges_[index].first;
    int endIndex = PartRanges_[index].second;
    if (beginIndex == endIndex) {
        // This is an empty response.
        return TSharedRefArray();
    }
    return TSharedRefArray(std::vector<TSharedRef>(
        Attachments_.begin() + beginIndex,
        Attachments_.begin() + endIndex));
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::ExecuteBatch()
{
    return New<TReqExecuteBatch>(Channel_)
        ->SetTimeout(DefaultTimeout_);
}

////////////////////////////////////////////////////////////////////////////////

TError GetCumulativeError(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError, const TString& key)
{
    if (!batchRspOrError.IsOK()) {
        return batchRspOrError;
    }

    TError cumulativeError("Error communicating with master");
    const auto& batchRsp = batchRspOrError.Value();
    for (const auto& rspOrError : batchRsp->GetResponses(key)) {
        if (!rspOrError.IsOK()) {
            cumulativeError.InnerErrors().push_back(rspOrError);
        }
    }

    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

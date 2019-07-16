#include "object_service_proxy.h"

#include "private.h"

#include <yt/ytlib/object_client/object_ypath.pb.h>

#include <yt/core/misc/checksum.h>

#include <yt/core/rpc/message.h>

namespace NYT::NObjectClient {

using namespace NYTree;
using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectClientLogger;

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteSubbatch::TReqExecuteSubbatch(IChannelPtr channel)
    : TClientRequest(
        std::move(channel),
        TObjectServiceProxy::GetDescriptor(),
        TMethodDescriptor("Execute"))
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
    SerializedData_.Reset();
    Hash_.reset();

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
    return New<TRspExecuteBatch>(CreateClientContext());
}

TSharedRefArray TObjectServiceProxy::TReqExecuteSubbatch::PatchForRetry(const TSharedRefArray& message)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(ParseRequestHeader(message, &header));
    YT_VERIFY(!header.retry());
    header.set_retry(true);
    return SetRequestHeader(message, header);
}

TSharedRefArray TObjectServiceProxy::TReqExecuteSubbatch::SerializeData() const
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
    auto body = SerializeProtoToRefWithEnvelope(req);

    std::vector<TSharedRef> data;
    data.reserve(Attachments_.size() + 1);
    data.push_back(std::move(body));
    data.insert(data.end(), Attachments_.begin(), Attachments_.end());

    return TSharedRefArray(std::move(data), TSharedRefArray::TMoveParts{});
}

size_t TObjectServiceProxy::TReqExecuteSubbatch::GetHash() const
{
    if (!Hash_) {
        size_t hash = 0;
        HashCombine(hash, SuppressUpstreamSync_);
        for (const auto& descriptor : InnerRequestDescriptors_) {
            if (descriptor.Hash) {
                HashCombine(hash, descriptor.Hash);
            } else {
                for (auto* it = descriptor.Message.Begin(); it != descriptor.Message.End(); ++it) {
                    HashCombine(hash, GetChecksum(*it));
                }
            }
        }
        Hash_ = hash;
    }
    return *Hash_;
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
    const TYPathRequestPtr& innerRequest,
    std::optional<TString> key,
    std::optional<size_t> hash)
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

    InnerRequestDescriptors_.push_back({
        std::move(key),
        innerRequest->Tag(),
        std::move(innerRequestMessage),
        needsPatchingForRetry,
        hash
    });

    return this;
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequestMessage(
    TSharedRefArray innerRequestMessage,
    bool needsPatchingForRetry,
    std::optional<TString> key,
    std::optional<size_t> hash)
{
    InnerRequestDescriptors_.push_back(TInnerRequestDescriptor{
        std::move(key),
        {},
        std::move(innerRequestMessage),
        needsPatchingForRetry,
        hash
    });

    return this;
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::SetTimeout(std::optional<TDuration> timeout)
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

TObjectServiceProxy::TReqExecuteSubbatchPtr
TObjectServiceProxy::TReqExecuteBatch::Slice(int beginPos, int retriesEndPos, int endPos)
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
            YT_VERIFY(ParseRequestHeader(descriptor.Message, &requestHeader));
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
    return New<TRspExecuteBatch>(CreateClientContext(), InnerRequestDescriptors_);
}

void TObjectServiceProxy::TReqExecuteBatch::InvokeNextBatch()
{
    CurrentBatchBegin_ = GetTotalSubresponseCount();
    auto lastBatchEnd = CurrentBatchEnd_;
    CurrentBatchEnd_ = std::min(CurrentBatchBegin_ + MaxSingleSubbatchSize, GetTotalSubrequestCount());

    YT_VERIFY(CurrentBatchBegin_ < CurrentBatchEnd_ || GetTotalSubrequestCount() == 0);

    // Optimization for the typical case of a small batch.
    if (CurrentBatchBegin_ == 0 && CurrentBatchEnd_ == GetTotalSubrequestCount()) {
        CurrentReqFuture_ = DoInvoke();
    } else {
        // If the last batch was backed off, we may not have received all the
        // subresponses. We must mark relevant subrequests as retries to avoid
        // them being rejected by the response keeper.
        auto subbatchReq = Slice(CurrentBatchBegin_, lastBatchEnd, CurrentBatchEnd_);
        CurrentReqFuture_ = subbatchReq->DoInvoke();
        YT_LOG_DEBUG("Subbatch request invoked (BatchRequestId: %v, SubbatchRequestId: %v, SubbatchBegin: %v, SubbatchEnd: %v, SubbatchRetriesEnd: %v)",
            GetRequestId(),
            subbatchReq->GetRequestId(),
            CurrentBatchBegin_,
            CurrentBatchEnd_,
            lastBatchEnd);
    }

    CurrentReqFuture_.Apply(BIND(&TObjectServiceProxy::TReqExecuteBatch::OnSubbatchResponse, MakeStrong(this)));
}

void TObjectServiceProxy::TReqExecuteBatch::OnSubbatchResponse(const TErrorOr<TRspExecuteBatchPtr>& rspOrErr)
{
    if (!rspOrErr.IsOK()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    const auto& rsp = rspOrErr.Value();

    // Optimization for the typical case of a small batch.
    if (CurrentBatchBegin_ == 0 && rsp->GetSize() == GetTotalSubrequestCount()) {
        FullResponsePromise_.Set(rspOrErr);
        return;
    }

    YT_LOG_DEBUG("Subbatch response received (BatchRequestId: %v, SubbatchRequestId: %v, SubbatchBegin: %v, SubbatchSubresponseCount: %v)",
        GetRequestId(),
        rsp->GetRequestId(),
        CurrentBatchBegin_,
        rsp->GetSize());

    // The remote side shouldn't backoff until there's at least one subresponse.
    YT_VERIFY(rsp->GetSize() > 0 || GetTotalSubrequestCount() == 0);

    GetFullResponse()->Append(rsp);

    if (GetTotalSubresponseCount() == GetTotalSubrequestCount()) {
        GetFullResponse()->SetPromise({});
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

TObjectServiceProxy::TRspExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatch::GetFullResponse()
{
    if (!FullResponse_) {
        // Make sure the full response uses the promise we've returned to the caller.
        FullResponse_ = New<TRspExecuteBatch>(
            CreateClientContext(),
            InnerRequestDescriptors_,
            FullResponsePromise_);
    }
    return FullResponse_;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::vector<TInnerRequestDescriptor>& innerRequestDescriptors,
    TPromise<TRspExecuteBatchPtr> promise)
    : TClientResponse(std::move(clientContext))
    , InnerRequestDescriptors_(innerRequestDescriptors)
    , Promise_(promise ? std::move(promise) : NewPromise<TRspExecuteBatchPtr>())
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

void TObjectServiceProxy::TRspExecuteBatch::DeserializeBody(
    TRef data,
    std::optional<NCompression::ECodec> codecId)
{
    NProto::TRspExecute body;
    if (codecId) {
        DeserializeProtoWithCompression(&body, data, *codecId);
    } else {
        DeserializeProtoWithEnvelope(&body, data);
    }

    int currentIndex = 0;
    PartRanges_.reserve(body.part_counts_size());
    for (int partCount : body.part_counts()) {
        PartRanges_.push_back(std::make_pair(currentIndex, currentIndex + partCount));
        currentIndex += partCount;
    }

    FromProto(&Revisions_, body.revisions());
}

void TObjectServiceProxy::TRspExecuteBatch::Append(const TRspExecuteBatchPtr& subbatchResponse)
{
    YT_VERIFY(
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

std::vector<TErrorOr<NYTree::TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const std::optional<TString>& key) const
{
    return GetResponses<TYPathResponse>(key);
}

TSharedRefArray TObjectServiceProxy::TRspExecuteBatch::GetResponseMessage(int index) const
{
    YT_VERIFY(index >= 0 && index < GetSize());
    int beginIndex = PartRanges_[index].first;
    int endIndex = PartRanges_[index].second;
    if (beginIndex == endIndex) {
        // This is an empty response.
        return TSharedRefArray();
    }
    return TSharedRefArray(
        MakeRange(
            Attachments_.begin() + beginIndex,
            Attachments_.begin() + endIndex),
        TSharedRefArray::TCopyParts{});
}

std::optional<ui64> TObjectServiceProxy::TRspExecuteBatch::GetRevision(int index) const
{
    if (Revisions_.empty()) {
        return std::nullopt;
    }

    YT_VERIFY(index >= 0 && index <= Revisions_.size());
    return Revisions_[index];
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::ExecuteBatch()
{
    return New<TReqExecuteBatch>(Channel_)
        ->SetTimeout(DefaultTimeout_);
}

////////////////////////////////////////////////////////////////////////////////

TError GetCumulativeError(
    const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError,
    const std::optional<TString>& key)
{
    if (!batchRspOrError.IsOK()) {
        return batchRspOrError;
    }
    return GetCumulativeError(batchRspOrError.Value());
}

TError GetCumulativeError(
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp,
    const std::optional<TString>& key)
{
    TError cumulativeError("Error communicating with master");
    for (const auto& rspOrError : batchRsp->GetResponses(key)) {
        if (!rspOrError.IsOK()) {
            cumulativeError.InnerErrors().push_back(rspOrError);
        }
    }
    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

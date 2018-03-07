#include "object_service_proxy.h"

#include <yt/ytlib/object_client/object_ypath.pb.h>

#include <yt/core/rpc/message.h>

namespace NYT {
namespace NObjectClient {

using namespace NYTree;
using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatch::TReqExecuteBatch(IChannelPtr channel)
    : TClientRequest(
        std::move(channel),
        TObjectServiceProxy::GetDescriptor().ServiceName,
        "Execute",
        TObjectServiceProxy::GetDescriptor().ProtocolVersion)
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteBatch::Invoke()
{
    // Push TPrerequisitesExt down to individual requests.
    if (Header_.HasExtension(NProto::TPrerequisitesExt::prerequisites_ext)) {
        const auto& batchPrerequisitesExt = Header_.GetExtension(NProto::TPrerequisitesExt::prerequisites_ext);
        for (auto& innerRequestMessage : InnerRequestMessages_) {
            NRpc::NProto::TRequestHeader requestHeader;
            YCHECK(ParseRequestHeader(innerRequestMessage, &requestHeader));
            auto* prerequisitesExt = requestHeader.MutableExtension(NProto::TPrerequisitesExt::prerequisites_ext);
            prerequisitesExt->mutable_transactions()->MergeFrom(batchPrerequisitesExt.transactions());
            prerequisitesExt->mutable_revisions()->MergeFrom(batchPrerequisitesExt.revisions());
            innerRequestMessage = SetRequestHeader(innerRequestMessage, requestHeader);
        }
        Header_.ClearExtension(NProto::TPrerequisitesExt::prerequisites_ext);
    }

    // Prepare attachments.
    for (const auto& innerRequestMessage : InnerRequestMessages_) {
        if (innerRequestMessage) {
            Attachments_.insert(Attachments_.end(), innerRequestMessage.Begin(), innerRequestMessage.End());
        }
    }

    auto clientContext = CreateClientContext();
    auto batchRsp = New<TRspExecuteBatch>(clientContext, KeyToIndexes_);
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

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequest(
    TYPathRequestPtr innerRequest,
    const TString& key)
{
    return AddRequestMessage(
        innerRequest ? innerRequest->Serialize() : TSharedRefArray(),
        key);
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequestMessage(
    TSharedRefArray innerRequestMessage,
    const TString& key)
{
    if (!key.empty()) {
        int index = static_cast<int>(InnerRequestMessages_.size());
        KeyToIndexes_.insert(std::make_pair(key, index));
    }

    InnerRequestMessages_.push_back(innerRequestMessage);

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

int TObjectServiceProxy::TReqExecuteBatch::GetSize() const
{
    return static_cast<int>(InnerRequestMessages_.size());
}

TSharedRef TObjectServiceProxy::TReqExecuteBatch::SerializeBody() const
{
    NProto::TReqExecute req;
    req.set_suppress_upstream_sync(SuppressUpstreamSync_);
    for (const auto& innerRequestMessage : InnerRequestMessages_) {
        if (innerRequestMessage) {
            req.add_part_counts(innerRequestMessage.Size());
        } else {
            req.add_part_counts(0);
        }
    }
    return SerializeProtoToRefWithEnvelope(req);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::multimap<TString, int>& keyToIndexes)
    : TClientResponse(std::move(clientContext))
    , KeyToIndexes_(keyToIndexes)
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

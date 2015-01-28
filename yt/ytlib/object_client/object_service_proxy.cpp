#include "stdafx.h"
#include "object_service_proxy.h"

#include <core/rpc/message.h>

#include <yt/ytlib/object_client/object_ypath.pb.h>

namespace NYT {
namespace NObjectClient {

using namespace NYTree;
using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatch::TReqExecuteBatch(
    IChannelPtr channel,
    const Stroka& path,
    const Stroka& method)
    : TClientRequest(
        std::move(channel),
        path,
        method,
        false,
        TObjectServiceProxy::GetProtocolVersion())
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteBatch::Invoke()
{
    auto clientContxt = CreateClientContext();
    auto batchRsp = New<TRspExecuteBatch>(clientContxt, KeyToIndexes);
    auto promise = batchRsp->GetAsyncResult();
    Send(batchRsp);
    return promise;
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequest(
    TYPathRequestPtr innerRequest,
    const Stroka& key)
{
    return AddRequestMessage(
        innerRequest ? innerRequest->Serialize() : TSharedRefArray(),
        key);
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequestMessage(
    TSharedRefArray innerRequestMessage,
    const Stroka& key)
{
    if (!key.empty()) {
        int index = static_cast<int>(InnerRequestMessages.size());
        KeyToIndexes.insert(std::make_pair(key, index));
    }

    InnerRequestMessages.push_back(innerRequestMessage);

    return this;
}

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::TReqExecuteBatch::SetTimeout(
    TNullable<TDuration> timeout)
{
    TClientRequest::SetTimeout(timeout);
    return this;
}

int TObjectServiceProxy::TReqExecuteBatch::GetSize() const
{
    return static_cast<int>(InnerRequestMessages.size());
}

TSharedRef TObjectServiceProxy::TReqExecuteBatch::SerializeBody()
{
    // Push TPrerequisitesExt down to individual requests.
    if (Header_.HasExtension(NProto::TPrerequisitesExt::prerequisites_ext)) {
        auto batchPrerequisitesExt = Header_.GetExtension(NProto::TPrerequisitesExt::prerequisites_ext);
        for (auto& innerRequestMessage : InnerRequestMessages) {
            NRpc::NProto::TRequestHeader requestHeader;
            YCHECK(ParseRequestHeader(innerRequestMessage, &requestHeader));
            auto* prerequisitesExt = requestHeader.MutableExtension(NProto::TPrerequisitesExt::prerequisites_ext);
            prerequisitesExt->mutable_transactions()->MergeFrom(batchPrerequisitesExt.transactions());
            prerequisitesExt->mutable_revisions()->MergeFrom(batchPrerequisitesExt.revisions());
            innerRequestMessage = SetRequestHeader(innerRequestMessage, requestHeader);
        }
        Header_.ClearExtension(NProto::TPrerequisitesExt::prerequisites_ext);
    }

    NProto::TReqExecute req;
    for (const auto& innerRequestMessage : InnerRequestMessages) {
        if (innerRequestMessage) {
            req.add_part_counts(innerRequestMessage.Size());
            Attachments_.insert(Attachments_.end(), innerRequestMessage.Begin(), innerRequestMessage.End());
        } else {
            req.add_part_counts(0);
        }
    }

    TSharedRef data;
    YCHECK(SerializeToProtoWithEnvelope(req, &data));
    return data;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    TClientContextPtr clientContext,
    const std::multimap<Stroka, int>& keyToIndexes)
    : TClientResponse(std::move(clientContext))
    , KeyToIndexes(keyToIndexes)
    , Promise(NewPromise<TRspExecuteBatchPtr>())
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TRspExecuteBatch::GetAsyncResult()
{
    return Promise;
}

void TObjectServiceProxy::TRspExecuteBatch::SetPromise(const TError& error)
{
    if (error.IsOK()) {
        Promise.Set(this);
    } else {
        Promise.Set(error);
    }
    Promise.Reset();
}

void TObjectServiceProxy::TRspExecuteBatch::DeserializeBody(const TRef& data)
{
    YCHECK(DeserializeFromProtoWithEnvelope(&Body, data));

    int currentIndex = 0;
    BeginPartIndexes.clear();
    BeginPartIndexes.reserve(Body.part_counts_size());
    for (int partCount : Body.part_counts()) {
        BeginPartIndexes.push_back(currentIndex);
        currentIndex += partCount;
    }
}

int TObjectServiceProxy::TRspExecuteBatch::GetSize() const
{
    return Body.part_counts_size();
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

TNullable<TErrorOr<TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const Stroka& key) const
{
    return FindResponse<TYPathResponse>(key);
}

TErrorOr<TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<TErrorOr<NYTree::TYPathResponsePtr>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key) const
{
    return GetResponses<TYPathResponse>(key);
}

TSharedRefArray TObjectServiceProxy::TRspExecuteBatch::GetResponseMessage(int index) const
{
    YCHECK(index >= 0 && index < GetSize());
    int beginIndex = BeginPartIndexes[index];
    int endIndex = beginIndex + Body.part_counts(index);
    if (beginIndex == endIndex) {
        // This is an empty response.
        return TSharedRefArray();
    }

    std::vector<TSharedRef> innerParts(
        Attachments_.begin() + beginIndex,
        Attachments_.begin() + endIndex);
    return TSharedRefArray(std::move(innerParts));
}

////////////////////////////////////////////////////////////////////////////////

Stroka TObjectServiceProxy::GetServiceName()
{
    return "ObjectService";
}

int TObjectServiceProxy::GetProtocolVersion()
{
    return 3;
}

TObjectServiceProxy::TObjectServiceProxy(IChannelPtr channel)
    : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
{ }

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::ExecuteBatch()
{
    // Keep this in sync with DEFINE_RPC_PROXY_METHOD.
    return
        New<TReqExecuteBatch>(Channel_, ServiceName_, "Execute")
        ->SetTimeout(DefaultTimeout_);
}

////////////////////////////////////////////////////////////////////////////////

TError GetCumulativeError(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
{
    if (!batchRspOrError.IsOK()) {
        return batchRspOrError;
    }

    TError cumulativeError("Error communicating with master");
    const auto& batchRsp = batchRspOrError.Value();
    for (const auto& rspOrError : batchRsp->GetResponses()) {
        if (!rspOrError.IsOK()) {
            cumulativeError.InnerErrors().push_back(rspOrError);
        }
    }

    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

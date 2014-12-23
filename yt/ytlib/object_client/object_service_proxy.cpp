#include "stdafx.h"
#include "object_service_proxy.h"

#include <core/rpc/message.h>

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
    DoInvoke(batchRsp);
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
        int index = static_cast<int>(PartCounts.size());
        KeyToIndexes.insert(std::make_pair(key, index));
    }

    if (innerRequestMessage) {
        PartCounts.push_back(static_cast<int>(innerRequestMessage.Size()));
        Attachments_.insert(
            Attachments_.end(),
            innerRequestMessage.Begin(),
            innerRequestMessage.End());
    } else {
        PartCounts.push_back(0);
    }

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
    return static_cast<int>(PartCounts.size());
}

TSharedRef TObjectServiceProxy::TReqExecuteBatch::SerializeBody() const
{
    NProto::TReqExecute req;

    ToProto(req.mutable_part_counts(), PartCounts);

    for (const auto& prerequisite : PrerequisiteTransactions_) {
        auto* protoPrerequisite = req.add_prerequisite_transactions();
        ToProto(protoPrerequisite->mutable_transaction_id(), prerequisite.TransactionId);
    }

    for (const auto& prerequisite : PrerequisiteRevisions_) {
        auto* protoPrerequisite = req.add_prerequisite_revisions();
        protoPrerequisite->set_path(prerequisite.Path);
        ToProto(protoPrerequisite->mutable_transaction_id(), prerequisite.TransactionId);
        protoPrerequisite->set_revision(prerequisite.Revision);
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

void TObjectServiceProxy::TRspExecuteBatch::FireCompleted()
{
    BeforeCompleted();
    Promise.Set(this);
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

TError TObjectServiceProxy::TRspExecuteBatch::GetCumulativeError()
{
    if (!IsOK()) {
        return GetError();
    }

    TError cumulativeError("Error communicating with master");
    for (auto rsp : GetResponses()) {
        auto error = rsp->GetError();
        if (!error.IsOK()) {
            cumulativeError.InnerErrors().push_back(error);
        }
    }

    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

TYPathResponsePtr TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

TYPathResponsePtr TObjectServiceProxy::TRspExecuteBatch::FindResponse(const Stroka& key) const
{
    return FindResponse<TYPathResponse>(key);
}

TYPathResponsePtr TObjectServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<NYTree::TYPathResponsePtr> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key) const
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
    return 1;
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

} // namespace NObjectClient
} // namespace NYT

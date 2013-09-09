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
    const Stroka& verb)
    : TClientRequest(channel, path, verb, false)
{ }

TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>
TObjectServiceProxy::TReqExecuteBatch::Invoke()
{
    auto batchRsp = New<TRspExecuteBatch>(GetRequestId(), KeyToIndexes);
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
        innerRequest ? innerRequest->Serialize() : nullptr,
        key);
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequestMessage(
    IMessagePtr innerRequestMessage,
    const Stroka& key)
{
    if (!key.empty()) {
        int index = static_cast<int>(PartCounts.size());
        KeyToIndexes.insert(std::make_pair(key, index));
    }

    if (innerRequestMessage) {
        const auto& innerParts = innerRequestMessage->GetParts();
        PartCounts.push_back(static_cast<int>(innerParts.size()));
        Attachments_.insert(
            Attachments_.end(),
            innerParts.begin(),
            innerParts.end());
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

    FOREACH (const auto& prerequisite, PrerequisiteTransactions_) {
        auto* protoPrerequisite = req.add_prerequisite_transactions();
        ToProto(protoPrerequisite->mutable_transaction_id(), prerequisite.TransactionId);
    }

    FOREACH (const auto& prerequisite, PrerequisiteRevisions_) {
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
    const TRequestId& requestId,
    const std::multimap<Stroka, int>& keyToIndexes)
    : TClientResponse(requestId)
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
    Promise.Set(this);
    Promise.Reset();
}

void TObjectServiceProxy::TRspExecuteBatch::DeserializeBody(const TRef& data)
{
    YCHECK(DeserializeFromProtoWithEnvelope(&Body, data));

    int currentIndex = 0;
    BeginPartIndexes.clear();
    BeginPartIndexes.reserve(Body.part_counts_size());
    FOREACH (int partCount, Body.part_counts()) {
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
    FOREACH (auto rsp, GetResponses()) {
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

IMessagePtr TObjectServiceProxy::TRspExecuteBatch::GetResponseMessage(int index) const
{
    YCHECK(index >= 0 && index < GetSize());
    int beginIndex = BeginPartIndexes[index];
    int endIndex = beginIndex + Body.part_counts(index);
    if (beginIndex == endIndex) {
        // This is an empty response.
        return nullptr;
    }

    std::vector<TSharedRef> innerParts(
        Attachments_.begin() + beginIndex,
        Attachments_.begin() + endIndex);
    return CreateMessageFromParts(std::move(innerParts));
}

////////////////////////////////////////////////////////////////////////////////

Stroka TObjectServiceProxy::GetServiceName()
{
    return "ObjectService";
}

TObjectServiceProxy::TObjectServiceProxy(IChannelPtr channel)
    : TProxyBase(channel, GetServiceName())
{ }

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::ExecuteBatch()
{
    // Keep this in sync with DEFINE_RPC_PROXY_METHOD.
    return
        New<TReqExecuteBatch>(Channel, ServiceName, "Execute")
        ->SetTimeout(DefaultTimeout_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

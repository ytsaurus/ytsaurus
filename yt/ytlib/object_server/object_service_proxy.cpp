#include "stdafx.h"
#include "object_service_proxy.h"

#include <ytlib/misc/rvalue.h>

namespace NYT {
namespace NObjectServer {

using namespace NYTree;
using namespace NRpc;

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
    auto response = New<TRspExecuteBatch>(GetRequestId(), KeyToIndexes);
    auto asyncResult = response->GetAsyncResult();
    DoInvoke(response);
    return asyncResult;
}

TObjectServiceProxy::TReqExecuteBatchPtr
TObjectServiceProxy::TReqExecuteBatch::AddRequest(
    TYPathRequestPtr innerRequest,
    const Stroka& key)
{
    if (!key.empty()) {
        int index = Body.part_counts_size();
        KeyToIndexes.insert(MakePair(key, index));
    }

    if (innerRequest) {
        auto innerMessage = innerRequest->Serialize();
        const auto& innerParts = innerMessage->GetParts();
        Body.add_part_counts(static_cast<int>(innerParts.size()));
        Attachments_.insert(
            Attachments_.end(),
            innerParts.begin(),
            innerParts.end());
    } else {
        Body.add_part_counts(0);
    }

    return this;
}

int TObjectServiceProxy::TReqExecuteBatch::GetSize() const
{
    return Body.part_counts_size();
}

TBlob TObjectServiceProxy::TReqExecuteBatch::SerializeBody() const
{
    TBlob blob;
    YVERIFY(SerializeToProto(&Body, &blob));
    return blob;
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
    YVERIFY(DeserializeFromProto(&Body, data));

    int currentIndex = 0;
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

TYPathResponse::TPtr TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

TYPathResponse::TPtr TObjectServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<NYTree::TYPathResponse::TPtr> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key) const
{
    return GetResponses<TYPathResponse>(key);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceProxy::TReqExecuteBatchPtr TObjectServiceProxy::ExecuteBatch()
{
    // Keep this in sync with DEFINE_RPC_PROXY_METHOD.
    return
        New<TReqExecuteBatch>(~Channel, ServiceName, "Execute")
        ->SetTimeout(DefaultTimeout_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

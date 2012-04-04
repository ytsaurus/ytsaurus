#include "stdafx.h"
#include "cypress_service_proxy.h"

#include <ytlib/misc/rvalue.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCypressServiceProxy::TReqExecuteBatch::TReqExecuteBatch(
    IChannel::TPtr channel,
    const Stroka& path,
    const Stroka& verb)
    : TClientRequest(channel, path, verb, false)
{ }

TFuture<TCypressServiceProxy::TRspExecuteBatch::TPtr>::TPtr
TCypressServiceProxy::TReqExecuteBatch::Invoke()
{
    auto response = New<TRspExecuteBatch>(GetRequestId(), KeyToIndexes);
    auto asyncResult = response->GetAsyncResult();
    DoInvoke(~response, Timeout_);
    return asyncResult;
}

TCypressServiceProxy::TReqExecuteBatch::TPtr
TCypressServiceProxy::TReqExecuteBatch::AddRequest(
    TYPathRequestPtr innerRequest,
    const Stroka& key)
{
    if (!key.empty()) {
        int index = Body.part_counts_size();
        KeyToIndexes.insert(MakePair(key, index));
    }
    auto innerMessage = innerRequest->Serialize();
    const auto& innerParts = innerMessage->GetParts();
    Body.add_part_counts(innerParts.ysize());
    Attachments_.insert(
        Attachments_.end(),
        innerParts.begin(),
        innerParts.end());
    return this;
}

int TCypressServiceProxy::TReqExecuteBatch::GetSize() const
{
    return Body.part_counts_size();
}

TBlob TCypressServiceProxy::TReqExecuteBatch::SerializeBody() const
{
    TBlob blob;
    YVERIFY(SerializeToProto(&Body, &blob));
    return blob;
}

////////////////////////////////////////////////////////////////////////////////

TCypressServiceProxy::TRspExecuteBatch::TRspExecuteBatch(
    const TRequestId& requestId,
    const std::multimap<Stroka, int>& keyToIndexes)
    : TClientResponse(requestId)
    , KeyToIndexes(keyToIndexes)
    , AsyncResult(New< TFuture<TPtr> >())
{ }

TFuture<TCypressServiceProxy::TRspExecuteBatch::TPtr>::TPtr
TCypressServiceProxy::TRspExecuteBatch::GetAsyncResult()
{
    return AsyncResult;
}

void TCypressServiceProxy::TRspExecuteBatch::FireCompleted()
{
    AsyncResult->Set(this);
    AsyncResult.Reset();
}

void TCypressServiceProxy::TRspExecuteBatch::DeserializeBody(const TRef& data)
{
    YVERIFY(DeserializeFromProto(&Body, data));

    int currentIndex = 0;
    BeginPartIndexes.reserve(Body.part_counts_size());
    FOREACH (int partCount, Body.part_counts()) {
        BeginPartIndexes.push_back(currentIndex);
        currentIndex += partCount;
    }
}

int TCypressServiceProxy::TRspExecuteBatch::GetSize() const
{
    return Body.part_counts_size();
}

TYPathResponse::TPtr TCypressServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

TYPathResponse::TPtr TCypressServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<NYTree::TYPathResponse::TPtr> TCypressServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key) const
{
    return GetResponses<TYPathResponse>(key);
}

////////////////////////////////////////////////////////////////////////////////

TCypressServiceProxy::TReqExecuteBatch::TPtr TCypressServiceProxy::ExecuteBatch()
{
    // Keep this in sync with DEFINE_RPC_PROXY_METHOD.
    return
        New<TReqExecuteBatch>(~Channel, ServiceName, "Execute")
        ->SetTimeout(DefaultTimeout_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

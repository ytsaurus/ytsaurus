#include "stdafx.h"
#include "common.h"
#include "cypress_service_proxy.h"

#include <ytlib/misc/rvalue.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

const TYPath ObjectIdMarker = "#";
const TYPath TransactionIdMarker = "!";
const TYPath SystemPath = "#0-0-0-0";

////////////////////////////////////////////////////////////////////////////////

TYPath FromObjectId(const TObjectId& id)
{
    return ObjectIdMarker + id.ToString();
}

TYPath WithTransaction(const TYPath& path, const TTransactionId& id)
{
    return TransactionIdMarker + id.ToString() + path;
}

////////////////////////////////////////////////////////////////////////////////

TCypressServiceProxy::TReqExecuteBatch::TReqExecuteBatch(
    IChannel* channel,
    const Stroka& path,
    const Stroka& verb)
    : TClientRequest(channel, path, verb, false)
{ }

TFuture<TCypressServiceProxy::TRspExecuteBatch::TPtr>::TPtr
TCypressServiceProxy::TReqExecuteBatch::Invoke()
{
    auto response = New<TRspExecuteBatch>(GetRequestId());
    auto asyncResult = response->GetAsyncResult();
    DoInvoke(~response, Timeout_);
    return asyncResult;
}

TCypressServiceProxy::TReqExecuteBatch::TPtr
TCypressServiceProxy::TReqExecuteBatch::AddRequest(TYPathRequest* innerRequest)
{
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
    if (!SerializeProtobuf(&Body, &blob)) {
        LOG_FATAL("Error serializing request body");
    }
    return blob;
}

////////////////////////////////////////////////////////////////////////////////

TCypressServiceProxy::TRspExecuteBatch::TRspExecuteBatch(const TRequestId& requestId)
    : TClientResponse(requestId)
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
    if (!DeserializeProtobuf(&Body, data)) {
        LOG_FATAL("Error deserializing response body");
    }

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

////////////////////////////////////////////////////////////////////////////////

TCypressServiceProxy::TReqExecuteBatch::TPtr TCypressServiceProxy::ExecuteBatch()
{
    // Keep this in sync with DEFINE_RPC_PROXY_METHOD.
    return
        New<TReqExecuteBatch>(~Channel, ServiceName, "Execute")
        ->SetTimeout(Timeout_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

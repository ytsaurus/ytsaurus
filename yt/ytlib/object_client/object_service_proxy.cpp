#include "stdafx.h"
#include "object_service_proxy.h"

#include <ytlib/misc/rvalue.h>

#include <ytlib/rpc/message.h>

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
    if (Channel->GetRetryEnabled()) {
        auto startTime = TInstant::Now();
        auto timeout = GetTimeout();
        auto deadline = timeout ? startTime + timeout.Get() : TNullable<TInstant>();
        auto promise = NewPromise<TObjectServiceProxy::TRspExecuteBatchPtr>();
        SendRetryingRequest(deadline, timeout, promise);
        return promise;
    } else {
        auto batchRsp = New<TRspExecuteBatch>(GetRequestId(), KeyToIndexes);
        auto promise = batchRsp->GetAsyncResult();
        DoInvoke(batchRsp);
        return promise;
    }
}

void TObjectServiceProxy::TReqExecuteBatch::SendRetryingRequest(
    TNullable<TInstant> deadline,
    TNullable<TDuration> timeout,
    TPromise<TRspExecuteBatchPtr> promise)
{
    auto batchRsp = New<TRspExecuteBatch>(GetRequestId(), KeyToIndexes);
    Channel->Send(this, batchRsp, timeout);
    batchRsp->GetAsyncResult().Subscribe(BIND(
        &TReqExecuteBatch::OnResponse,
        MakeStrong(this),
        deadline,
        promise));
}

void TObjectServiceProxy::TReqExecuteBatch::OnResponse(
    TNullable<TInstant> deadline,
    TPromise<TRspExecuteBatchPtr> promise,
    TRspExecuteBatchPtr batchRsp)
{
    if (!batchRsp->IsOK()) {
        promise.Set(batchRsp);
        return;
    }

    bool ok = true;
    for (int index = 0; index < batchRsp->GetSize(); ++index) {
        auto rsp = batchRsp->GetResponse(index);
        auto rspError = rsp->GetError();
        if (IsRetriableError(rspError)) {
            RetryErrors.push_back(rspError);
            ok = false;
        }
    }

    if (ok) {
        promise.Set(batchRsp);
        return;
    }

    auto now = TInstant::Now();
    if (deadline && deadline.Get() > now) {
        ReportError(promise, TError("Request retries timed out"));
    } else {
        auto timeout = deadline ? now - deadline.Get() : TNullable<TDuration>();
        SendRetryingRequest(deadline, timeout, promise);
    }
}

void TObjectServiceProxy::TReqExecuteBatch::ReportError(
    TPromise<TRspExecuteBatchPtr> promise,
    TError error)
{
    error.InnerErrors() = RetryErrors;

    auto batchRsp = New<TRspExecuteBatch>(GetRequestId(), KeyToIndexes);
    auto responseHandler = IClientResponseHandlerPtr(batchRsp);
    responseHandler->OnError(error);

    promise.Set(batchRsp);
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

TSharedRef TObjectServiceProxy::TReqExecuteBatch::SerializeBody() const
{
    TSharedRef data;
    YCHECK(SerializeToProtoWithEnvelope(Body, &data));
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

////////////////////////////////////////////////////////////////////////////////

TFuture<IMessagePtr> TObjectServiceProxy::Execute(IMessagePtr innerRequestMessage)
{
    auto outerRequest = Execute();
    outerRequest->add_part_counts(static_cast<int>(innerRequestMessage->GetParts().size()));
    outerRequest->Attachments() = innerRequestMessage->GetParts();

    return outerRequest->Invoke().Apply(BIND(
        [] (TRspExecutePtr outerResponse) -> IMessagePtr {
            auto error = outerResponse->GetError();
            if (error.IsOK()) {
                return CreateMessageFromParts(outerResponse->Attachments());
            } else {
                return CreateErrorResponseMessage(error);
            }
        }));
}

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

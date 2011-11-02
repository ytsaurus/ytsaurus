#include "stdafx.h"
#include "channel.h"
#include "client.h"
#include "message.h"

#include "../misc/assert.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TChannel::TChannel(TBusClient::TPtr client)
    : Terminated(false)
{
    YASSERT(~client != NULL);

    Bus = client->CreateBus(this);
}

TChannel::TChannel(Stroka address)
    : Terminated(false)
{
    Bus = New<TBusClient>(address)->CreateBus(this);
}

TFuture<EErrorCode>::TPtr TChannel::Send(
    IClientRequest::TPtr request,
    IClientResponseHandler::TPtr responseHandler,
    TDuration timeout)
{
    YASSERT(~request != NULL);
    YASSERT(~responseHandler != NULL);

    VERIFY_THREAD_AFFINITY_ANY();

    auto requestId = request->GetRequestId();

    TActiveRequest activeRequest;
    activeRequest.RequestId = requestId;
    activeRequest.ResponseHandler = responseHandler;
    activeRequest.Ready = New< TFuture<EErrorCode> >();

    if (timeout != TDuration::Zero()) {
        activeRequest.TimeoutCookie = TDelayedInvoker::Get()->Submit(FromMethod(
            &TChannel::OnTimeout,
            TPtr(this),
            requestId),
            timeout);
    }

    auto requestMessage = request->Serialize();

    IBus::TPtr bus;

    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(!Terminated);
        YVERIFY(ActiveRequests.insert(MakePair(requestId, activeRequest)).Second());
        bus = Bus;
    }

    bus->Send(requestMessage)->Subscribe(FromMethod(
        &TChannel::OnAcknowledgement,
        TPtr(this),
        requestId));
    
    LOG_DEBUG("Request sent (RequestId: %s, ServiceName: %s, MethodName: %s)",
        ~requestId.ToString(),
        ~request->GetServiceName(),
        ~request->GetMethodName());

    return activeRequest.Ready;
}

void TChannel::Terminate()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    if (Terminated)
        return;

    YASSERT(~Bus != NULL);
    Bus->Terminate();
    Bus.Reset();
    Terminated = true;
}

void TChannel::OnMessage(
    IMessage::TPtr message,
    IBus::TPtr replyBus)
{
    VERIFY_THREAD_AFFINITY_ANY();
    UNUSED(replyBus);

    const auto& parts = message->GetParts();
    if (parts.ysize() == 0) {
        LOG_ERROR("Missing header part");
        return;
    }

    TResponseHeader header;
    DeserializeMessage(&header, parts[0]);

    auto requestId = TRequestId::FromProto(header.GetRequestId());
    
    {
        TGuard<TSpinLock> guard(&SpinLock);

        if (Terminated) {
            LOG_WARNING("Response received via a terminated channel (RequestId: %s)",
                ~requestId.ToString());
            return;
        }

        auto it = ActiveRequests.find(requestId);
        if (it == ActiveRequests.end()) {
            // This may happen when the other party responds to an already timed-out request.
            LOG_DEBUG("Response for an incorrect or obsolete request received (RequestId: %s)",
                ~requestId.ToString());
            return;
        }

        // NB: Make copies, the instance will die soon.
        auto& activeRequest = it->Second();
        auto responseHandler = activeRequest.ResponseHandler;
        auto ready = activeRequest.Ready;

        UnregisterRequest(it);

        // Don't need the guard anymore.
        guard.Release();

        auto errorCode = header.GetErrorCode();
        responseHandler->OnResponse(errorCode, message);
        ready->Set(errorCode);
    }
}

void TChannel::OnAcknowledgement(
    IBus::ESendResult sendResult,
    TRequestId requestId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    auto it = ActiveRequests.find(requestId);
    if (it == ActiveRequests.end()) {
        // This is quite typical: one may easily get the actual response before the acknowledgment.
        LOG_DEBUG("Acknowledgment for an incorrect or obsolete request received (RequestId: %s)",
            ~requestId.ToString());
        return;
    }

    // NB: Make copies, the instance will die soon.
    auto& activeRequest = it->Second();
    auto responseHandler = activeRequest.ResponseHandler;
    auto ready = activeRequest.Ready;

    if (sendResult == IBus::ESendResult::Failed) {
        UnregisterRequest(it);
        
        // Don't need the guard anymore.
        guard.Release();

        responseHandler->OnAcknowledgement(sendResult);
        ready->Set(EErrorCode::TransportError);
    } else {
        // Don't need the guard anymore.
        guard.Release();

        responseHandler->OnAcknowledgement(sendResult);
    }
}

void TChannel::OnTimeout(TRequestId requestId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    auto it = ActiveRequests.find(requestId);
    if (it == ActiveRequests.end()) {
        LOG_WARNING("Timeout of an incorrect or obsolete request occurred (RequestId: %s)",
            ~requestId.ToString());
        return;
    }

    // NB: Make copies, the instance will die soon.
    auto& activeRequest = it->Second();
    auto responseHandler = activeRequest.ResponseHandler;
    auto ready = activeRequest.Ready;

    UnregisterRequest(it);

    // Don't need the guard anymore.
    guard.Release();

    responseHandler->OnTimeout();
    ready->Set(EErrorCode::Timeout);
}

void TChannel::UnregisterRequest(TRequestMap::iterator it)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    auto& activeRequest = it->Second();
    if (activeRequest.TimeoutCookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(activeRequest.TimeoutCookie);
        activeRequest.TimeoutCookie = TDelayedInvoker::TCookie();
    }

    ActiveRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////

TChannelCache::TChannelCache()
    : IsTerminated(false)
{ } 

TChannel::TPtr TChannelCache::GetChannel(Stroka address)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // NB: double-checked locking.
    TGuard<TSpinLock> firstAttemptGuard(SpinLock);

    YASSERT(!IsTerminated);

    auto it = ChannelMap.find(address);
    if (it == ChannelMap.end()) {
        firstAttemptGuard.Release();
        auto channel = New<TChannel>(address);

        TGuard<TSpinLock> secondAttemptGuard(SpinLock);
        it = ChannelMap.find(address);
        if (it == ChannelMap.end()) {
            it = ChannelMap.insert(MakePair(address, channel)).First();
        } else {
            channel->Terminate();
        }
    }

    return it->Second();
}

void TChannelCache::Shutdown()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    if (IsTerminated)
        return;

    IsTerminated  = true;

    FOREACH (const auto& pair, ChannelMap) {
        pair.Second()->Terminate();
    }

    ChannelMap.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#include "stdafx.h"
#include "channel.h"
#include "client.h"
#include "message.h"
#include "rpc.pb.h"

#include "../misc/delayed_invoker.h"
#include "../misc/assert.h"
#include "../misc/thread_affinity.h"
#include "../actions/future.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IChannel
    , public NBus::IMessageHandler
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel(NBus::TBusClient* client);

    virtual TFuture<TError>::TPtr Send(
        TIntrusivePtr<IClientRequest> request,
        TIntrusivePtr<IClientResponseHandler> responseHandler,
        TDuration timeout);

    virtual void Terminate();

private:
    friend class TClientRequest;
    friend class TClientResponse;

    struct TActiveRequest
    {
        TRequestId RequestId;
        TIntrusivePtr<IClientResponseHandler> ResponseHandler;
        TFuture<TError>::TPtr Ready;
        TDelayedInvoker::TCookie TimeoutCookie;
    };

    typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

    volatile bool Terminated;
    NBus::IBus::TPtr Bus;
    TRequestMap ActiveRequests;
    //! Protects #ActiveRequests and #Terminated.
    TSpinLock SpinLock;

    void OnAcknowledgement(
        NBus::IBus::ESendResult sendResult,
        TRequestId requestId);

    virtual void OnMessage(
        NBus::IMessage::TPtr message,
        NBus::IBus::TPtr replyBus);

    void OnTimeout(TRequestId requestId);

    void UnregisterRequest(TRequestMap::iterator it);
};          

IChannel::TPtr CreateBusChannel(NBus::TBusClient* client)
{
    YASSERT(client != NULL);

    return New<TChannel>(client);
}

IChannel::TPtr CreateBusChannel(const Stroka& address)
{
    return New<TChannel>(~New<TBusClient>(address));
}

////////////////////////////////////////////////////////////////////////////////

TChannel::TChannel(TBusClient* client)
    : Terminated(false)
{
    Bus = client->CreateBus(this);
}

TFuture<TError>::TPtr TChannel::Send(
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
    activeRequest.Ready = New< TFuture<TError> >();

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
    
    LOG_DEBUG("Request sent (RequestId: %s, Path: %s, Verb: %s)",
        ~requestId.ToString(),
        ~request->GetPath(),
        ~request->GetVerb());

    return activeRequest.Ready;
}

void TChannel::Terminate()
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TGuard<TSpinLock> guard(SpinLock);
        if (Terminated)
            return;

        Terminated = true;
    }

    YASSERT(~Bus != NULL);
    Bus->Terminate();
    Bus.Reset();
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
    if (!DeserializeProtobuf(&header, parts[0])) {
        LOG_FATAL("Error deserializing response header");
    }

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

        TError error(header.GetErrorCode(), header.GetErrorMessage());
        responseHandler->OnResponse(error, ~message);
        ready->Set(error);
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
        ready->Set(TError(
            EErrorCode::TransportError,
            "Bus is unable to deliver the request"));
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
    ready->Set(TError(EErrorCode::Timeout, "Request timed out"));
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

} // namespace NRpc
} // namespace NYT

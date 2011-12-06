#include "stdafx.h"
#include "channel.h"
#include "client.h"
#include "message.h"
#include "rpc.pb.h"

#include "../bus/nl_client.h"

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
    , public IMessageHandler
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel(NBus::IBusClient* client)
        : Terminated(false)
    {
        Bus = client->CreateBus(this);
    }

    virtual void Send(
        IClientRequest* request,
        IClientResponseHandler* responseHandler,
        TDuration timeout)
    {
        YASSERT(request != NULL);
        YASSERT(responseHandler != NULL);

        VERIFY_THREAD_AFFINITY_ANY();

        auto requestId = request->GetRequestId();

        TActiveRequest activeRequest;
        activeRequest.RequestId = requestId;
        activeRequest.ResponseHandler = responseHandler;

        if (timeout != TDuration::Zero()) {
            activeRequest.TimeoutCookie = TDelayedInvoker::Submit(
                ~FromMethod(
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
    }

    virtual void Terminate()
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

private:
    friend class TClientRequest;
    friend class TClientResponse;

    struct TActiveRequest
    {
        TRequestId RequestId;
        TIntrusivePtr<IClientResponseHandler> ResponseHandler;
        TDelayedInvoker::TCookie TimeoutCookie;
    };

    typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

    volatile bool Terminated;
    NBus::IBus::TPtr Bus;
    TRequestMap ActiveRequests;
    //! Protects #ActiveRequests and #Terminated.
    TSpinLock SpinLock;

    void OnAcknowledgement(IBus::ESendResult sendResult, TRequestId requestId)
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

        if (sendResult == IBus::ESendResult::Failed) {
            UnregisterRequest(it);
        
            // Don't need the guard anymore.
            guard.Release();

            responseHandler->OnError(TError(
                EErrorCode::TransportError,
                "Unable to deliver the message"));
        } else {
            // Don't need the guard anymore.
            guard.Release();

            responseHandler->OnAcknowledgement();
        }
    }

    virtual void OnMessage(IMessage::TPtr message, IBus::TPtr replyBus)
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

        auto requestId = TRequestId::FromProto(header.requestid());
    
        IClientResponseHandler::TPtr responseHandler;
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

            responseHandler = it->Second().ResponseHandler;

            UnregisterRequest(it);
        }

        if (header.errorcode() == TError::OK) {
            responseHandler->OnResponse(~message);
        } else {
            responseHandler->OnError(TError(
                header.errorcode(),
                header.errormessage()));
        }
    }


    void OnTimeout(TRequestId requestId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        IClientResponseHandler::TPtr responseHandler;
        {
            TGuard<TSpinLock> guard(SpinLock);

            auto it = ActiveRequests.find(requestId);
            if (it == ActiveRequests.end()) {
                LOG_WARNING("Timeout of an incorrect or obsolete request occurred (RequestId: %s)",
                    ~requestId.ToString());
                return;
            }

            responseHandler = it->Second().ResponseHandler;

            UnregisterRequest(it);
        }

        responseHandler->OnError(TError(
            EErrorCode::Timeout,
            "Request timed out"));
    }

    void UnregisterRequest(TRequestMap::iterator it)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock);

        auto& activeRequest = it->Second();
        if (activeRequest.TimeoutCookie != TDelayedInvoker::NullCookie) {
            TDelayedInvoker::CancelAndClear(activeRequest.TimeoutCookie);
        }

        ActiveRequests.erase(it);
    }

};          

IChannel::TPtr CreateBusChannel(NBus::IBusClient* client)
{
    YASSERT(client != NULL);

    return New<TChannel>(client);
}

IChannel::TPtr CreateBusChannel(const Stroka& address)
{
    return New<TChannel>(~CreateNLBusClient(TNLBusClientConfig(address)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

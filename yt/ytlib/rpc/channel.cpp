#include "stdafx.h"
#include "channel.h"
#include "client.h"
#include "message.h"
#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/bus/nl_client.h>

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Rpc");
// TODO(babenko): consider using per-channel profiler
static NProfiling::TProfiler Profiler("/rpc/client");

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IChannel
    , public IMessageHandler
{
public:
    TChannel(IBusClient* client, TNullable<TDuration> defaultTimeout)
        : DefaultTimeout(defaultTimeout)
        , Terminated(false)
    {
        Bus = client->CreateBus(this);
    }

    virtual TNullable<TDuration> GetDefaultTimeout() const
    {
        return DefaultTimeout;
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout)
    {
        YASSERT(request);
        YASSERT(responseHandler);

        VERIFY_THREAD_AFFINITY_ANY();

        auto requestId = request->GetRequestId();

        TActiveRequest activeRequest;
        activeRequest.ClientRequest = request;
        activeRequest.ResponseHandler = responseHandler;
        activeRequest.Timer = Profiler.TimingStart("/" + request->GetPath() +
            "/" + request->GetVerb() +
            "/time");

        if (timeout) {
            activeRequest.TimeoutCookie = TDelayedInvoker::Submit(
                BIND(&TChannel::OnTimeout, MakeStrong(this), requestId),
                timeout.Get());
        }

        auto requestMessage = request->Serialize();

        IBus::TPtr bus;
        {
            TGuard<TSpinLock> guard(SpinLock);

            YASSERT(!Terminated);
            YVERIFY(ActiveRequests.insert(MakePair(requestId, activeRequest)).second);
            bus = Bus;
        }

        bus->Send(requestMessage).Subscribe(BIND(
            &TChannel::OnAcknowledgement,
            MakeStrong(this),
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

        YASSERT(Bus);
        Bus->Terminate();
        Bus.Reset();
    }

private:
    friend class TClientRequest;
    friend class TClientResponse;

    struct TActiveRequest
    {
        IClientRequestPtr ClientRequest;
        IClientResponseHandlerPtr ResponseHandler;
        TDelayedInvoker::TCookie TimeoutCookie;
        NProfiling::TTimer Timer;
    };

    typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

    TNullable<TDuration> DefaultTimeout;
    volatile bool Terminated;
    IBus::TPtr Bus;
    TRequestMap ActiveRequests;
    //! Protects #ActiveRequests and #Terminated.
    TSpinLock SpinLock;

    // TODO(babenko): make const&
    void OnAcknowledgement(TRequestId requestId, ESendResult sendResult)
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
        auto& activeRequest = it->second;
        auto responseHandler = activeRequest.ResponseHandler;

        Profiler.TimingCheckpoint(activeRequest.Timer, "ack");

        if (sendResult == ESendResult::Failed) {
            CompleteRequest(it);
        
            // Don't need the guard anymore.
            guard.Release();

            responseHandler->OnError(TError(
                EErrorCode::TransportError,
                "Unable to deliver the message"));
        } else {
            if (activeRequest.ClientRequest->IsOneWay()) {
                CompleteRequest(it);
            }

            // Don't need the guard anymore.
            guard.Release();

            responseHandler->OnAcknowledgement();
        }
    }

    virtual void OnMessage(IMessage::TPtr message, IBus::TPtr replyBus)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        UNUSED(replyBus);

        auto header = GetResponseHeader(~message);
        auto requestId = TRequestId::FromProto(header.request_id());
    
        IClientResponseHandlerPtr responseHandler;
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

            auto& activeRequest = it->second;
            Profiler.TimingCheckpoint(activeRequest.Timer, "reply");
            responseHandler = activeRequest.ResponseHandler;

            CompleteRequest(it);
        }

        auto error = TError::FromProto(header.error());
        if (error.IsOK()) {
            responseHandler->OnResponse(~message);
        } else {
            responseHandler->OnError(error);
        }
    }


    // TODO(babenko): make const&
    void OnTimeout(TRequestId requestId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        IClientResponseHandlerPtr responseHandler;
        {
            TGuard<TSpinLock> guard(SpinLock);

            auto it = ActiveRequests.find(requestId);
            if (it == ActiveRequests.end()) {
                LOG_WARNING("Timeout of an incorrect or obsolete request occurred (RequestId: %s)",
                    ~requestId.ToString());
                return;
            }

            auto& activeRequest = it->second;
            Profiler.TimingCheckpoint(activeRequest.Timer, "timeout");
            responseHandler = activeRequest.ResponseHandler;

            CompleteRequest(it);
        }

        responseHandler->OnError(TError(
            EErrorCode::Timeout,
            "Request timed out"));
    }

    void CompleteRequest(TRequestMap::iterator it)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock);

        auto& activeRequest = it->second;
        TDelayedInvoker::CancelAndClear(activeRequest.TimeoutCookie);
        Profiler.TimingStop(activeRequest.Timer);
        ActiveRequests.erase(it);
    }

};          

IChannelPtr CreateBusChannel(
    IBusClient* client,
    TNullable<TDuration> defaultTimeout)
{
    YASSERT(client);

    return New<TChannel>(client, defaultTimeout);
}

IChannelPtr CreateBusChannel(
    const Stroka& address,
    TNullable<TDuration> defaultTimeout)
{
    return CreateBusChannel(
        ~CreateNLBusClient(~New<TNLBusClientConfig>(address)),
        defaultTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

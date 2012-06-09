#include "stdafx.h"
#include "channel.h"
#include "private.h"
#include "client.h"
#include "message.h"

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/thread_affinity.h>

#include <ytlib/bus/bus.h>
#include <ytlib/bus/tcp_client.h>
#include <ytlib/bus/config.h>

#include <ytlib/ytree/ypath_client.h>

#include <ytlib/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcClientLogger;
static NProfiling::TProfiler& Profiler = RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IChannel
{
public:
    TChannel(IBusClientPtr client, TNullable<TDuration> defaultTimeout)
        : Client(MoveRV(client))
        , DefaultTimeout(defaultTimeout)
        , Terminated(false)
    {
        YASSERT(Client);
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
        VERIFY_THREAD_AFFINITY_ANY();

        auto sessionOrError = GetOrCreateSession();
        if (!sessionOrError.IsOK()) {
            responseHandler->OnError(sessionOrError);
            return;
        }

        sessionOrError.Value()->Send(request, responseHandler, timeout);
    }

    virtual void Terminate(const TError& error)
    {
        YCHECK(!error.IsOK());
        VERIFY_THREAD_AFFINITY_ANY();

        TSessionPtr session;
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (Terminated) {
                return;
            }
            session = Session;
            Session.Reset();
            Terminated = true;
        }

        if (session) {
            session->Terminate(error);
        }
    }

private:
    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    //! Provides a weak wrapper around a session and breaks the cycle
    //! between the session and its underlying bus.
    class TMessageHandler
        : public IMessageHandler
    {
    public:
        explicit TMessageHandler(TSessionPtr session)
            : Session(session)
        { }

        virtual void OnMessage(IMessagePtr message, IBusPtr replyBus)
        {
            auto session_ = Session.Lock();
            if (session_) {
                session_->OnMessage(message, replyBus);
            }
        }

    private:
        TWeakPtr<TSession> Session;

    };

    //! Directs requests sent via a channel to go through its underlying bus.
    //! Terminates when the underlying bus does so.
    class TSession
        : public IMessageHandler
    {
    public:
        explicit TSession(TNullable<TDuration> defaultTimeout)
            : DefaultTimeout(defaultTimeout)
            , Terminated(false)
        { }

        void Init(IBusPtr bus)
        {
            Bus = bus;
        }

        void Terminate(const TError& error)
        {
            TGuard<TSpinLock> guard(SpinLock);
            Terminated = true;
            TerminationError = error;
        }

        void Send(
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
            activeRequest.Timer = Profiler.TimingStart(
                "/services/" +
                EscapeYPathToken(request->GetPath()) + "/" +
                EscapeYPathToken(request->GetVerb()) +
                "/time");

            IBusPtr bus;
            {
                TGuard<TSpinLock> guard(SpinLock);

                if (Terminated) {
                    auto error = TerminationError;
                    guard.Release();

                    LOG_DEBUG("Request via terminated channel is dropped (RequestId: %s, Path: %s, Verb: %s)",
                        ~requestId.ToString(),
                        ~request->GetPath(),
                        ~request->GetVerb());

                    responseHandler->OnError(error);
                    return;
                }

                if (timeout) {
                    activeRequest.TimeoutCookie = TDelayedInvoker::Submit(
                        BIND(&TSession::OnTimeout, MakeStrong(this), requestId),
                        timeout.Get());
                }

                YCHECK(ActiveRequests.insert(MakePair(requestId, activeRequest)).second);
                bus = Bus;
            }

            auto requestMessage = request->Serialize();

            bus->Send(requestMessage).Subscribe(BIND(
                &TSession::OnAcknowledgement,
                MakeStrong(this),
                requestId));

            LOG_DEBUG("Request sent (RequestId: %s, Path: %s, Verb: %s, Timeout: %s)",
                ~requestId.ToString(),
                ~request->GetPath(),
                ~request->GetVerb(),
                ~ToString(timeout));
        }

        void OnMessage(IMessagePtr message, IBusPtr replyBus)
        {
            VERIFY_THREAD_AFFINITY_ANY();
            UNUSED(replyBus);

            auto header = GetResponseHeader(message);
            auto requestId = TRequestId::FromProto(header.request_id());

            IClientResponseHandlerPtr responseHandler;
            {
                TGuard<TSpinLock> guard(SpinLock);

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
                responseHandler->OnResponse(message);
            } else {
                responseHandler->OnError(error);
            }
        }

    private:
        IBusPtr Bus;
        TNullable<TDuration> DefaultTimeout;

        struct TActiveRequest
        {
            IClientRequestPtr ClientRequest;
            IClientResponseHandlerPtr ResponseHandler;
            TDelayedInvoker::TCookie TimeoutCookie;
            NProfiling::TTimer Timer;
        };

        typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

        TSpinLock SpinLock;
        TRequestMap ActiveRequests;
        volatile bool Terminated;
        TError TerminationError;


        void OnAcknowledgement(const TRequestId& requestId, ESendResult sendResult)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TGuard<TSpinLock> guard(SpinLock);

            auto it = ActiveRequests.find(requestId);
            if (it == ActiveRequests.end()) {
                // This one may easily get the actual response before the acknowledgment.
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

        void OnTimeout(const TRequestId& requestId)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            IClientResponseHandlerPtr responseHandler;
            {
                TGuard<TSpinLock> guard(SpinLock);

                auto it = ActiveRequests.find(requestId);
                if (it == ActiveRequests.end()) {
                    LOG_DEBUG("Timeout for an incorrect or obsolete request occurred (RequestId: %s)",
                        ~requestId.ToString());
                    return;
                }

                auto& activeRequest = it->second;
                Profiler.TimingCheckpoint(activeRequest.Timer, "timeout");
                responseHandler = activeRequest.ResponseHandler;

                CompleteRequest(it);
            }

            responseHandler->OnError(TError(EErrorCode::Timeout, "Request timed out"));
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

    IBusClientPtr Client;
    TNullable<TDuration> DefaultTimeout;

    TSpinLock SpinLock;
    volatile bool Terminated;
    TSessionPtr Session;

    TValueOrError<TSessionPtr> GetOrCreateSession()
    {
        IBusPtr bus;
        TSessionPtr session;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Session) {
                return Session;
            }

            if (Terminated) {
                return TError("Channel terminated");
            }

            Session = session = New<TSession>(DefaultTimeout);
            auto messageHandler = New<TMessageHandler>(session);

            try {
                bus = Client->CreateBus(messageHandler);
            } catch (const std::exception& ex) {
                return TError(ex.what());
            }
            session->Init(bus);
        }

        bus->SubscribeTerminated(BIND(
            &TChannel::OnBusTerminated,
            MakeWeak(this),
            MakeWeak(session)));
        return session;
    }

    void OnBusTerminated(TWeakPtr<TSession> session, TError error)
    {
        auto session_ = session.Lock();
        if (!session_) {
            return;
        }

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (Session == session_) {
                Session.Reset();
            }
        }
        
        session_->Terminate(error);
    }
};          

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateBusChannel(
    IBusClientPtr client,
    TNullable<TDuration> defaultTimeout)
{
    YASSERT(client);

    return New<TChannel>(client, defaultTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

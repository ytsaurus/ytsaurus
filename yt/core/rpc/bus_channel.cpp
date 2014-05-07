#include "stdafx.h"
#include "bus_channel.h"
#include "private.h"
#include "client.h"
#include "message.h"
#include "dispatcher.h"

#include <core/misc/singleton.h>

#include <core/actions/future.h>

#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/thread_affinity.h>

#include <core/bus/bus.h>
#include <core/bus/tcp_client.h>
#include <core/bus/config.h>

#include <core/ypath/token.h>

#include <core/ytree/yson_string.h>

#include <core/rpc/rpc.pb.h>

#include <core/profiling/profiling_manager.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcClientLogger;
static auto& Profiler = RpcClientProfiler;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TMethodDescriptor
{
    NProfiling::TTagIdList TagIds;
};

TSpinLock MethodDescriptorLock;
yhash_map<std::pair<Stroka, Stroka>, TMethodDescriptor> MethodDescriptors;

const TMethodDescriptor& GetMethodDescriptor(const Stroka& service, const Stroka& method)
{
    TGuard<TSpinLock> guard(MethodDescriptorLock);
    auto pair = std::make_pair(service, method);
    auto it = MethodDescriptors.find(pair);
    if (it == MethodDescriptors.end()) {
        TMethodDescriptor descriptor;
        auto* profilingManager = NProfiling::TProfilingManager::Get();
        descriptor.TagIds.push_back(profilingManager->RegisterTag("service", TYsonString(service)));
        descriptor.TagIds.push_back(profilingManager->RegisterTag("method", TYsonString(method)));
        it = MethodDescriptors.insert(std::make_pair(pair, descriptor)).first;
    }
    return it->second;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IChannel
{
public:
    explicit TChannel(IBusClientPtr client)
        : Client(std::move(client))
        , Terminated(false)
    {
        YCHECK(Client);
    }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout;
    }

    void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout = timeout;
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto sessionOrError = GetOrCreateSession();
        if (!sessionOrError.IsOK()) {
            responseHandler->OnError(sessionOrError);
            return;
        }

        sessionOrError.Value()->Send(
            request,
            responseHandler,
            timeout,
            requestAck);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        YCHECK(!error.IsOK());
        VERIFY_THREAD_AFFINITY_ANY();

        TSessionPtr session;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Terminated) {
                return MakeFuture();
            }

            session = Session;
            Session.Reset();

            Terminated = true;
            TerminationError = error;
        }

        if (session) {
            session->Terminate(error);
        }
        return MakeFuture();
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

        virtual void OnMessage(TSharedRefArray message, IBusPtr replyBus) override
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
            YCHECK(bus);
            Bus = bus;
        }

        void Terminate(const TError& error)
        {
            // Mark the channel as terminated to disallow any further usage.
            // Swap out all active requests and mark them as failed.
            TRequestMap activeRequests;

            {
                TGuard<TSpinLock> guard(SpinLock);
                Terminated = true;
                TerminationError = error;
                activeRequests.swap(ActiveRequests);
            }

            for (auto& pair : activeRequests) {
                const auto& requestId = pair.first;
                auto& request = pair.second;
                LOG_DEBUG("Request failed due to channel termination (RequestId: %s)",
                    ~ToString(requestId));
                FinalizeRequest(request);
                request.ResponseHandler->OnError(error);
            }
        }

        void Send(
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            TNullable<TDuration> timeout,
            bool requestAck)
        {
            YCHECK(request);
            YCHECK(responseHandler);
            VERIFY_THREAD_AFFINITY_ANY();

            auto requestId = request->GetRequestId();

            const auto& descriptor = GetMethodDescriptor(request->GetService(), request->GetMethod());

            TActiveRequest activeRequest;
            activeRequest.ClientRequest = request;
            activeRequest.Timeout = timeout;
            activeRequest.ResponseHandler = responseHandler;
            activeRequest.Timer = Profiler.TimingStart(
                "/request_time",
                descriptor.TagIds,
                NProfiling::ETimerMode::Sequential);

            IBusPtr bus;
            {
                TGuard<TSpinLock> guard(SpinLock);

                if (Terminated) {
                    auto error = TerminationError;
                    guard.Release();

                    LOG_DEBUG("Request via terminated channel is dropped (RequestId: %s, Service: %s, Method: %s)",
                        ~ToString(requestId),
                        ~request->GetService(),
                        ~request->GetMethod());

                    responseHandler->OnError(error);
                    return;
                }

                if (timeout) {
                    activeRequest.TimeoutCookie = TDelayedExecutor::Submit(
                        BIND(&TSession::OnTimeout, MakeStrong(this), requestId),
                        timeout.Get());
                }

                YCHECK(ActiveRequests.insert(std::make_pair(requestId, activeRequest)).second);
                bus = Bus;
            }

            if (request->IsRequestHeavy()) {
                BIND(&IClientRequest::Serialize, request)
                    .AsyncVia(TDispatcher::Get()->GetPoolInvoker())
                    .Run()
                    .Subscribe(BIND(
                        &TSession::OnRequestSerialized,
                        MakeStrong(this),
                        bus,
                        request,
                        timeout,
                        requestAck));
            } else {
                auto requestMessage = request->Serialize();
                OnRequestSerialized(
                    bus,
                    request,
                    timeout,
                    requestAck,
                    requestMessage);
            }
        }

        void OnMessage(TSharedRefArray message, IBusPtr replyBus)
        {
            VERIFY_THREAD_AFFINITY_ANY();
            UNUSED(replyBus);

            NProto::TResponseHeader header;
            if (!ParseResponseHeader(message, &header)) {
                LOG_ERROR("Error parsing response header");
                return;
            }

            auto requestId = FromProto<TRequestId>(header.request_id());

            TActiveRequest activeRequest;
            {
                TGuard<TSpinLock> guard(SpinLock);

                if (Terminated) {
                    LOG_WARNING("Response received via a terminated channel (RequestId: %s)",
                        ~ToString(requestId));
                    return;
                }

                auto it = ActiveRequests.find(requestId);
                if (it == ActiveRequests.end()) {
                    // This may happen when the other party responds to an already timed-out request.
                    LOG_DEBUG("Response for an incorrect or obsolete request received (RequestId: %s)",
                        ~ToString(requestId));
                    return;
                }

                activeRequest = it->second;
                Profiler.TimingCheckpoint(activeRequest.Timer, STRINGBUF("reply"));

                UnregisterRequest(it);
            }

            auto error = FromProto<TError>(header.error());
            if (error.IsOK()) {
                NotifyResponse(activeRequest, std::move(message));
            } else {
                if (error.GetCode() == EErrorCode::PoisonPill) {
                    LOG_FATAL(error, "Poison pill received");
                }
                NotifyError(activeRequest, error);
            }
        }

    private:
        IBusPtr Bus;

        TNullable<TDuration> DefaultTimeout;

        struct TActiveRequest
        {
            IClientRequestPtr ClientRequest;
            TNullable<TDuration> Timeout;
            IClientResponseHandlerPtr ResponseHandler;
            TDelayedExecutor::TCookie TimeoutCookie;
            NProfiling::TTimer Timer;
        };

        typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

        TSpinLock SpinLock;
        TRequestMap ActiveRequests;
        volatile bool Terminated;
        TError TerminationError;

        void OnRequestSerialized(
            IBusPtr bus,
            IClientRequestPtr request,
            TNullable<TDuration> timeout,
            bool requestAck,
            TSharedRefArray requestMessage)
        {
            const auto& requestId = request->GetRequestId();

            EDeliveryTrackingLevel level = requestAck
                ? EDeliveryTrackingLevel::Full
                : EDeliveryTrackingLevel::ErrorOnly;

            bus->Send(requestMessage, level).Subscribe(BIND(
                &TSession::OnAcknowledgement,
                MakeStrong(this),
                requestId));

            LOG_DEBUG("Request sent (RequestId: %s, Service: %s, Method: %s, Timeout: %s)",
                ~ToString(requestId),
                ~request->GetService(),
                ~request->GetMethod(),
                ~ToString(timeout));
        }

        void OnAcknowledgement(const TRequestId& requestId, TError error)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TGuard<TSpinLock> guard(SpinLock);

            auto it = ActiveRequests.find(requestId);
            if (it == ActiveRequests.end()) {
                // This one may easily get the actual response before the acknowledgment.
                LOG_DEBUG("Acknowledgment for an incorrect or obsolete request received (RequestId: %s)",
                    ~ToString(requestId));
                return;
            }

            // NB: Make copies, the instance will die soon.
            auto activeRequest = it->second;

            Profiler.TimingCheckpoint(activeRequest.Timer, STRINGBUF("ack"));

            if (error.IsOK()) {
                if (activeRequest.ClientRequest->IsOneWay()) {
                    UnregisterRequest(it);
                }

                // Don't need the guard anymore.
                guard.Release();

                NotifyAcknowledgement(activeRequest);
            } else {
                UnregisterRequest(it);

                // Don't need the guard anymore.
                guard.Release();

                auto wrappedError = AddErrorAttributes(
                    activeRequest,
                    TError(NRpc::EErrorCode::TransportError, "Request acknowledgment failed")
                        << error);
                NotifyError(activeRequest, wrappedError);
            }
        }

        void OnTimeout(const TRequestId& requestId)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TActiveRequest activeRequest;
            {
                TGuard<TSpinLock> guard(SpinLock);

                auto it = ActiveRequests.find(requestId);
                if (it == ActiveRequests.end()) {
                    LOG_DEBUG("Timeout for an incorrect or obsolete request occurred (RequestId: %s)",
                        ~ToString(requestId));
                    return;
                }

                activeRequest = it->second;
                Profiler.TimingCheckpoint(activeRequest.Timer, STRINGBUF("timeout"));

                UnregisterRequest(it);
            }

            auto error = AddErrorAttributes(
                activeRequest,
                TError(EErrorCode::Timeout, "Request timed out"));
            NotifyError(activeRequest, error);
        }

        void FinalizeRequest(TActiveRequest& request)
        {
            TDelayedExecutor::CancelAndClear(request.TimeoutCookie);
            Profiler.TimingStop(request.Timer, STRINGBUF("total"));
        }

        void UnregisterRequest(TRequestMap::iterator it)
        {
            VERIFY_SPINLOCK_AFFINITY(SpinLock);

            FinalizeRequest(it->second);
            ActiveRequests.erase(it);
        }


        void NotifyAcknowledgement(const TActiveRequest& activeRequest)
        {
            LOG_DEBUG("Request acknowledged (RequestId: %s)",
                ~ToString(activeRequest.ClientRequest->GetRequestId()));

            activeRequest.ResponseHandler->OnAcknowledgement();
        }

        void NotifyError(const TActiveRequest& activeRequest, const TError& error)
        {
            LOG_DEBUG(error, "Request failed (RequestId: %s)",
                ~ToString(activeRequest.ClientRequest->GetRequestId()));

            activeRequest.ResponseHandler->OnError(error);
        }

        void NotifyResponse(const TActiveRequest& activeRequest, TSharedRefArray message)
        {
            LOG_DEBUG("Response received (RequestId: %s)",
                ~ToString(activeRequest.ClientRequest->GetRequestId()));

            if (activeRequest.ClientRequest->IsResponseHeavy()) {
                TDispatcher::Get()
                    ->GetPoolInvoker()
                    ->Invoke(BIND(
                        &IClientResponseHandler::OnResponse,
                        activeRequest.ResponseHandler,
                        std::move(message)));
            } else {
                activeRequest.ResponseHandler->OnResponse(std::move(message));
            }
        }


        static TError AddErrorAttributes(const TActiveRequest& activeRequest, TError error)
        {
            error = error
                << TErrorAttribute("service", activeRequest.ClientRequest->GetService())
                << TErrorAttribute("method", activeRequest.ClientRequest->GetMethod());
            if (activeRequest.Timeout) {
                error = error
                    << TErrorAttribute("timeout", activeRequest.Timeout->MilliSeconds());
            }
            return error;
        }

    };

    IBusClientPtr Client;

    TNullable<TDuration> DefaultTimeout;

    TSpinLock SpinLock;
    volatile bool Terminated;
    TError TerminationError;
    TSessionPtr Session;

    TErrorOr<TSessionPtr> GetOrCreateSession()
    {
        IBusPtr bus;
        TSessionPtr session;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Session) {
                return Session;
            }

            if (Terminated) {
                return TError(EErrorCode::TransportError, "Channel terminated")
                    << TerminationError;
            }

            session = New<TSession>(DefaultTimeout);
            auto messageHandler = New<TMessageHandler>(session);

            try {
                bus = Client->CreateBus(messageHandler);
            } catch (const std::exception& ex) {
                return ex;
            }

            session->Init(bus);
            Session = session;
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

IChannelPtr CreateBusChannel(IBusClientPtr client)
{
    YCHECK(client);

    return New<TChannel>(std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

class TBusChannelFactory
    : public IChannelFactory
{
public:
    virtual IChannelPtr CreateChannel(const Stroka& address) override
    {
        auto config = New<TTcpBusClientConfig>(address);
        auto client = CreateTcpBusClient(config);
        return CreateBusChannel(client);
    }

};

IChannelFactoryPtr GetBusChannelFactory()
{
    return RefCountedSingleton<TBusChannelFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

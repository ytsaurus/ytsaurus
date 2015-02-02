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

#include <core/profiling/profile_manager.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;
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
        auto* profilingManager = NProfiling::TProfileManager::Get();
        descriptor.TagIds.push_back(profilingManager->RegisterTag("service", TYsonString(service)));
        descriptor.TagIds.push_back(profilingManager->RegisterTag("method", TYsonString(method)));
        it = MethodDescriptors.insert(std::make_pair(pair, descriptor)).first;
    }
    return it->second;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TBusChannel
    : public IChannel
{
public:
    explicit TBusChannel(IBusClientPtr client)
        : Client_(std::move(client))
    {
        YCHECK(Client_);
    }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout_;
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout_ = timeout;
    }

    virtual TYsonString GetEndpointDescription() const override
    {
        return Client_->GetEndpointDescription();
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto sessionOrError = GetOrCreateSession();
        if (!sessionOrError.IsOK()) {
            responseHandler->HandleError(sessionOrError);
            return nullptr;
        }

        return sessionOrError.Value()->Send(
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        YCHECK(!error.IsOK());
        VERIFY_THREAD_AFFINITY_ANY();

        TSessionPtr session;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Terminated_) {
                return VoidFuture;
            }

            session = Session_;
            Session_.Reset();

            Terminated_ = true;
            TerminationError_ = error;
        }

        if (session) {
            session->Terminate(error);
        }

        return VoidFuture;
    }

private:
    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    class TClientRequestControl;
    typedef TIntrusivePtr<TClientRequestControl> TClientRequestControlPtr;

    const IBusClientPtr Client_;

    TNullable<TDuration> DefaultTimeout_;

    TSpinLock SpinLock_;
    volatile bool Terminated_ = false;
    TError TerminationError_;
    TSessionPtr Session_;


    TErrorOr<TSessionPtr> GetOrCreateSession()
    {
        IBusPtr bus;
        TSessionPtr session;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Session_) {
                return Session_;
            }

            if (Terminated_) {
                return TError(NRpc::EErrorCode::TransportError, "Channel terminated")
                    << TerminationError_;
            }

            session = New<TSession>(DefaultTimeout_);
            auto messageHandler = New<TMessageHandler>(session);

            try {
                bus = Client_->CreateBus(messageHandler);
            } catch (const std::exception& ex) {
                return ex;
            }

            session->Initialize(bus);
            Session_ = session;
        }

        bus->SubscribeTerminated(BIND(
            &TBusChannel::OnBusTerminated,
            MakeWeak(this),
            MakeWeak(session)));
        return session;
    }

    void OnBusTerminated(TWeakPtr<TSession> session, const TError& error)
    {
        auto session_ = session.Lock();
        if (!session_) {
            return;
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Session_ == session_) {
                Session_.Reset();
            }
        }

        session_->Terminate(error);
    }


    //! Provides a weak wrapper around a session and breaks the cycle
    //! between the session and its underlying bus.
    class TMessageHandler
        : public IMessageHandler
    {
    public:
        explicit TMessageHandler(TSessionPtr session)
            : Session_(session)
        { }

        virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) override
        {
            auto session_ = Session_.Lock();
            if (session_) {
                session_->HandleMessage(message, replyBus);
            }
        }

    private:
        const TWeakPtr<TSession> Session_;

    };

    //! Directs requests sent via a channel to go through its underlying bus.
    //! Terminates when the underlying bus does so.
    class TSession
        : public IMessageHandler
    {
    public:
        explicit TSession(TNullable<TDuration> defaultTimeout)
            : DefaultTimeout_(defaultTimeout)
        { }

        void Initialize(IBusPtr bus)
        {
            YCHECK(bus);
            Bus_ = bus;
        }

        void Terminate(const TError& error)
        {
            // Mark the channel as terminated to disallow any further usage.
            // Swap out all active requests and mark them as failed.
            TActiveRequestMap activeRequests;

            {
                TGuard<TSpinLock> guard(SpinLock_);
                Terminated_ = true;
                TerminationError_ = error;
                activeRequests.swap(ActiveRequestMap_);
            }

            for (auto& pair : activeRequests) {
                const auto& requestId = pair.first;
                const auto& requestControl = pair.second;
                LOG_DEBUG(error, "Request failed due to channel termination (RequestId: %v)",
                    requestId);
                requestControl->GetResponseHandler()->HandleError(error);
                requestControl->Finalize();
            }
        }

        IClientRequestControlPtr Send(
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            TNullable<TDuration> timeout,
            bool requestAck)
        {
            YCHECK(request);
            YCHECK(responseHandler);
            VERIFY_THREAD_AFFINITY_ANY();

            auto requestId = request->GetRequestId();

            auto requestControl = New<TClientRequestControl>(
                this,
                request,
                timeout,
                responseHandler);

            IBusPtr bus;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                if (Terminated_) {
                    auto error = TerminationError_;
                    guard.Release();

                    LOG_DEBUG("Request via terminated channel is dropped (RequestId: %v, Method: %v:%v)",
                        requestId,
                        request->GetService(),
                        request->GetMethod());

                    responseHandler->HandleError(error);
                    return nullptr;
                }

                YCHECK(ActiveRequestMap_.insert(std::make_pair(requestId, requestControl)).second);
                bus = Bus_;
            }

            if (request->IsRequestHeavy()) {
                BIND(&IClientRequest::Serialize, request)
                    .AsyncVia(TDispatcher::Get()->GetInvoker())
                    .Run()
                    .Subscribe(BIND(
                        &TSession::OnRequestSerialized,
                        MakeStrong(this),
                        std::move(bus),
                        request,
                        timeout,
                        requestAck));
            } else {
                auto requestMessage = request->Serialize();
                OnRequestSerialized(
                    std::move(bus),
                    std::move(request),
                    timeout,
                    requestAck,
                    std::move(requestMessage));
            }

            return requestControl;
        }

        void Cancel(TClientRequestControlPtr requestControl)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            const auto& requestId = requestControl->GetRequestId();
            IClientRequestPtr request;
            IClientResponseHandlerPtr responseHandler;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    LOG_DEBUG("Attempt to cancel an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                request = requestControl->GetRequest();
                responseHandler = requestControl->GetResponseHandler();
                requestControl->TimingCheckpoint(STRINGBUF("cancel"));
                requestControl->Finalize();
                ActiveRequestMap_.erase(it);
            }

            LOG_DEBUG("Request canceled (RequestId: %v)",
                requestId);

            NotifyError(
                requestControl,
                request,
                responseHandler,
                TError(NYT::EErrorCode::Canceled, "Request canceled"));

            IBusPtr bus;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                if (Terminated_)
                    return;

                bus = Bus_;
            }

            NProto::TRequestCancelationHeader header;
            ToProto(header.mutable_request_id(), requestId);
            header.set_service(request->GetService());
            header.set_method(request->GetMethod());
            if (request->GetRealmId() != NullRealmId) {
                ToProto(header.mutable_realm_id(), request->GetRealmId());
            }

            auto message = CreateRequestCancelationMessage(header);
            bus->Send(std::move(message), EDeliveryTrackingLevel::None);
        }

        void HandleTimeout(const TRequestId& requestId)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TClientRequestControlPtr requestControl;
            IClientRequestPtr request;
            IClientResponseHandlerPtr responseHandler;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    LOG_DEBUG("Timeout occurred for an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl = it->second;
                request = requestControl->GetRequest();
                responseHandler = requestControl->GetResponseHandler();
                requestControl->TimingCheckpoint(STRINGBUF("timeout"));
                requestControl->Finalize();
                ActiveRequestMap_.erase(it);
            }

            NotifyError(
                requestControl,
                request,
                responseHandler,
                TError(NYT::EErrorCode::Timeout, "Request timed out"));
        }

        void HandleMessage(TSharedRefArray message, IBusPtr /*replyBus*/)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            NProto::TResponseHeader header;
            if (!ParseResponseHeader(message, &header)) {
                LOG_ERROR("Error parsing response header");
                return;
            }

            auto requestId = FromProto<TRequestId>(header.request_id());

            TClientRequestControlPtr requestControl;
            IClientRequestPtr request;
            IClientResponseHandlerPtr responseHandler;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                if (Terminated_) {
                    LOG_WARNING("Response received via a terminated channel (RequestId: %v)",
                        requestId);
                    return;
                }

                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    // This may happen when the other party responds to an already timed-out request.
                    LOG_DEBUG("Response for an incorrect or obsolete request received (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl = it->second;
                request = requestControl->GetRequest();
                responseHandler = requestControl->GetResponseHandler();
                requestControl->TimingCheckpoint(STRINGBUF("reply"));
                requestControl->Finalize();
                ActiveRequestMap_.erase(it);
            }

            {
                TError error;
                if (header.has_error()) {
                    error = FromProto<TError>(header.error());
                }
                if (error.IsOK()) {
                    NotifyResponse(request, responseHandler, std::move(message));
                } else {
                    if (error.GetCode() == EErrorCode::PoisonPill) {
                        LOG_FATAL(error, "Poison pill received");
                    }
                    NotifyError(requestControl, request, responseHandler, error);
                }
            }
        }

    private:
        const TNullable<TDuration> DefaultTimeout_;

        IBusPtr Bus_;

        TSpinLock SpinLock_;
        typedef yhash_map<TRequestId, TClientRequestControlPtr> TActiveRequestMap;
        TActiveRequestMap ActiveRequestMap_;
        volatile bool Terminated_ = false;
        TError TerminationError_;


        void OnRequestSerialized(
            IBusPtr bus,
            IClientRequestPtr request,
            TNullable<TDuration> timeout,
            bool requestAck,
            const TErrorOr<TSharedRefArray>& requestMessageOrError)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            const auto& requestId = request->GetRequestId();

            if (!requestMessageOrError.IsOK()) {
                TGuard<TSpinLock> guard(SpinLock_);
                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    // This one may easily get the actual response before the acknowledgment.
                    LOG_DEBUG(requestMessageOrError, "Failed to serialize an incorrect or obsolete request (RequestId: %v)",
                        requestId);
                } else {
                    // NB: Make copies, the instance will die soon.
                    auto requestControl = it->second;
                    auto request = requestControl->GetRequest();
                    auto responseHandler = requestControl->GetResponseHandler();
                    requestControl->Finalize();
                    ActiveRequestMap_.erase(it);

                    // Don't need the guard anymore.
                    guard.Release();

                    NotifyError(
                        requestControl,
                        request,
                        responseHandler,
                        TError(NRpc::EErrorCode::TransportError, "Request serialization failed")
                            << requestMessageOrError);
                }
                return;
            }

            const auto& requestMessage = requestMessageOrError.Value();

            auto level = requestAck
                ? EDeliveryTrackingLevel::Full
                : EDeliveryTrackingLevel::ErrorOnly;

            bus->Send(requestMessage, level).Subscribe(BIND(
                &TSession::OnAcknowledgement,
                MakeStrong(this),
                requestId));

            LOG_DEBUG("Request sent (RequestId: %v, Method: %v:%v, Timeout: %v, TrackingLevel: %v)",
                requestId,
                request->GetService(),
                request->GetMethod(),
                timeout,
                level);
        }

        void OnAcknowledgement(const TRequestId& requestId, const TError& error)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TClientRequestControlPtr requestControl;
            IClientRequestPtr request;
            IClientResponseHandlerPtr responseHandler;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    // This one may easily get the actual response before the acknowledgment.
                    LOG_DEBUG("Acknowledgment received for an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl = it->second;
                request = requestControl->GetRequest();
                responseHandler = requestControl->GetResponseHandler();
                requestControl->TimingCheckpoint(STRINGBUF("ack"));
                if (!error.IsOK() || request->IsOneWay()) {
                    requestControl->Finalize();
                    ActiveRequestMap_.erase(it);
                }
            }

            if (error.IsOK()) {
                NotifyAcknowledgement(request, responseHandler);
            } else {
                NotifyError(
                    requestControl,
                    request,
                    responseHandler,
                    TError(NRpc::EErrorCode::TransportError, "Request acknowledgment failed")
                         << error);
            }
        }


        void NotifyAcknowledgement(
            const IClientRequestPtr& request,
            const IClientResponseHandlerPtr& responseHandler)
        {
            LOG_DEBUG("Request acknowledged (RequestId: %v)",
                request->GetRequestId());

            responseHandler->HandleAcknowledgement();
        }

        void NotifyError(
            const TClientRequestControlPtr& requestControl,
            const IClientRequestPtr& request,
            const IClientResponseHandlerPtr& responseHandler,
            const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("request_id", request->GetRequestId())
                << TErrorAttribute("service", request->GetService())
                << TErrorAttribute("method", request->GetMethod())
                << TErrorAttribute("endpoint", Bus_->GetEndpointDescription());

            auto timeout = requestControl->GetTimeout();
            if (timeout) {
                detailedError = detailedError
                    << TErrorAttribute("timeout", *timeout);
            }

            LOG_DEBUG(detailedError, "Request failed (RequestId: %v)",
                request->GetRequestId());

            responseHandler->HandleError(detailedError);
        }

        void NotifyResponse(
            const IClientRequestPtr& request,
            const IClientResponseHandlerPtr& responseHandler,
            TSharedRefArray message)
        {
            LOG_DEBUG("Response received (RequestId: %v)",
                request->GetRequestId());

            if (request->IsResponseHeavy()) {
                TDispatcher::Get()
                    ->GetInvoker()
                    ->Invoke(
                BIND(
                    &IClientResponseHandler::HandleResponse,
                    responseHandler,
                    std::move(message)));
            } else {
                responseHandler->HandleResponse(std::move(message));
            }
        }

    };

    //! Controls a sent request.
    class TClientRequestControl
        : public IClientRequestControl
    {
    public:
        TClientRequestControl(
            TSessionPtr session,
            IClientRequestPtr request,
            TNullable<TDuration> timeout,
            IClientResponseHandlerPtr responseHandler)
            : Session_(std::move(session))
            , Request_(std::move(request))
            , RequestId_(Request_->GetRequestId())
            , Timeout_(timeout)
            , ResponseHandler_(std::move(responseHandler))
        {
            const auto& descriptor = GetMethodDescriptor(Request_->GetService(), Request_->GetMethod());
            Timer_ = Profiler.TimingStart(
                "/request_time",
                descriptor.TagIds,
                NProfiling::ETimerMode::Sequential);

            if (Timeout_) {
                TimeoutCookie_ = TDelayedExecutor::Submit(
                    BIND(&TSession::HandleTimeout, Session_, RequestId_),
                    *Timeout_);
            }
        }

        const IClientRequestPtr& GetRequest() const
        {
            return Request_;
        }

        const TRequestId& GetRequestId() const
        {
            return RequestId_;
        }

        TNullable<TDuration> GetTimeout() const
        {
            return Timeout_;
        }

        const IClientResponseHandlerPtr& GetResponseHandler() const
        {
            return ResponseHandler_;
        }

        void TimingCheckpoint(const TStringBuf& key)
        {
            Profiler.TimingCheckpoint(Timer_, key);
        }

        void Finalize()
        {
            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
            Profiler.TimingStop(Timer_, STRINGBUF("total"));
            Request_.Reset();
            ResponseHandler_.Reset();
        }

        virtual void Cancel() override
        {
            Session_->Cancel(this);
        }

    private:
        const TSessionPtr Session_;
        IClientRequestPtr Request_;
        const TRequestId RequestId_;
        const TNullable<TDuration> Timeout_;
        IClientResponseHandlerPtr ResponseHandler_;

        TDelayedExecutorCookie TimeoutCookie_;
        NProfiling::TTimer Timer_;

    };

};

IChannelPtr CreateBusChannel(IBusClientPtr client)
{
    YCHECK(client);

    return New<TBusChannel>(std::move(client));
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

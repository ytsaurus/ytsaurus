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

class TChannel
    : public IChannel
{
public:
    explicit TChannel(IBusClientPtr client)
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

    //! Provides a weak wrapper around a session and breaks the cycle
    //! between the session and its underlying bus.
    class TMessageHandler
        : public IMessageHandler
    {
    public:
        explicit TMessageHandler(TSessionPtr session)
            : Session_(session)
        { }

        virtual void OnMessage(TSharedRefArray message, IBusPtr replyBus) override
        {
            auto session_ = Session_.Lock();
            if (session_) {
                session_->OnMessage(message, replyBus);
            }
        }

    private:
        TWeakPtr<TSession> Session_;

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

        void Init(IBusPtr bus)
        {
            YCHECK(bus);
            Bus_ = bus;
        }

        void Terminate(const TError& error)
        {
            // Mark the channel as terminated to disallow any further usage.
            // Swap out all active requests and mark them as failed.
            TRequestMap activeRequests;

            {
                TGuard<TSpinLock> guard(SpinLock_);
                Terminated_ = true;
                TerminationError_ = error;
                activeRequests.swap(ActiveRequests_);
            }

            for (auto& pair : activeRequests) {
                const auto& requestId = pair.first;
                auto& request = pair.second;
                LOG_DEBUG("Request failed due to channel termination (RequestId: %v)",
                    requestId);
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
                TGuard<TSpinLock> guard(SpinLock_);

                if (Terminated_) {
                    auto error = TerminationError_;
                    guard.Release();

                    LOG_DEBUG("Request via terminated channel is dropped (RequestId: %v, Service: %v, Method: %v)",
                        requestId,
                        request->GetService(),
                        request->GetMethod());

                    responseHandler->OnError(error);
                    return;
                }

                if (timeout) {
                    activeRequest.TimeoutCookie = TDelayedExecutor::Submit(
                        BIND(&TSession::OnTimeout, MakeStrong(this), requestId),
                        timeout.Get());
                }

                YCHECK(ActiveRequests_.insert(std::make_pair(requestId, activeRequest)).second);
                bus = Bus_;
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
                TGuard<TSpinLock> guard(SpinLock_);

                if (Terminated_) {
                    LOG_WARNING("Response received via a terminated channel (RequestId: %v)",
                        requestId);
                    return;
                }

                auto it = ActiveRequests_.find(requestId);
                if (it == ActiveRequests_.end()) {
                    // This may happen when the other party responds to an already timed-out request.
                    LOG_DEBUG("Response for an incorrect or obsolete request received (RequestId: %v)",
                        requestId);
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
        IBusPtr Bus_;

        TNullable<TDuration> DefaultTimeout_;

        struct TActiveRequest
        {
            IClientRequestPtr ClientRequest;
            TNullable<TDuration> Timeout;
            IClientResponseHandlerPtr ResponseHandler;
            TDelayedExecutorCookie TimeoutCookie;
            NProfiling::TTimer Timer;
        };

        typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

        TSpinLock SpinLock_;
        TRequestMap ActiveRequests_;
        volatile bool Terminated_ = false;
        TError TerminationError_;

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

            LOG_DEBUG("Request sent (RequestId: %v, Service: %v, Method: %v, Timeout: %v)",
                requestId,
                request->GetService(),
                request->GetMethod(),
                timeout);
        }

        void OnAcknowledgement(const TRequestId& requestId, TError error)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TGuard<TSpinLock> guard(SpinLock_);

            auto it = ActiveRequests_.find(requestId);
            if (it == ActiveRequests_.end()) {
                // This one may easily get the actual response before the acknowledgment.
                LOG_DEBUG("Acknowledgment for an incorrect or obsolete request received (RequestId: %v)",
                    requestId);
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

                auto detailedError = TError(
                    NRpc::EErrorCode::TransportError,
                    "Request acknowledgment failed") <<
                    error;
                NotifyError(activeRequest, detailedError);
            }
        }

        void OnTimeout(const TRequestId& requestId)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TActiveRequest activeRequest;
            {
                TGuard<TSpinLock> guard(SpinLock_);

                auto it = ActiveRequests_.find(requestId);
                if (it == ActiveRequests_.end()) {
                    LOG_DEBUG("Timeout for an incorrect or obsolete request occurred (RequestId: %v)",
                        requestId);
                    return;
                }

                activeRequest = it->second;
                Profiler.TimingCheckpoint(activeRequest.Timer, STRINGBUF("timeout"));

                UnregisterRequest(it);
            }

            auto error = TError(NRpc::EErrorCode::Timeout, "Request timed out");
            NotifyError(activeRequest, error);
        }

        void FinalizeRequest(TActiveRequest& request)
        {
            TDelayedExecutor::CancelAndClear(request.TimeoutCookie);
            Profiler.TimingStop(request.Timer, STRINGBUF("total"));
        }

        void UnregisterRequest(TRequestMap::iterator it)
        {
            VERIFY_SPINLOCK_AFFINITY(SpinLock_);

            FinalizeRequest(it->second);
            ActiveRequests_.erase(it);
        }


        void NotifyAcknowledgement(const TActiveRequest& activeRequest)
        {
            LOG_DEBUG("Request acknowledged (RequestId: %v)",
                activeRequest.ClientRequest->GetRequestId());

            activeRequest.ResponseHandler->OnAcknowledgement();
        }

        void NotifyError(const TActiveRequest& activeRequest, const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("request_id", activeRequest.ClientRequest->GetRequestId())
                << TErrorAttribute("service", activeRequest.ClientRequest->GetService())
                << TErrorAttribute("method", activeRequest.ClientRequest->GetMethod())
                << TErrorAttribute("endpoint", Bus_->GetEndpointDescription());
            if (activeRequest.Timeout) {
                detailedError = detailedError
                    << TErrorAttribute("timeout", activeRequest.Timeout->MilliSeconds());
            }

            LOG_DEBUG(detailedError, "Request failed (RequestId: %v)",
                activeRequest.ClientRequest->GetRequestId());

            activeRequest.ResponseHandler->OnError(detailedError);
        }

        void NotifyResponse(const TActiveRequest& activeRequest, TSharedRefArray message)
        {
            LOG_DEBUG("Response received (RequestId: %v)",
                activeRequest.ClientRequest->GetRequestId());

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

    };

    IBusClientPtr Client_;

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

            session->Init(bus);
            Session_ = session;
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
            TGuard<TSpinLock> guard(SpinLock_);
            if (Session_ == session_) {
                Session_.Reset();
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

#include "channel.h"

#include <yt/core/actions/future.h>

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/private.h>

#include <yt/core/bus/bus.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/client.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/tls_cache.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/proto/rpc.pb.h>

#include <array>

namespace NYT {
namespace NRpc {
namespace NBus {

using namespace NYT::NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;
static const auto& Profiler = RpcClientProfiler;

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

    virtual const TString& GetEndpointDescription() const override
    {
        return Client_->GetEndpointDescription();
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Client_->GetEndpointAttributes();
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TSessionPtr session;

        try {
            session = GetOrCreateSession(options.MultiplexingBand);
        } catch (const std::exception& ex) {
            responseHandler->HandleError(TError(ex));
            return nullptr;
        }

        return session->Send(
            std::move(request),
            std::move(responseHandler),
            options);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        YCHECK(!error.IsOK());
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TSessionPtr> sessions;
        {
            TWriterGuard guard(SpinLock_);

            if (Terminated_) {
                return VoidFuture;
            }

            for (auto& session : Sessions_) {
                if (session) {
                    sessions.push_back(session);
                    session.Reset();
                }
            }

            Terminated_ = true;
            TerminationError_ = error;
        }

        for (const auto& session : sessions) {
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

    TReaderWriterSpinLock SpinLock_;
    bool Terminated_ = false;
    TError TerminationError_;
    TEnumIndexedVector<TSessionPtr, EMultiplexingBand> Sessions_;

    TSessionPtr* GetPerBandSession(EMultiplexingBand band)
    {
        return &Sessions_[band];
    }

    TSessionPtr GetOrCreateSession(EMultiplexingBand band)
    {
        auto* perBandSession = GetPerBandSession(band);

        // Fast path.
        {
            TReaderGuard guard(SpinLock_);

            if (*perBandSession) {
                return *perBandSession;
            }
        }

        IBusPtr bus;
        TSessionPtr session;

        // Slow path.
        {
            TWriterGuard guard(SpinLock_);

            if (*perBandSession) {
                return *perBandSession;
            }

            if (Terminated_) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::TransportError, "Channel terminated")
                    << TerminationError_;
            }

            session = New<TSession>(band);

            auto messageHandler = New<TMessageHandler>(session);
            bus = Client_->CreateBus(messageHandler);

            session->Initialize(bus);

            *perBandSession = session;
        }

        bus->SubscribeTerminated(BIND(
            &TBusChannel::OnBusTerminated,
            MakeWeak(this),
            MakeWeak(session),
            band));
        return session;
    }

    void OnBusTerminated(const TWeakPtr<TSession>& session, EMultiplexingBand band, const TError& error)
    {
        auto session_ = session.Lock();
        if (!session_) {
            return;
        }

        {
            TWriterGuard guard(SpinLock_);

            auto* perBandSession = GetPerBandSession(band);
            if (*perBandSession == session_) {
                perBandSession->Reset();
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
            : Session_(std::move(session))
        { }

        virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) throw() override
        {
            auto session_ = Session_.Lock();
            if (session_) {
                session_->HandleMessage(std::move(message), std::move(replyBus));
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
        explicit TSession(EMultiplexingBand band)
            : TosLevel_(TDispatcher::Get()->GetTosLevelForBand(band))
        { }

        void Initialize(IBusPtr bus)
        {
            Y_ASSERT(bus);
            Bus_ = std::move(bus);
            Bus_->SetTosLevel(TosLevel_);
        }

        void Terminate(const TError& error)
        {
            std::vector<std::tuple<TClientRequestControlPtr, IClientResponseHandlerPtr>> existingRequests;

            // Mark the channel as terminated to disallow any further usage.
            {
                auto guard = Guard(SpinLock_);
                Terminated_ = true;
                TerminationError_ = error;

                existingRequests.reserve(ActiveRequestMap_.size());
                for (const auto& pair : ActiveRequestMap_) {
                    auto& requestControl = pair.second;

                    IClientResponseHandlerPtr responseHandler;
                    requestControl->Finalize(guard, &responseHandler);

                    existingRequests.emplace_back(std::move(requestControl), std::move(responseHandler));
                }

                ActiveRequestMap_.clear();
            }

            for (const auto& existingRequest : existingRequests) {
                NotifyError(
                    std::get<0>(existingRequest),
                    std::get<1>(existingRequest),
                    AsStringBuf("Request failed due to channel termination"),
                    error);
            }
        }

        IClientRequestControlPtr Send(
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            const TSendOptions& options)
        {
            YCHECK(request);
            YCHECK(responseHandler);
            VERIFY_THREAD_AFFINITY_ANY();

            auto requestControl = New<TClientRequestControl>(
                this,
                request,
                options.Timeout,
                std::move(responseHandler));

            auto& header = request->Header();
            header.set_start_time(ToProto<i64>(TInstant::Now()));

            // NB: Requests without timeout are rare but may occur.
            // For these requests we still need to register a timeout cookie with TDelayedExecutor
            // since this also provides proper cleanup and cancelation when global shutdown happens.
            auto effectiveTimeout = options.Timeout.Get(TDuration::Hours(24));
            auto timeoutCookie = TDelayedExecutor::Submit(
                BIND(&TSession::HandleTimeout, MakeWeak(this), requestControl),
                effectiveTimeout);
            requestControl->SetTimeoutCookie(Guard(SpinLock_), std::move(timeoutCookie));

            if (options.Timeout) {
                header.set_timeout(ToProto<i64>(*options.Timeout));
            } else {
                header.clear_timeout();
            }

            if (request->IsHeavy()) {
                BIND(&IClientRequest::Serialize, request)
                    .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
                    .Run()
                    .Subscribe(BIND(
                        &TSession::OnRequestSerialized,
                        MakeStrong(this),
                        requestControl,
                        options));
            } else {
                auto&& requestMessage = request->Serialize();
                OnRequestSerialized(
                    requestControl,
                    options,
                    std::move(requestMessage));
            }

            return requestControl;
        }

        void Cancel(const TClientRequestControlPtr& requestControl)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            const auto& requestId = requestControl->GetRequestId();

            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(SpinLock_);

                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    LOG_DEBUG("Attempt to cancel an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                if (requestControl != it->second) {
                    LOG_DEBUG("Attempt to cancel a resent request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl->ProfileCancel();
                requestControl->Finalize(guard, &responseHandler);
                ActiveRequestMap_.erase(it);
            }

            NotifyError(
                requestControl,
                responseHandler,
                AsStringBuf("Request canceled"),
                TError(NYT::EErrorCode::Canceled, "Request canceled"));

            IBusPtr bus;
            {
                auto guard = Guard(SpinLock_);

                if (Terminated_) {
                    return;
                }

                bus = Bus_;
            }

            const auto& realmId = requestControl->GetRealmId();
            const auto& service = requestControl->GetService();
            const auto& method = requestControl->GetMethod();

            NProto::TRequestCancelationHeader header;
            ToProto(header.mutable_request_id(), requestId);
            header.set_service(service);
            header.set_method(method);
            ToProto(header.mutable_realm_id(), realmId);

            auto message = CreateRequestCancelationMessage(header);
            bus->Send(std::move(message), NBus::TSendOptions(EDeliveryTrackingLevel::None));
        }

        void HandleTimeout(const TClientRequestControlPtr& requestControl, bool aborted)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            const auto& requestId = requestControl->GetRequestId();

            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(SpinLock_);

                if (!requestControl->IsActive(guard)) {
                    return;
                }

                auto it = ActiveRequestMap_.find(requestId);
                if (it != ActiveRequestMap_.end() && requestControl == it->second) {
                    ActiveRequestMap_.erase(it);
                } else {
                    LOG_DEBUG("Timeout occurred for an unknown or resent request (RequestId: %v)",
                        requestId);
                }

                requestControl->ProfileTimeout();
                requestControl->Finalize(guard, &responseHandler);
            }

            NotifyError(
                requestControl,
                responseHandler,
                AsStringBuf("Request timed out"),
                TError(NYT::EErrorCode::Timeout, aborted
                    ? "Request timed out or timer was aborted"
                    : "Request timed out"));
        }

        virtual void HandleMessage(TSharedRefArray message, IBusPtr /*replyBus*/) throw() override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            NProto::TResponseHeader header;
            if (!ParseResponseHeader(message, &header)) {
                LOG_ERROR("Error parsing response header");
                return;
            }

            auto requestId = FromProto<TRequestId>(header.request_id());

            TClientRequestControlPtr requestControl;
            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(SpinLock_);

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

                requestControl = std::move(it->second);
                requestControl->ProfileReply(message);
                requestControl->Finalize(guard, &responseHandler);
                ActiveRequestMap_.erase(it);
            }

            {
                TError error;
                if (header.has_error()) {
                    error = FromProto<TError>(header.error());
                }
                if (error.IsOK()) {
                    NotifyResponse(
                        requestId,
                        requestControl,
                        responseHandler,
                        std::move(message));
                } else {
                    if (error.GetCode() == EErrorCode::PoisonPill) {
                        LOG_FATAL(error, "Poison pill received");
                    }
                    NotifyError(
                        requestControl,
                        responseHandler,
                        AsStringBuf("Request failed"),
                        error);
                }
            }
        }


        //! Cached method metdata.
        struct TMethodMetadata
        {
            NProfiling::TAggregateGauge AckTimeCounter;
            NProfiling::TAggregateGauge ReplyTimeCounter;
            NProfiling::TAggregateGauge TimeoutTimeCounter;
            NProfiling::TAggregateGauge CancelTimeCounter;
            NProfiling::TAggregateGauge TotalTimeCounter;
            NProfiling::TMonotonicCounter RequestMessageBodySizeCounter;
            NProfiling::TMonotonicCounter RequestMessageAttachmentSizeCounter;
            NProfiling::TMonotonicCounter ResponseMessageBodySizeCounter;
            NProfiling::TMonotonicCounter ResponseMessageAttachmentSizeCounter;
        };

        struct TMethodMetadataProfilingTrait
        {
            using TKey = std::pair<TString, TString>;
            using TValue = TMethodMetadata;

            static TKey ToKey(const TString& service, const TString& method)
            {
                return std::make_pair(service, method);
            }

            static TValue ToValue(const TString& service, const TString& method)
            {
                TMethodMetadata metadata;

                auto* profilingManager = NProfiling::TProfileManager::Get();
                NProfiling::TTagIdList tagIds{
                    profilingManager->RegisterTag("service", TYsonString(service)),
                    profilingManager->RegisterTag("method", TYsonString(method))
                };
                metadata.AckTimeCounter = NProfiling::TAggregateGauge("/request_time/ack", tagIds, NProfiling::EAggregateMode::All);
                metadata.ReplyTimeCounter = NProfiling::TAggregateGauge("/request_time/reply", tagIds, NProfiling::EAggregateMode::All);
                metadata.TimeoutTimeCounter = NProfiling::TAggregateGauge("/request_time/timeout", tagIds, NProfiling::EAggregateMode::All);
                metadata.CancelTimeCounter = NProfiling::TAggregateGauge("/request_time/cancel", tagIds, NProfiling::EAggregateMode::All);
                metadata.TotalTimeCounter = NProfiling::TAggregateGauge("/request_time/total", tagIds, NProfiling::EAggregateMode::All);
                metadata.RequestMessageBodySizeCounter = NProfiling::TMonotonicCounter("/request_message_body_bytes", tagIds);
                metadata.RequestMessageAttachmentSizeCounter = NProfiling::TMonotonicCounter("/request_message_attachment_bytes", tagIds);
                metadata.ResponseMessageBodySizeCounter = NProfiling::TMonotonicCounter("/response_message_body_bytes", tagIds);
                metadata.ResponseMessageAttachmentSizeCounter = NProfiling::TMonotonicCounter("/response_message_attachment_bytes", tagIds);

                return metadata;
            }
        };

        TMethodMetadata* GetMethodMetadata(const TString& service, const TString& method)
        {
            return &GetLocallyGloballyCachedValue<TMethodMetadataProfilingTrait>(service, method);
        }

    private:
        const TTosLevel TosLevel_;

        IBusPtr Bus_;

        TSpinLock SpinLock_;
        bool Terminated_ = false;
        TError TerminationError_;
        typedef THashMap<TRequestId, TClientRequestControlPtr> TActiveRequestMap;
        TActiveRequestMap ActiveRequestMap_;

        void OnRequestSerialized(
            const TClientRequestControlPtr& requestControl,
            const TSendOptions& options,
            TErrorOr<TSharedRefArray> requestMessageOrError)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (requestMessageOrError.IsOK()) {
                auto requestMessageError = CheckBusMessageLimits(requestMessageOrError.Value());
                if (!requestMessageError.IsOK()){
                    requestMessageOrError = requestMessageError;
                }
            }

            const auto& requestId = requestControl->GetRequestId();

            IBusPtr bus;

            TClientRequestControlPtr existingRequestControl;
            IClientResponseHandlerPtr existingResponseHandler;
            {
                auto guard = Guard(SpinLock_);

                if (!requestControl->IsActive(guard)) {
                    return;
                }

                if (!requestMessageOrError.IsOK()) {
                    IClientResponseHandlerPtr responseHandler;
                    requestControl->Finalize(guard, &responseHandler);
                    guard.Release();

                    NotifyError(
                        requestControl,
                        responseHandler,
                        AsStringBuf("Request serialization failed"),
                        TError(NRpc::EErrorCode::TransportError, "Request serialization failed")
                            << requestMessageOrError);
                    return;
                }

                if (Terminated_) {
                    IClientResponseHandlerPtr responseHandler;
                    requestControl->Finalize(guard, &responseHandler);
                    guard.Release();

                    NotifyError(
                        requestControl,
                        responseHandler,
                        AsStringBuf("Request is dropped because channel is terminated"),
                        TError(NRpc::EErrorCode::TransportError, "Channel terminated")
                            << TerminationError_);
                    return;
                }

                // NB: We're OK with duplicate request ids.
                auto pair = ActiveRequestMap_.insert(std::make_pair(requestId, requestControl));
                if (!pair.second) {
                    existingRequestControl = std::move(pair.first->second);
                    existingRequestControl->Finalize(guard, &existingResponseHandler);
                    pair.first->second = requestControl;
                }

                bus = Bus_;
            }

            if (existingResponseHandler) {
                NotifyError(
                    existingRequestControl,
                    existingResponseHandler,
                    "Request resent",
                    TError(NRpc::EErrorCode::TransportError, "Request resent"));
            }

            const auto& requestMessage = requestMessageOrError.Value();

            NBus::TSendOptions busOptions;
            busOptions.TrackingLevel = options.RequestAck
                ? EDeliveryTrackingLevel::Full
                : EDeliveryTrackingLevel::ErrorOnly;
            busOptions.ChecksummedPartCount = options.GenerateAttachmentChecksums
                ? NBus::TSendOptions::AllParts
                : 2; // RPC header + request body
            bus->Send(requestMessage, busOptions).Subscribe(BIND(
                &TSession::OnAcknowledgement,
                MakeStrong(this),
                requestId));

            requestControl->ProfileRequest(requestMessage);

            LOG_DEBUG("Request sent (RequestId: %v, Method: %v:%v, Timeout: %v, TrackingLevel: %v, "
                "ChecksummedPartCount: %v, MultiplexingBand: %v, Endpoint: %v, AttachmentSize: %v)",
                requestId,
                requestControl->GetService(),
                requestControl->GetMethod(),
                requestControl->GetTimeout(),
                busOptions.TrackingLevel,
                busOptions.ChecksummedPartCount,
                options.MultiplexingBand,
                bus->GetEndpointDescription(),
                GetTotalMesageAttachmentSize(requestMessage));
        }

        void OnAcknowledgement(const TRequestId& requestId, const TError& error)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TClientRequestControlPtr requestControl;
            IClientResponseHandlerPtr responseHandler;
            {
                auto guard = Guard(SpinLock_);

                auto it = ActiveRequestMap_.find(requestId);
                if (it == ActiveRequestMap_.end()) {
                    // This one may easily get the actual response before the acknowledgment.
                    LOG_DEBUG(error, "Acknowledgment received for an unknown request, ignored (RequestId: %v)",
                        requestId);
                    return;
                }

                requestControl = it->second;
                requestControl->ProfileAck();
                if (!error.IsOK()) {
                    requestControl->Finalize(guard, &responseHandler);
                    ActiveRequestMap_.erase(it);
                } else {
                    requestControl->GetResponseHandler(guard, &responseHandler);
                }
            }

            if (error.IsOK()) {
                NotifyAcknowledgement(requestId, responseHandler);
            } else {
                NotifyError(
                    requestControl,
                    responseHandler,
                    AsStringBuf("Request acknowledgment failed"),
                    TError(NRpc::EErrorCode::TransportError, "Request acknowledgment failed")
                         << error);
            }
        }

        void NotifyError(
            const TClientRequestControlPtr& requestControl,
            const IClientResponseHandlerPtr& responseHandler,
            TStringBuf reason,
            const TError& error)
        {
            YCHECK(responseHandler);

            auto detailedError = error
                << TErrorAttribute("realm_id", requestControl->GetRealmId())
                << TErrorAttribute("service", requestControl->GetService())
                << TErrorAttribute("method", requestControl->GetMethod())
                << TErrorAttribute("request_id", requestControl->GetRequestId())
                << Bus_->GetEndpointAttributes();

            if (requestControl->GetTimeout()) {
                detailedError = detailedError
                    << TErrorAttribute("timeout", *requestControl->GetTimeout());
            }

            LOG_DEBUG(detailedError, "%v (RequestId: %v)",
                reason,
                requestControl->GetRequestId());

            responseHandler->HandleError(detailedError);
        }

        void NotifyAcknowledgement(
            const TRequestId& requestId,
            const IClientResponseHandlerPtr& responseHandler)
        {
            LOG_DEBUG("Request acknowledged (RequestId: %v)", requestId);

            responseHandler->HandleAcknowledgement();
        }

        void NotifyResponse(
            const TRequestId& requestId,
            const TClientRequestControlPtr& requestControl,
            const IClientResponseHandlerPtr& responseHandler,
            TSharedRefArray message)
        {
            LOG_DEBUG("Response received (RequestId: %v, Method: %v:%v, TotalTime: %v)",
                requestId,
                requestControl->GetService(),
                requestControl->GetMethod(),
                requestControl->GetTotalTime());

            responseHandler->HandleResponse(std::move(message));
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
            , RealmId_(request->GetRealmId())
            , Service_(request->GetService())
            , Method_(request->GetMethod())
            , RequestId_(request->GetRequestId())
            , Timeout_(timeout)
            , MethodMetadata_(Session_->GetMethodMetadata(Service_, Method_))
            , ResponseHandler_(std::move(responseHandler))
        { }

        ~TClientRequestControl()
        {
            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
        }

        const TRealmId& GetRealmId() const
        {
            return RealmId_;
        }

        const TString& GetService() const
        {
            return Service_;
        }

        const TString& GetMethod() const
        {
            return Method_;
        }

        const TRequestId& GetRequestId() const
        {
            return RequestId_;
        }

        TNullable<TDuration> GetTimeout() const
        {
            return Timeout_;
        }

        TDuration GetTotalTime() const
        {
            return TotalTime_;
        }

        bool IsActive(const TGuard<TSpinLock>&) const
        {
            return static_cast<bool>(ResponseHandler_);
        }

        void SetTimeoutCookie(const TGuard<TSpinLock>&, TDelayedExecutorEntryPtr newTimeoutCookie)
        {
            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
            TimeoutCookie_ = std::move(newTimeoutCookie);
        }

        void GetResponseHandler(const TGuard<TSpinLock>&, IClientResponseHandlerPtr* responseHandler)
        {
            *responseHandler = ResponseHandler_;
        }

        void Finalize(const TGuard<TSpinLock>&, IClientResponseHandlerPtr* responseHandler)
        {
            *responseHandler = std::move(ResponseHandler_);
            TotalTime_ = DoProfile(MethodMetadata_->TotalTimeCounter);
            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
        }

        void ProfileRequest(const TSharedRefArray& requestMessage)
        {
            Profiler.Increment(
                MethodMetadata_->RequestMessageBodySizeCounter,
                GetMessageBodySize(requestMessage));
            Profiler.Increment(
                MethodMetadata_->RequestMessageAttachmentSizeCounter,
                GetTotalMesageAttachmentSize(requestMessage));
        }

        void ProfileReply(const TSharedRefArray& responseMessage)
        {
            DoProfile(MethodMetadata_->ReplyTimeCounter);

            Profiler.Increment(
                MethodMetadata_->ResponseMessageBodySizeCounter,
                GetMessageBodySize(responseMessage));
            Profiler.Increment(
                MethodMetadata_->ResponseMessageAttachmentSizeCounter,
                GetTotalMesageAttachmentSize(responseMessage));
        }

        void ProfileAck()
        {
            DoProfile(MethodMetadata_->AckTimeCounter);
        }

        void ProfileCancel()
        {
            DoProfile(MethodMetadata_->CancelTimeCounter);
        }

        void ProfileTimeout()
        {
            DoProfile(MethodMetadata_->TimeoutTimeCounter);
        }

        // IClientRequestControl overrides
        virtual void Cancel() override
        {
            // YT-1639: Avoid calling TSession::Cancel directly as this may lead
            // to an extremely long chain of recursive calls.
            TDispatcher::Get()->GetLightInvoker()->Invoke(
                BIND(&TSession::Cancel, Session_, MakeStrong(this)));
        }

    private:
        const TSessionPtr Session_;
        const TRealmId RealmId_;
        const TString Service_;
        const TString Method_;
        const TRequestId RequestId_;
        const TNullable<TDuration> Timeout_;
        TSession::TMethodMetadata* const MethodMetadata_;

        TDelayedExecutorCookie TimeoutCookie_;
        IClientResponseHandlerPtr ResponseHandler_;

        NProfiling::TWallTimer Timer_;
        TDuration TotalTime_;


        TDuration DoProfile(NProfiling::TAggregateGauge& counter)
        {
            auto elapsed = Timer_.GetElapsedTime();
            Profiler.Update(counter, NProfiling::DurationToValue(elapsed));
            return elapsed;
        }
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
    explicit TBusChannelFactory(TTcpBusConfigPtr config)
        : Config_(ConvertToNode(std::move(config)))
    { }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        auto config = TTcpBusClientConfig::CreateTcp(address);
        config->Load(Config_, true, false);
        auto client = CreateTcpBusClient(std::move(config));
        return CreateBusChannel(std::move(client));
    }

private:
    const INodePtr Config_;
};

IChannelFactoryPtr CreateBusChannelFactory(TTcpBusConfigPtr config)
{
    return New<TBusChannelFactory>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NRpc
} // namespace NYT

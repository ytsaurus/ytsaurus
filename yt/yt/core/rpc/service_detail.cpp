#include "service_detail.h"
#include "private.h"
#include "config.h"
#include "dispatcher.h"
#include "helpers.h"
#include "message.h"
#include "response_keeper.h"
#include "server_detail.h"
#include "authenticator.h"
#include "authentication_identity.h"
#include "stream.h"

#include <atomic>

#include <yt/core/bus/bus.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/lease_manager.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/net/address.h>
#include <yt/core/net/local_address.h>

#include <yt/core/misc/string.h>
#include <yt/core/misc/finally.h>

#include <yt/core/ytalloc/memory_zone.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;
using namespace NTracing;
using namespace NConcurrency;
using namespace NYTAlloc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    TString method,
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Method(std::move(method))
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
{ }

auto TServiceBase::TMethodDescriptor::SetInvoker(IInvokerPtr value) -> TMethodDescriptor&
{
    Invoker = std::move(value);
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetInvokerProvider(TInvokerProvider value) -> TMethodDescriptor&
{
    InvokerProvider = std::move(value);
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetHeavy(bool value) -> TMethodDescriptor&
{
    Options.Heavy = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetResponseCodec(NCompression::ECodec value) -> TMethodDescriptor&
{
    Options.ResponseCodec = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetQueueSizeLimit(int value) -> TMethodDescriptor&
{
    QueueSizeLimit = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetConcurrencyLimit(int value) -> TMethodDescriptor&
{
    ConcurrencyLimit = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetSystem(bool value) -> TMethodDescriptor&
{
    System = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetLogLevel(NLogging::ELogLevel value) -> TMethodDescriptor&
{
    LogLevel = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetLoggingSuppressionTimeout(TDuration timeout) -> TMethodDescriptor&
{
    LoggingSuppressionTimeout = timeout;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetCancelable(bool value) -> TMethodDescriptor&
{
    Cancelable = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetGenerateAttachmentChecksums(bool value) -> TMethodDescriptor&
{
    GenerateAttachmentChecksums = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetStreamingEnabled(bool value) -> TMethodDescriptor&
{
    StreamingEnabled = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetPooled(bool value) -> TMethodDescriptor&
{
    Pooled = value;
    return *this;
}

auto TServiceBase::TMethodDescriptor::SetRequestBytesThrottler(TThroughputThrottlerConfigPtr config) -> TMethodDescriptor&
{
    RequestBytesThrottlerConfig = std::move(config);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodPerformanceCounters::TMethodPerformanceCounters(const NProfiling::TRegistry& registry)
    : RequestCounter(registry.Counter("/request_count"))
    , CanceledRequestCounter(registry.Counter("/canceled_request_count"))
    , FailedRequestCounter(registry.Counter("/failed_request_count"))
    , TimedOutRequestCounter(registry.Counter("/timed_out_request_count"))
    , ExecutionTimeCounter(registry.Timer("/request_time/execution"))
    , RemoteWaitTimeCounter(registry.Timer("/request_time/remote_wait"))
    , LocalWaitTimeCounter(registry.Timer("/request_time/local_wait"))
    , TotalTimeCounter(registry.Timer("/request_time/total"))
    , HandlerFiberTimeCounter(registry.TimeCounter("/request_time/handler_fiber"))
    , TraceContextTimeCounter(registry.TimeCounter("/request_time/trace_context"))
    , RequestMessageBodySizeCounter(registry.Counter("/request_message_body_bytes"))
    , RequestMessageAttachmentSizeCounter(registry.Counter("/request_message_attachment_bytes"))
    , ResponseMessageBodySizeCounter(registry.Counter("/response_message_body_bytes"))
    , ResponseMessageAttachmentSizeCounter(registry.Counter("/response_message_attachment_bytes"))
{ }

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    const TMethodDescriptor& descriptor,
    const NProfiling::TRegistry& registry)
    : Descriptor(descriptor)
    , Registry(registry.WithTag("method", descriptor.Method, -1))
    , RequestBytesThrottler(
        CreateReconfigurableThroughputThrottler(
            Descriptor.RequestBytesThrottlerConfig
            ? Descriptor.RequestBytesThrottlerConfig
            : New<TThroughputThrottlerConfig>()
        ))
    , RequestBytesThrottlerSpecified(static_cast<bool>(Descriptor.RequestBytesThrottlerConfig))
    , LoggingSuppressionFailedRequestThrottler(
        CreateReconfigurableThroughputThrottler(TMethodConfig::DefaultLoggingSuppressionFailedRequestThrottler))
{
    Registry.AddFuncGauge("/request_queue_size", MakeStrong(this), [this] {
        return QueueSize.load(std::memory_order_relaxed);
    });
    Registry.AddFuncGauge("/request_queue_size_limit", MakeStrong(this), [this] {
        return Descriptor.QueueSizeLimit;
    });
    Registry.AddFuncGauge("/concurrency", MakeStrong(this), [this] {
        return ConcurrencySemaphore.load(std::memory_order_relaxed);
    });
    Registry.AddFuncGauge("/concurrency_limit", MakeStrong(this), [this] {
        return Descriptor.ConcurrencyLimit;
    });
}

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        TServiceBasePtr&& service,
        TAcceptedRequest&& acceptedRequest,
        const NLogging::TLogger& logger)
        : TServiceContextBase(
            std::move(acceptedRequest.Header),
            std::move(acceptedRequest.Message),
            logger,
            acceptedRequest.RuntimeInfo->Descriptor.LogLevel)
        , Service_(std::move(service))
        , RequestId_(acceptedRequest.RequestId)
        , ReplyBus_(std::move(acceptedRequest.ReplyBus))
        , RuntimeInfo_(std::move(acceptedRequest.RuntimeInfo))
        , PerformanceCounters_(Service_->GetMethodPerformanceCounters(RuntimeInfo_, GetAuthenticationIdentity().UserTag))
        , TraceContext_(std::move(acceptedRequest.TraceContext))
        , ArriveInstant_(GetCpuInstant())

    {
        YT_ASSERT(RequestMessage_);
        YT_ASSERT(ReplyBus_);
        YT_ASSERT(Service_);
        YT_ASSERT(RuntimeInfo_);

        Initialize();
    }

    ~TServiceContext()
    {
        if (!Replied_ && !Canceled_.IsFired()) {
            Reply(TError(NRpc::EErrorCode::Unavailable, "Service is unable to complete your request"));
        }

        Finalize();
    }

    virtual bool IsPooled() const override
    {
        return RuntimeInfo_->Descriptor.Pooled;
    }

    virtual TTcpDispatcherStatistics GetBusStatistics() const override
    {
        return ReplyBus_->GetStatistics();
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return ReplyBus_->GetEndpointAttributes();
    }

    const TRuntimeMethodInfoPtr& GetRuntimeInfo() const
    {
        return RuntimeInfo_;
    }

    const IBusPtr& GetReplyBus() const
    {
        return ReplyBus_;
    }

    void CheckAndRun(const TErrorOr<TLiteHandler>& handlerOrError)
    {
        if (!handlerOrError.IsOK()) {
            Reply(TError(handlerOrError));
            return;
        }

        const auto& handler = handlerOrError.Value();
        if (!handler) {
            return;
        }

        Run(handler);
    }

    void Run(const TLiteHandler& handler)
    {
        auto this_ = MakeStrong(this);
        const auto& descriptor = RuntimeInfo_->Descriptor;
        IInvoker* invoker;
        IInvokerPtr invokerHolder;
        if (descriptor.InvokerProvider) {
            invokerHolder = descriptor.InvokerProvider(this_);
            invoker = invokerHolder.Get();
        } else if (descriptor.Invoker) {
            invoker = descriptor.Invoker.Get();
        } else {
            invoker = Service_->DefaultInvoker_.Get();
        }
        invoker->Invoke(BIND(&TServiceContext::DoRun, std::move(this_), handler));
    }

    virtual void SubscribeCanceled(const TClosure& callback) override
    {
        Canceled_.Subscribe(callback);
    }

    virtual void UnsubscribeCanceled(const TClosure& callback) override
    {
        Canceled_.Unsubscribe(callback);
    }

    virtual bool IsCanceled() override
    {
        return Canceled_.IsFired();
    }

    virtual void Cancel() override
    {
        if (!Canceled_.Fire()) {
            return;
        }

        YT_LOG_DEBUG("Request canceled (RequestId: %v)",
            RequestId_);

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto CanceledError = TError("Request canceled");
            AbortStreamsUnlessClosed(CanceledError);
        }

        PerformanceCounters_->CanceledRequestCounter.Increment();
    }

    virtual void SetComplete() override
    {
        DoSetComplete();
    }

    void HandleTimeout()
    {
        if (TimedOutLatch_.test_and_set()) {
            return;
        }

        YT_LOG_DEBUG("Request timed out, canceling (RequestId: %v)",
            RequestId_);

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto TimedOutError = TError("Request timed out");
            AbortStreamsUnlessClosed(TimedOutError);
        }

        Canceled_.Fire();
        PerformanceCounters_->TimedOutRequestCounter.Increment();

        // Guards from race with DoGuardedRun.
        // We can only mark as complete those requests that will not be run
        // as there's no guarantee that, if started,  the method handler will respond promptly to cancelation.
        if (!RunLatch_.test_and_set()) {
            SetComplete();
        }
    }

    virtual IAsyncZeroCopyInputStreamPtr GetRequestAttachmentsStream() override
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
        }
        CreateRequestAttachmentsStream();
        return RequestAttachmentsStream_;
    }

    virtual void SetResponseCodec(NCompression::ECodec codec) override
    {
        auto guard = Guard(StreamsLock_);
        if (ResponseAttachmentsStream_) {
            THROW_ERROR_EXCEPTION("Cannot update response codec after response attachments stream is accessed");
        }
        TServiceContextBase::SetResponseCodec(codec);
    }

    virtual IAsyncZeroCopyOutputStreamPtr GetResponseAttachmentsStream() override
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
        }
        CreateResponseAttachmentsStream();
        return ResponseAttachmentsStream_;
    }

    void HandleStreamingPayload(const TStreamingPayload& payload)
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            YT_LOG_DEBUG("Received streaming payload for a method that does not support streaming; ignored "
                "(Method: %v.%v, RequestId: %v)",
                Service_->ServiceId_.ServiceName,
                RuntimeInfo_->Descriptor.Method,
                RequestId_);
            return;
        }
        CreateRequestAttachmentsStream();
        try {
            RequestAttachmentsStream_->EnqueuePayload(payload);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error handling streaming payload (RequestId: %v)",
                RequestId_);
            RequestAttachmentsStream_->Abort(ex);
        }
    }

    void HandleStreamingFeedback(const TStreamingFeedback& feedback)
    {
        TAttachmentsOutputStreamPtr stream;
        {
            auto guard = Guard(StreamsLock_);
            stream = ResponseAttachmentsStream_;
        }

        if (!stream) {
            YT_LOG_DEBUG("Received streaming feedback for a method that does not support streaming; ignored "
                "(Method: %v.%v, RequestId: %v)",
                Service_->ServiceId_.ServiceName,
                RuntimeInfo_->Descriptor.Method,
                RequestId_);
            return;
        }

        try {
            stream->HandleFeedback(feedback);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error handling streaming feedback (RequestId: %v)",
                RequestId_);
            stream->Abort(ex);
        }
    }

private:
    const TServiceBasePtr Service_;
    const TRequestId RequestId_;
    const IBusPtr ReplyBus_;
    const TRuntimeMethodInfoPtr RuntimeInfo_;
    TMethodPerformanceCounters* const PerformanceCounters_;
    const TTraceContextPtr TraceContext_;

    EMemoryZone ResponseMemoryZone_;

    NCompression::ECodec RequestCodec_;

    TDelayedExecutorCookie TimeoutCookie_;

    bool Cancelable_ = false;
    TSingleShotCallbackList<void()> Canceled_;

    const NProfiling::TCpuInstant ArriveInstant_;
    NProfiling::TCpuInstant RunInstant_ = 0;
    NProfiling::TCpuInstant ReplyInstant_ = 0;

    TDuration ExecutionTime_;
    TDuration TotalTime_;
    TDuration LocalWaitTime_;

    std::atomic_flag CompletedLatch_ = ATOMIC_FLAG_INIT;
    std::atomic_flag TimedOutLatch_ = ATOMIC_FLAG_INIT;
    std::atomic_flag RunLatch_ = ATOMIC_FLAG_INIT;
    bool FinalizeLatch_ = false;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, StreamsLock_);
    TError StreamsError_;
    TAttachmentsInputStreamPtr RequestAttachmentsStream_;
    TAttachmentsOutputStreamPtr ResponseAttachmentsStream_;


    bool IsRegistrable()
    {
        if (RuntimeInfo_->Descriptor.Cancelable && !RequestHeader_->uncancelable()) {
            return true;
        }

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            return true;
        }

        return false;
    }

    void Initialize()
    {
        PerformanceCounters_->RequestCounter.Increment();
        PerformanceCounters_->RequestMessageBodySizeCounter.Increment(
            GetMessageBodySize(RequestMessage_));
        PerformanceCounters_->RequestMessageAttachmentSizeCounter.Increment(
            GetTotalMessageAttachmentSize(RequestMessage_));

        if (RequestHeader_->has_start_time()) {
            // Decode timing information.
            auto retryStart = FromProto<TInstant>(RequestHeader_->start_time());
            auto now = NProfiling::GetInstant();

            // Make sanity adjustments to account for possible clock skew.
            retryStart = std::min(retryStart, now);

            PerformanceCounters_->RemoteWaitTimeCounter.Record(now - retryStart);
        }

        {
            auto intMemoryZone = RequestHeader_->response_memory_zone();
            if (!TryEnumCast<EMemoryZone>(intMemoryZone, &ResponseMemoryZone_)) {
                Reply(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Response memory zone %v is not supported",
                    intMemoryZone));
                return;
            }
        }

        // COMPAT(kiselyovp): legacy RPC codecs
        if (RequestHeader_->has_request_codec()) {
            int intRequestCodecId = RequestHeader_->request_codec();
            if (!TryEnumCast(intRequestCodecId, &RequestCodec_)) {
                Reply(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Request codec %v is not supported",
                    intRequestCodecId));
                return;
            }
        } else {
            RequestCodec_ = NCompression::ECodec::None;
        }

        if (RequestHeader_->has_response_codec()) {
            int intResponseCodecId = RequestHeader_->response_codec();
            if (!TryEnumCast(intResponseCodecId, &ResponseCodec_)) {
                Reply(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Response codec %v is not supported",
                    intResponseCodecId));
                return;
            }
        } else {
            ResponseCodec_ = NCompression::ECodec::None;
        }

        BuildGlobalRequestInfo();

        if (IsRegistrable()) {
            Service_->RegisterRequest(this);
        }

        if (RuntimeInfo_->Descriptor.Cancelable && !RequestHeader_->uncancelable()) {
            Cancelable_ = true;
            auto timeout = GetTimeout();
            if (timeout) {
                TimeoutCookie_ = TDelayedExecutor::Submit(
                    BIND(&TServiceBase::OnRequestTimeout, Service_, RequestId_),
                    *timeout);
            }
        }

        ++RuntimeInfo_->QueueSize;
        ++Service_->ActiveRequestCount_;
    }

    void BuildGlobalRequestInfo()
    {
        TStringBuilder builder;
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        if (RequestHeader_->has_request_id()) {
            delimitedBuilder->AppendFormat("RequestId: %v", FromProto<TRequestId>(RequestHeader_->request_id()));
        }

        if (RequestHeader_->has_realm_id()) {
            delimitedBuilder->AppendFormat("RealmId: %v", FromProto<TRealmId>(RequestHeader_->realm_id()));
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        if (RequestHeader_->has_user_tag() && RequestHeader_->user_tag() != RequestHeader_->user()) {
            delimitedBuilder->AppendFormat("UserTag: %v", RequestHeader_->user_tag());
        }

        if (RequestHeader_->has_mutation_id()) {
            delimitedBuilder->AppendFormat("MutationId: %v", FromProto<TMutationId>(RequestHeader_->mutation_id()));
        }

        if (RequestHeader_->has_start_time()) {
            delimitedBuilder->AppendFormat("StartTime: %v", FromProto<TInstant>(RequestHeader_->start_time()));
        }

        delimitedBuilder->AppendFormat("Retry: %v", RequestHeader_->retry());

        if (RequestHeader_->has_user_agent()) {
            delimitedBuilder->AppendFormat("UserAgent: %v", RequestHeader_->user_agent());
        }

        if (RequestHeader_->has_timeout()) {
            delimitedBuilder->AppendFormat("Timeout: %v", FromProto<TDuration>(RequestHeader_->timeout()));
        }

        if (RequestHeader_->tos_level() != NBus::DefaultTosLevel) {
            delimitedBuilder->AppendFormat("TosLevel: %x", RequestHeader_->tos_level());
        }

        delimitedBuilder->AppendFormat("Endpoint: %v", ReplyBus_->GetEndpointDescription());

        delimitedBuilder->AppendFormat("BodySize: %v, AttachmentsSize: %v/%v",
            GetMessageBodySize(RequestMessage_),
            GetTotalMessageAttachmentSize(RequestMessage_),
            GetMessageAttachmentCount(RequestMessage_));

        // COMPAT(kiselyovp)
        if (RequestHeader_->has_request_codec() && RequestHeader_->has_response_codec()) {
            delimitedBuilder->AppendFormat("RequestCodec: %v, ResponseCodec: %v",
                RequestCodec_,
                ResponseCodec_);
        }

        RequestInfos_.push_back(builder.Flush());
    }

    void Finalize()
    {
        // Finalize is called from DoReply and ~TServiceContext.
        // Clearly there could be no race between these two and thus no atomics are needed.
        if (FinalizeLatch_) {
            return;
        }
        FinalizeLatch_ = true;

        if (TraceContext_) {
            TraceContext_->Finish();
        }

        if (IsRegistrable()) {
            Service_->UnregisterRequest(this);
        }

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto FinalizedError = TError("Request finalized");
            AbortStreamsUnlessClosed(Error_.IsOK() ? Error_ : FinalizedError);
        }

        DoSetComplete();
    }

    void AbortStreamsUnlessClosed(const TError& error)
    {
        auto guard = Guard(StreamsLock_);

        if (!StreamsError_.IsOK()) {
            return;
        }

        StreamsError_ = error;

        auto requestAttachmentsStream = RequestAttachmentsStream_;
        auto responseAttachmentsStream = ResponseAttachmentsStream_;

        guard.Release();

        if (requestAttachmentsStream) {
            requestAttachmentsStream->AbortUnlessClosed(Error_);
        }

        if (responseAttachmentsStream) {
            responseAttachmentsStream->AbortUnlessClosed(Error_);
        }
    }


    void DoRun(const TLiteHandler& handler)
    {
        DoBeforeRun();

        TFiberWallTimer timer;

        auto finally = Finally([&] {
            DoAfterRun(timer.GetElapsedTime());
        });

        try {
            TTraceContextGuard guard(TraceContext_);
            DoGuardedRun(handler);
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void DoBeforeRun()
    {
        RunInstant_ = GetCpuInstant();
        LocalWaitTime_ = CpuDurationToDuration(RunInstant_ - ArriveInstant_);
        PerformanceCounters_->LocalWaitTimeCounter.Record(LocalWaitTime_);
    }

    void DoGuardedRun(const TLiteHandler& handler)
    {
        const auto& descriptor = RuntimeInfo_->Descriptor;

        if (!descriptor.System) {
            Service_->BeforeInvoke(this);
        }

        auto timeout = GetTimeout();
        if (timeout && NProfiling::GetCpuInstant() > ArriveInstant_ + NProfiling::DurationToCpuDuration(*timeout)) {
            if (!TimedOutLatch_.test_and_set()) {
                Reply(TError(NYT::EErrorCode::Timeout, "Request dropped due to timeout before being run"));
                PerformanceCounters_->TimedOutRequestCounter.Increment();
            }
            return;
        }

        if (Cancelable_) {
            // TODO(lukyan): Wrap in CancelableExecution.

            auto fiberCanceler = GetCurrentFiberCanceler();
            if (fiberCanceler) {
                auto cancelationHandler = BIND([fiberCanceler = std::move(fiberCanceler)] {
                    fiberCanceler(TError("RPC request canceled"));
                });
                if (!Canceled_.TrySubscribe(std::move(cancelationHandler))) {
                    YT_LOG_DEBUG("Request was canceled before being run (RequestId: %v)",
                        RequestId_);
                    return;
                }
            }
        }

        // Guards from race with HandleTimeout.
        if (RunLatch_.test_and_set()) {
            return;
        }

        {
            TCurrentAuthenticationIdentityGuard identityGuard(&GetAuthenticationIdentity());
            handler.Run(this, descriptor.Options);
        }
    }

    void DoAfterRun(TDuration handlerElapsedValue)
    {
        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        PerformanceCounters_->HandlerFiberTimeCounter.Add(handlerElapsedValue);

        if (TraceContext_) {
            FlushCurrentTraceContextTime();
            auto traceContextTime = TraceContext_->GetElapsedTime();
            PerformanceCounters_->TraceContextTimeCounter.Add(traceContextTime);
        }
    }

    virtual void DoReply() override
    {
        auto responseMessage = GetResponseMessage();

        NBus::TSendOptions busOptions;
        busOptions.TrackingLevel = EDeliveryTrackingLevel::None;
        busOptions.ChecksummedPartCount = RuntimeInfo_->Descriptor.GenerateAttachmentChecksums
            ? NBus::TSendOptions::AllParts
            : 2; // RPC header + response body
        busOptions.MemoryZone = ResponseMemoryZone_;
        ReplyBus_->Send(responseMessage, busOptions);

        ReplyInstant_ = GetCpuInstant();
        ExecutionTime_ = RunInstant_ != 0
            ? CpuDurationToDuration(ReplyInstant_ - RunInstant_)
            : TDuration();
        TotalTime_ = CpuDurationToDuration(ReplyInstant_ - ArriveInstant_);

        PerformanceCounters_->ExecutionTimeCounter.Record(ExecutionTime_);
        PerformanceCounters_->TotalTimeCounter.Record(TotalTime_);
        if (!Error_.IsOK()) {
            PerformanceCounters_->FailedRequestCounter.Increment();
        }

        HandleLoggingSuppression();

        PerformanceCounters_->ResponseMessageBodySizeCounter.Increment(
            GetMessageBodySize(responseMessage));
        PerformanceCounters_->ResponseMessageAttachmentSizeCounter.Increment(
            GetTotalMessageAttachmentSize(responseMessage));

        Finalize();
    }

    void HandleLoggingSuppression()
    {
        auto timeout = RuntimeInfo_->Descriptor.LoggingSuppressionTimeout;
        if (RequestHeader_->has_logging_suppression_timeout()) {
            timeout = FromProto<TDuration>(RequestHeader_->logging_suppression_timeout());
        }

        if (TotalTime_ >= timeout) {
            return;
        }

        if (!Error_.IsOK() &&
            (RequestHeader_->disable_logging_suppression_if_request_failed() ||
            RuntimeInfo_->LoggingSuppressionFailedRequestThrottler->TryAcquire(1)))
        {
            return;
        }

        NLogging::TLogManager::Get()->SuppressRequest(GetRequestId());
    }

    void DoSetComplete()
    {
        // DoSetComplete could be called from anywhere so it is racy.
        if (CompletedLatch_.test_and_set()) {
            return;
        }

        --RuntimeInfo_->QueueSize;
        if (--Service_->ActiveRequestCount_ == 0 && Service_->Stopped_.load()) {
            Service_->StopResult_.TrySet();
        }

        TServiceBase::ReleaseRequestSemaphore(RuntimeInfo_);
        TServiceBase::ScheduleRequests(RuntimeInfo_);
    }


    virtual void LogRequest() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v.%v <- ",
            GetService(),
            GetMethod());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        for (const auto& info : RequestInfos_) {
            delimitedBuilder->AppendString(info);
        }

        if (RuntimeInfo_->Descriptor.Cancelable && !Cancelable_) {
            delimitedBuilder->AppendFormat("Cancelable: %v", Cancelable_);
        }

        auto logMessage = builder.Flush();
        AddTag(RequestInfoAnnotation, logMessage);
        YT_LOG_EVENT(Logger, LogLevel_, logMessage);
    }

    virtual void LogResponse() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v.%v -> ",
            GetService(),
            GetMethod());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        if (RequestId_) {
            delimitedBuilder->AppendFormat("RequestId: %v", RequestId_);
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        if (RequestHeader_->has_user_tag() && RequestHeader_->user_tag() != RequestHeader_->user()) {
            delimitedBuilder->AppendFormat("UserTag: %v", RequestHeader_->user_tag());
        }

        auto responseMessage = GetResponseMessage();
        delimitedBuilder->AppendFormat("Error: %v, BodySize: %v, AttachmentsSize: %v/%v",
            Error_,
            GetMessageBodySize(responseMessage),
            GetTotalMessageAttachmentSize(responseMessage),
            GetMessageAttachmentCount(responseMessage));

        for (const auto& info : ResponseInfos_) {
            delimitedBuilder->AppendString(info);
        }

        delimitedBuilder->AppendFormat("ExecutionTime: %v, TotalTime: %v",
            ExecutionTime_,
            TotalTime_);

        auto logMessage = builder.Flush();
        AddTag(ResponseInfoAnnotation, logMessage);
        YT_LOG_EVENT(Logger, LogLevel_, logMessage);
    }


    void CreateRequestAttachmentsStream()
    {
        auto guard = Guard(StreamsLock_);

        if (!RequestAttachmentsStream_) {
            auto parameters = FromProto<TStreamingParameters>(RequestHeader_->server_attachments_streaming_parameters());
            RequestAttachmentsStream_ =  New<TAttachmentsInputStream>(
                BIND(&TServiceContext::OnRequestAttachmentsStreamRead, MakeWeak(this)),
                TDispatcher::Get()->GetCompressionPoolInvoker(),
                parameters.ReadTimeout);
        }

        auto error = StreamsError_;

        guard.Release();

        if (!error.IsOK()) {
            RequestAttachmentsStream_->AbortUnlessClosed(error);
        }
    }

    void CreateResponseAttachmentsStream()
    {
        auto guard = Guard(StreamsLock_);

        if (!ResponseAttachmentsStream_) {
            auto parameters = FromProto<TStreamingParameters>(RequestHeader_->server_attachments_streaming_parameters());
            ResponseAttachmentsStream_ = New<TAttachmentsOutputStream>(
                ResponseMemoryZone_,
                ResponseCodec_,
                TDispatcher::Get()->GetCompressionPoolInvoker(),
                BIND(&TServiceContext::OnPullResponseAttachmentsStream, MakeWeak(this)),
                parameters.WindowSize,
                parameters.WriteTimeout);
        }

        auto error = StreamsError_;

        guard.Release();

        if (!error.IsOK()) {
            ResponseAttachmentsStream_->AbortUnlessClosed(error);
        }
    }

    void OnPullResponseAttachmentsStream()
    {
        YT_VERIFY(ResponseAttachmentsStream_);
        auto payload = ResponseAttachmentsStream_->TryPull();
        if (!payload) {
            return;
        }

        YT_LOG_DEBUG("Response streaming attachments pulled (RequestId: %v, SequenceNumber: %v, Sizes: %v, Closed: %v)",
            RequestId_,
            payload->SequenceNumber,
            MakeFormattableView(payload->Attachments, [] (auto* builder, const auto& attachment) {
                builder->AppendFormat("%v", GetStreamingAttachmentSize(attachment));
            }),
            !payload->Attachments.back());

        NProto::TStreamingPayloadHeader header;
        ToProto(header.mutable_request_id(), RequestId_);
        header.set_service(GetService());
        header.set_method(GetMethod());
        if (GetRealmId()) {
            ToProto(header.mutable_realm_id(), GetRealmId());
        }
        header.set_sequence_number(payload->SequenceNumber);
        header.set_codec(static_cast<int>(payload->Codec));
        header.set_memory_zone(static_cast<int>(payload->MemoryZone));

        auto message = CreateStreamingPayloadMessage(header, payload->Attachments);

        NBus::TSendOptions options;
        options.TrackingLevel = EDeliveryTrackingLevel::Full;
        options.MemoryZone = payload->MemoryZone;
        ReplyBus_->Send(std::move(message), options).Subscribe(
            BIND(&TServiceContext::OnResponseStreamingPayloadAcked, MakeStrong(this), payload->SequenceNumber));
    }

    void OnResponseStreamingPayloadAcked(int sequenceNumber, const TError& error)
    {
        YT_VERIFY(ResponseAttachmentsStream_);
        if (error.IsOK()) {
            YT_LOG_DEBUG("Response streaming payload delivery acknowledged (RequestId: %v, SequenceNumber: %v)",
                RequestId_,
                sequenceNumber);
        } else {
            YT_LOG_DEBUG(error, "Response streaming payload delivery failed (RequestId: %v, SequenceNumber: %v)",
                RequestId_,
                sequenceNumber);
            ResponseAttachmentsStream_->Abort(error);
        }
    }

    void OnRequestAttachmentsStreamRead()
    {
        YT_VERIFY(RequestAttachmentsStream_);
        auto feedback = RequestAttachmentsStream_->GetFeedback();

        YT_LOG_DEBUG("Request streaming attachments read (RequestId: %v, ReadPosition: %v)",
            RequestId_,
            feedback.ReadPosition);

        NProto::TStreamingFeedbackHeader header;
        ToProto(header.mutable_request_id(), RequestId_);
        header.set_service(GetService());
        header.set_method(GetMethod());
        if (GetRealmId()) {
            ToProto(header.mutable_realm_id(), GetRealmId());
        }
        header.set_read_position(feedback.ReadPosition);

        auto message = CreateStreamingFeedbackMessage(header);

        NBus::TSendOptions options;
        options.TrackingLevel = EDeliveryTrackingLevel::Full;
        ReplyBus_->Send(std::move(message), options).Subscribe(
            BIND(&TServiceContext::OnRequestStreamingFeedbackAcked, MakeStrong(this)));
    }

    void OnRequestStreamingFeedbackAcked(const TError& error)
    {
        YT_VERIFY(RequestAttachmentsStream_);
        if (error.IsOK()) {
            YT_LOG_DEBUG("Request streaming feedback delivery acknowledged (RequestId: %v)",
                RequestId_);
        } else {
            YT_LOG_DEBUG(error, "Request streaming feedback delivery failed (RequestId: %v)",
                RequestId_);
            RequestAttachmentsStream_->Abort(error);
        }
    }


    const IInvokerPtr& GetInvoker()
    {
        const auto& descriptor = RuntimeInfo_->Descriptor;
        return descriptor.Invoker ? descriptor.Invoker : Service_->DefaultInvoker_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    TRealmId realmId,
    IAuthenticatorPtr authenticator)
    : Logger(logger)
    , DefaultInvoker_(std::move(defaultInvoker))
    , Authenticator_(std::move(authenticator))
    , ServiceDescriptor_(descriptor)
    , ServiceId_(descriptor.GetFullServiceName(), realmId)
    , ProfilingRegistry_(RpcServerProfiler.WithHot().WithTag("yt_service", ServiceId_.ServiceName))
    , AuthenticationTimer_(ProfilingRegistry_.Timer("/authentication_time"))
{
    YT_VERIFY(DefaultInvoker_);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(GetSyncInvoker())
        .SetSystem(true));

    ProfilingRegistry_.AddFuncGauge("/authentication_queue_size", MakeStrong(this), [this] {
        return AuthenticationQueueSize_.load(std::memory_order_relaxed);
    });
}

const TServiceId& TServiceBase::GetServiceId() const
{
    return ServiceId_;
}

void TServiceBase::HandleRequest(
    std::unique_ptr<NProto::TRequestHeader> header,
    TSharedRefArray message,
    IBusPtr replyBus)
{
    SetActive();

    const auto& method = header->method();
    auto requestId = FromProto<TRequestId>(header->request_id());

    auto replyError = [&] (TError error) {
        ReplyError(std::move(error), *header, replyBus);
    };

    if (Stopped_) {
        replyError(TError(
            EErrorCode::Unavailable,
            "Service is stopped"));
        return;
    }

    if (auto error = DoCheckRequestCompatibility(*header); !error.IsOK()) {
        replyError(std::move(error));
        return;
    }

    auto runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        replyError(TError(
            EErrorCode::NoSuchMethod,
            "Unknown method"));
        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    auto queueSizeLimit = runtimeInfo->Descriptor.QueueSizeLimit;
    if (runtimeInfo->QueueSize.load() > queueSizeLimit) {
        replyError(TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Request queue size limit exceeded")
            << TErrorAttribute("limit", queueSizeLimit));
        return;
    }

    auto forceTracing = runtimeInfo->ForceTracing.load(std::memory_order_relaxed);
    auto traceContext = GetOrCreateHandlerTraceContext(*header, forceTracing);
    if (traceContext && traceContext->IsSampled()) {
        traceContext->AddTag(EndpointAnnotation, replyBus->GetEndpointDescription());
    }

    TTraceContextGuard traceContextGuard(traceContext);

    // NOTE: Do not use replyError() after this line.
    TAcceptedRequest acceptedRequest{
        requestId,
        std::move(replyBus),
        std::move(runtimeInfo),
        traceContext,
        std::move(header),
        std::move(message)
    };

    if (!IsAuthenticationNeeded(acceptedRequest)) {
        HandleAuthenticatedRequest(std::move(acceptedRequest));
        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    auto authenticationQueueSizeLimit = AuthenticationQueueSizeLimit_.load(std::memory_order_relaxed);
    auto authenticationQueueSize = AuthenticationQueueSize_.load(std::memory_order_relaxed);
    if (authenticationQueueSize > authenticationQueueSizeLimit) {
        auto error = TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Authentication request queue size limit exceeded")
            << TErrorAttribute("limit", authenticationQueueSizeLimit);
        ReplyError(error, *acceptedRequest.Header, acceptedRequest.ReplyBus);
        return;
    }
    ++AuthenticationQueueSize_;

    NProfiling::TWallTimer timer;

    TAuthenticationContext context{
        .Header = acceptedRequest.Header.get(),
        .UserIP = acceptedRequest.ReplyBus->GetEndpointAddress()
    };
    auto asyncAuthResult = Authenticator_->Authenticate(context);
    if (asyncAuthResult.IsSet()) {
        OnRequestAuthenticated(timer, std::move(acceptedRequest), asyncAuthResult.Get());
    } else {
        asyncAuthResult.Subscribe(
            BIND(&TServiceBase::OnRequestAuthenticated, MakeStrong(this), timer, Passed(std::move(acceptedRequest))));
    }
}

void TServiceBase::ReplyError(
    TError error,
    const NProto::TRequestHeader& header,
    const IBusPtr& replyBus)
{
    NTracing::AddErrorTag();

    auto requestId = FromProto<TRequestId>(header.request_id());
    auto richError = std::move(error)
        << TErrorAttribute("request_id", requestId)
        << TErrorAttribute("realm_id", ServiceId_.RealmId)
        << TErrorAttribute("service", ServiceId_.ServiceName)
        << TErrorAttribute("method", header.method())
        << TErrorAttribute("endpoint", replyBus->GetEndpointDescription());

    auto code = richError.GetCode();
    auto logLevel =
        code == NRpc::EErrorCode::NoSuchMethod || code == NRpc::EErrorCode::ProtocolError
        ? NLogging::ELogLevel::Warning
        : NLogging::ELogLevel::Debug;
    YT_LOG_EVENT(Logger, logLevel, richError);

    auto errorMessage = CreateErrorResponseMessage(requestId, richError);
    replyBus->Send(errorMessage, NBus::TSendOptions(EDeliveryTrackingLevel::None));
}

void TServiceBase::OnRequestAuthenticated(
    const NProfiling::TWallTimer& timer,
    TAcceptedRequest acceptedRequest,
    const TErrorOr<TAuthenticationResult>& authResultOrError)
{
    AuthenticationTimer_.Record(timer.GetElapsedTime());
    --AuthenticationQueueSize_;

    if (authResultOrError.IsOK()) {
        const auto& authResult = authResultOrError.Value();
        const auto& Logger = RpcServerLogger;
        YT_LOG_DEBUG("Request authenticated (RequestId: %v, User: %v, Realm: %v)",
            acceptedRequest.RequestId,
            authResult.User,
            authResult.Realm);
        const auto& authenticatedUser = authResult.User;
        if (acceptedRequest.Header->has_user()) {
            const auto& user = acceptedRequest.Header->user();
            if (user != authenticatedUser) {
                ReplyError(
                    TError("Manually specified and authenticated users mismatch")
                        << TErrorAttribute("user", user)
                        << TErrorAttribute("authenticated_user", authenticatedUser),
                    *acceptedRequest.Header,
                    acceptedRequest.ReplyBus);
                return;
            }
        }
        acceptedRequest.Header->set_user(std::move(authResult.User));
        HandleAuthenticatedRequest(std::move(acceptedRequest));
    } else {
        ReplyError(
            TError(
                NYT::NRpc::EErrorCode::AuthenticationError,
                "Request authentication failed")
            << authResultOrError,
            *acceptedRequest.Header,
            acceptedRequest.ReplyBus);
    }
}

bool TServiceBase::IsAuthenticationNeeded(const TAcceptedRequest& acceptedRequest)
{
    return
        Authenticator_.operator bool() &&
        !acceptedRequest.RuntimeInfo->Descriptor.System;
}

void TServiceBase::HandleAuthenticatedRequest(
    TAcceptedRequest acceptedRequest)
{
    auto runtimeInfo = acceptedRequest.RuntimeInfo;

    auto context = New<TServiceContext>(
        this,
        std::move(acceptedRequest),
        Logger);

    runtimeInfo->RequestQueue.Enqueue(std::move(context));
    ScheduleRequests(runtimeInfo);
}

void TServiceBase::HandleRequestCancelation(TRequestId requestId)
{
    SetActive();

    auto context = FindRequest(requestId);
    if (!context) {
        YT_LOG_DEBUG("Received cancelation for an unknown request, ignored (RequestId: %v)",
            requestId);
        return;
    }

    context->Cancel();
}

void TServiceBase::HandleStreamingPayload(
    TRequestId requestId,
    const TStreamingPayload& payload)
{
    SetActive();

    auto* bucket = GetRequestBucket(requestId);
    auto guard = Guard(bucket->Lock);
    auto context = DoFindRequest(bucket, requestId);
    if (context) {
        guard.Release();
        context->HandleStreamingPayload(payload);
    } else {
        auto* entry = DoGetOrCreatePendingPayloadsEntry(bucket, requestId);
        entry->Payloads.emplace_back(payload);
        guard.Release();
        YT_LOG_DEBUG("Received streaming payload for an unknown request, saving (RequestId: %v)",
            requestId);
    }
}

void TServiceBase::HandleStreamingFeedback(
    TRequestId requestId,
    const TStreamingFeedback& feedback)
{
    auto context = FindRequest(requestId);
    if (!context) {
        YT_LOG_DEBUG("Received streaming feedback for an unknown request, ignored (RequestId: %v)",
            requestId);
        return;
    }

    context->HandleStreamingFeedback(feedback);
}

void TServiceBase::DoDeclareServerFeature(int featureId)
{
    ValidateInactive();

    // Failure here means that such feature is already registered.
    YT_VERIFY(SupportedServerFeatureIds_.insert(featureId).second);
}

TError TServiceBase::DoCheckRequestCompatibility(const NRpc::NProto::TRequestHeader& header)
{
    if (auto error = DoCheckRequestProtocol(header); !error.IsOK()) {
        return error;
    }
    if (auto error = DoCheckRequestFeatures(header); !error.IsOK()) {
        return error;
    }
    return {};
}

TError TServiceBase::DoCheckRequestProtocol(const NRpc::NProto::TRequestHeader& header)
{
    TProtocolVersion requestProtocolVersion{
        header.protocol_version_major(),
        header.protocol_version_minor()
    };

    if (requestProtocolVersion.Major != GenericProtocolVersion.Major) {
        if (ServiceDescriptor_.ProtocolVersion.Major != requestProtocolVersion.Major) {
            return TError(
                EErrorCode::ProtocolError,
                "Server major protocol version differs from client major protocol version: %v != %v",
                requestProtocolVersion.Major,
                ServiceDescriptor_.ProtocolVersion.Major);
        }

        if (ServiceDescriptor_.ProtocolVersion.Minor < requestProtocolVersion.Minor) {
            return TError(
                EErrorCode::ProtocolError,
                "Server minor protocol version is less than minor protocol version required by client: %v < %v",
                ServiceDescriptor_.ProtocolVersion.Minor,
                requestProtocolVersion.Minor);
        }
    }
    return {};
}

TError TServiceBase::DoCheckRequestFeatures(const NRpc::NProto::TRequestHeader& header)
{
    for (auto featureId : header.required_server_feature_ids()) {
        if (!SupportedServerFeatureIds_.contains(featureId)) {
            return TError(
                EErrorCode::UnsupportedServerFeature,
                "Server does not support the feature requested by client")
                << TErrorAttribute("feature_id", featureId);
        }
    }
    return {};
}

void TServiceBase::OnRequestTimeout(TRequestId requestId, bool /*aborted*/)
{
    auto context = FindRequest(requestId);
    if (!context) {
        return;
    }

    context->HandleTimeout();
}

void TServiceBase::OnReplyBusTerminated(const IBusPtr& bus, const TError& error)
{
    std::vector<TServiceContextPtr> contexts;
    {
        auto* bucket = GetReplyBusBucket(bus);
        auto guard = Guard(bucket->Lock);
        auto it = bucket->ReplyBusToContexts.find(bus);
        if (it == bucket->ReplyBusToContexts.end()) {
            return;
        }

        for (auto* rawContext : it->second) {
            auto context = DangerousGetPtr(rawContext);
            if (context) {
                contexts.push_back(context);
            }
        }

        bucket->ReplyBusToContexts.erase(it);
    }

    for (auto context : contexts) {
        YT_LOG_DEBUG(error, "Reply bus terminated, canceling request (RequestId: %v)",
            context->GetRequestId());
        context->Cancel();
    }
}

bool TServiceBase::TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    auto& semaphore = runtimeInfo->ConcurrencySemaphore;
    auto limit = runtimeInfo->Descriptor.ConcurrencyLimit;
    auto current = semaphore.load(std::memory_order_relaxed);
    while (true) {
        if (current >= limit) {
            return false;
        }
        if (semaphore.compare_exchange_weak(current, current + 1)) {
            return true;
        }
    }
}

void TServiceBase::ReleaseRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    --runtimeInfo->ConcurrencySemaphore;
}

void TServiceBase::ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    // Prevent reentrant invocations.
    static thread_local bool ScheduleRequestsLatch;
    if (ScheduleRequestsLatch) {
        return;
    }
    ScheduleRequestsLatch = true;
    auto latchGuard = Finally([&] {
        ScheduleRequestsLatch = false;
    });

#ifndef NDEBUG
    // Method handlers are allowed to run via sync invoker;
    // however these handlers must not yield.
    TForbidContextSwitchGuard contextSwitchGuard;
#endif

    auto* requestBytesThrottler = runtimeInfo->RequestBytesThrottlerSpecified
        ? runtimeInfo->RequestBytesThrottler.Get()
        : nullptr;

    while (true) {
        if (requestBytesThrottler && requestBytesThrottler->IsOverdraft()) {
            break;
        }

        if (!TryAcquireRequestSemaphore(runtimeInfo)) {
            break;
        }

        TServiceContextPtr context;
        if (!runtimeInfo->RequestQueue.Dequeue(&context)) {
            ReleaseRequestSemaphore(runtimeInfo);
            if (!runtimeInfo->RequestQueue.IsEmpty()) {
                continue;
            }
            break;
        }

        if (requestBytesThrottler) {
            i64 requestSize = GetMessageBodySize(context->GetRequestMessage()) +
                GetTotalMessageAttachmentSize(context->GetRequestMessage());
            requestBytesThrottler->Acquire(requestSize);
        }

        RunRequest(std::move(context));
    }
}

void TServiceBase::RunRequest(const TServiceContextPtr& context)
{
    const auto& runtimeInfo = context->GetRuntimeInfo();
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.Heavy) {
        BIND(runtimeInfo->Descriptor.HeavyHandler)
            .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
            .Run(context, options)
            .Subscribe(BIND(&TServiceContext::CheckAndRun, context));
    } else {
        context->Run(runtimeInfo->Descriptor.LiteHandler);
    }
}

TServiceBase::TRequestBucket* TServiceBase::GetRequestBucket(TRequestId requestId)
{
    return &RequestBuckets_[THash<TRequestId>()(requestId) % RequestBucketCount];
}

TServiceBase::TReplyBusBucket* TServiceBase::GetReplyBusBucket(const IBusPtr& bus)
{
    return &ReplyBusBuckets_[THash<IBusPtr>()(bus) % ReplyBusBucketCount];
}

void TServiceBase::RegisterRequest(TServiceContext* context)
{
    auto requestId = context->GetRequestId();
    {
        auto* bucket = GetRequestBucket(requestId);
        auto guard = Guard(bucket->Lock);
        // NB: We're OK with duplicate request ids.
        bucket->RequestIdToContext.emplace(requestId, context);
    }

    const auto& replyBus = context->GetReplyBus();
    bool subscribe = false;
    {
        auto* bucket = GetReplyBusBucket(replyBus);
        auto guard = Guard(bucket->Lock);
        auto it = bucket->ReplyBusToContexts.find(context->GetReplyBus());
        if (it == bucket->ReplyBusToContexts.end()) {
            subscribe = true;
            it = bucket->ReplyBusToContexts.emplace(replyBus, THashSet<TServiceContext*>()).first;
        }
        auto& contexts = it->second;
        contexts.insert(context);
    }

    if (subscribe) {
        replyBus->SubscribeTerminated(BIND(&TServiceBase::OnReplyBusTerminated, MakeWeak(this), replyBus));
    }

    auto pendingPayloads = GetAndErasePendingPayloads(requestId);
    if (!pendingPayloads.empty()) {
        YT_LOG_DEBUG("Pulling pending streaming payloads for a late request (RequestId: %v, PayloadCount: %v)",
            requestId,
            pendingPayloads.size());
        for (const auto& payload : pendingPayloads) {
            context->HandleStreamingPayload(payload);
        }
    }
}

void TServiceBase::UnregisterRequest(TServiceContext* context)
{
    auto requestId = context->GetRequestId();
    {
        auto* bucket = GetRequestBucket(requestId);
        auto guard = Guard(bucket->Lock);
        // NB: We're OK with duplicate request ids.
        bucket->RequestIdToContext.erase(requestId);
    }

    const auto& replyBus = context->GetReplyBus();
    {
        auto* bucket = GetReplyBusBucket(replyBus);
        auto guard = Guard(bucket->Lock);
        auto it = bucket->ReplyBusToContexts.find(replyBus);
        // Missing replyBus in ReplyBusToContexts is OK; see OnReplyBusTerminated.
        if (it != bucket->ReplyBusToContexts.end()) {
            auto& contexts = it->second;
            contexts.erase(context);
        }
    }
}

TServiceBase::TServiceContextPtr TServiceBase::FindRequest(TRequestId requestId)
{
    auto* bucket = GetRequestBucket(requestId);
    auto guard = Guard(bucket->Lock);
    return DoFindRequest(bucket, requestId);
}

TServiceBase::TServiceContextPtr TServiceBase::DoFindRequest(TRequestBucket* bucket, TRequestId requestId)
{
    auto it = bucket->RequestIdToContext.find(requestId);
    return it == bucket->RequestIdToContext.end() ? nullptr : DangerousGetPtr(it->second);
}

TServiceBase::TPendingPayloadsEntry* TServiceBase::DoGetOrCreatePendingPayloadsEntry(TRequestBucket* bucket, TRequestId requestId)
{
    auto& entry = bucket->RequestIdToPendingPayloads[requestId];
    if (!entry.Lease) {
        entry.Lease = NConcurrency::TLeaseManager::CreateLease(
            PendingPayloadsTimeout_.load(std::memory_order_relaxed),
            BIND(&TServiceBase::OnPendingPayloadsLeaseExpired, MakeWeak(this), requestId));
    }

    return &entry;
}

std::vector<TStreamingPayload> TServiceBase::GetAndErasePendingPayloads(TRequestId requestId)
{
    auto* bucket = GetRequestBucket(requestId);

    TPendingPayloadsEntry entry;
    {
        auto guard = Guard(bucket->Lock);
        auto it = bucket->RequestIdToPendingPayloads.find(requestId);
        if (it == bucket->RequestIdToPendingPayloads.end()) {
            return {};
        }
        entry = std::move(it->second);
        bucket->RequestIdToPendingPayloads.erase(it);
    }

    NConcurrency::TLeaseManager::CloseLease(entry.Lease);
    return std::move(entry.Payloads);
}

void TServiceBase::OnPendingPayloadsLeaseExpired(TRequestId requestId)
{
    auto payloads = GetAndErasePendingPayloads(requestId);
    if (!payloads.empty()) {
        YT_LOG_DEBUG("Pending payloads lease expired, erasing (RequestId: %v, PayloadCount: %v)",
            requestId,
            payloads.size());
    }
}

TServiceBase::TMethodPerformanceCountersPtr TServiceBase::CreateMethodPerformanceCounters(
    const TRuntimeMethodInfoPtr& runtimeInfo,
    const std::optional<TString>& userTag)
{
    TRegistry registry = runtimeInfo->Registry.WithSparse();
    if (userTag) {
        registry = registry.WithTag("user", *userTag);
    }

    return New<TMethodPerformanceCounters>(registry);
}

TServiceBase::TMethodPerformanceCounters* TServiceBase::GetMethodPerformanceCounters(
    const TRuntimeMethodInfoPtr& runtimeInfo,
    const TString& userTag)
{
    if (!EnablePerUserProfiling_.load(std::memory_order_relaxed)) {
        return runtimeInfo->GlobalPerformanceCounters.Get();
    }

    // Fast path.
    if (userTag == RootUserName) {
        return runtimeInfo->RootPerformanceCounters.Get();
    }

    // Also fast path.
    return runtimeInfo->UserTagToPerformanceCounters.FindOrInsert(userTag, [&, this] {
        return CreateMethodPerformanceCounters(runtimeInfo, userTag);
    }).first->Get();
}

void TServiceBase::SetActive()
{
    if (Active_.load(std::memory_order_relaxed)) {
        return;
    }

    if (Active_.exchange(true)) {
        return;
    }
}

void TServiceBase::ValidateInactive()
{
    // Failure here means that some service metadata (e.g. registered methods or supported
    // features) are changing after the service has started serving requests.
    YT_VERIFY(!Active_.load());
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    ValidateInactive();

    auto runtimeInfo = New<TRuntimeMethodInfo>(descriptor, ProfilingRegistry_);
    runtimeInfo->GlobalPerformanceCounters = CreateMethodPerformanceCounters(runtimeInfo, std::nullopt);
    runtimeInfo->RootPerformanceCounters = CreateMethodPerformanceCounters(runtimeInfo, RootUserName);

    // Failure here means that such method is already registered.
    YT_VERIFY(MethodMap_.emplace(descriptor.Method, runtimeInfo).second);
    return runtimeInfo;
}

void TServiceBase::ValidateRequestFeatures(const IServiceContextPtr& context)
{
    const auto& header = context->RequestHeader();
    if (auto error = DoCheckRequestFeatures(header); !error.IsOK()) {
        auto requestId = FromProto<TRequestId>(header.request_id());
        THROW_ERROR std::move(error)
            << TErrorAttribute("request_id", requestId)
            << TErrorAttribute("service", header.service())
            << TErrorAttribute("method", header.method());
    }
}

void TServiceBase::DoConfigure(TServiceCommonConfigPtr configDefaults, TServiceConfigPtr config)
{
    try {
        YT_LOG_DEBUG("Configuring RPC service (Service: %v)",
            ServiceId_.ServiceName);

        // Validate configuration.
        for (const auto& [methodName, _] : config->Methods) {
            GetMethodInfoOrThrow(methodName);
        }

        EnablePerUserProfiling_.store(config->EnablePerUserProfiling.value_or(configDefaults->EnablePerUserProfiling));
        AuthenticationQueueSizeLimit_.store(config->AuthenticationQueueSizeLimit);
        PendingPayloadsTimeout_.store(config->PendingPayloadsTimeout);

        for (const auto& [methodName, runtimeInfo] : MethodMap_) {
            TMethodConfigPtr methodConfig;
            if (auto it = config->Methods.find(methodName)) {
                methodConfig = it->second;
            } else {
                methodConfig = New<TMethodConfig>();
            }

            // TODO(babenko): fix races here
            auto& descriptor = runtimeInfo->Descriptor;
            descriptor.SetHeavy(methodConfig->Heavy);
            descriptor.SetQueueSizeLimit(methodConfig->QueueSizeLimit);
            descriptor.SetConcurrencyLimit(methodConfig->ConcurrencyLimit);
            descriptor.SetLogLevel(methodConfig->LogLevel);
            descriptor.SetLoggingSuppressionTimeout(methodConfig->LoggingSuppressionTimeout);

            auto requestBytesThrottlerConfig = methodConfig->RequestBytesThrottler
                ? methodConfig->RequestBytesThrottler
                : New<TThroughputThrottlerConfig>();
            runtimeInfo->RequestBytesThrottler->Reconfigure(requestBytesThrottlerConfig);
            runtimeInfo->RequestBytesThrottlerSpecified.store(methodConfig->RequestBytesThrottler.operator bool());

            runtimeInfo->LoggingSuppressionFailedRequestThrottler->Reconfigure(
                methodConfig->LoggingSuppressionFailedRequestThrottler);

            runtimeInfo->ForceTracing.store(
                methodConfig->ForceTracing.value_or(
                    config->ForceTracing.value_or(
                        configDefaults->ForceTracing)));
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error configuring RPC service %v",
            ServiceId_.ServiceName)
            << ex;
    }
}

void TServiceBase::Configure(TServiceCommonConfigPtr configDefaults, INodePtr configNode)
{
    TServiceConfigPtr config;
    if (configNode) {
        try {
            config = ConvertTo<TServiceConfigPtr>(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing RPC service %v config",
                ServiceId_.ServiceName)
                << ex;
        }
    } else {
        config = New<TServiceConfig>();
    }
    DoConfigure(std::move(configDefaults), std::move(config));
}

TFuture<void> TServiceBase::Stop()
{
    bool expected = false;
    if (Stopped_.compare_exchange_strong(expected, true)) {
        if (ActiveRequestCount_.load() == 0) {
            StopResult_.TrySet();
        }
    }
    return StopResult_.ToFuture();
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::FindMethodInfo(const TString& method)
{
    auto it = MethodMap_.find(method);
    return it == MethodMap_.end() ? nullptr : it->second;
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::GetMethodInfo(const TString& method)
{
    auto runtimeInfo = FindMethodInfo(method);
    YT_VERIFY(runtimeInfo);
    return runtimeInfo;
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::GetMethodInfoOrThrow(const TString& method)
{
    auto runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        THROW_ERROR_EXCEPTION("Method %Qv is not registered");
    }
    return runtimeInfo;
}

const IInvokerPtr& TServiceBase::GetDefaultInvoker() const
{
    return DefaultInvoker_;
}

void TServiceBase::BeforeInvoke(NRpc::IServiceContext* context)
{ }

bool TServiceBase::IsUp(const TCtxDiscoverPtr& /*context*/)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

std::vector<TString> TServiceBase::SuggestAddresses()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return std::vector<TString>();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TServiceBase, Discover)
{
    context->SetRequestInfo();

    response->set_up(IsUp(context));
    ToProto(response->mutable_suggested_addresses(), SuggestAddresses());

    context->SetResponseInfo("Up: %v, SuggestedAddresses: %v",
        response->up(),
        response->suggested_addresses());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

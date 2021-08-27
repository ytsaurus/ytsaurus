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

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/string.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

#include <yt/yt/core/profiling/timing.h>

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

static const auto DefaultRequestBytesThrottlerConfig = New<TThroughputThrottlerConfig>();
static const auto DefaultLoggingSuppressionFailedRequestThrottlerConfig = New<TThroughputThrottlerConfig>(1'000);

////////////////////////////////////////////////////////////////////////////////

THandlerInvocationOptions THandlerInvocationOptions::SetHeavy(bool value) const
{
    auto result = *this;
    result.Heavy = value;
    return result;
}

THandlerInvocationOptions THandlerInvocationOptions::SetResponseCodec(NCompression::ECodec value) const
{
    auto result = *this;
    result.ResponseCodec = value;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    TString method,
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Method(std::move(method))
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
{ }

auto TServiceBase::TMethodDescriptor::SetRequestQueueProvider(TRequestQueueProvider value) const -> TMethodDescriptor
{
    auto result = *this;
    result.RequestQueueProvider = std::move(value);
    return result;
}

auto TServiceBase::TMethodDescriptor::SetInvoker(IInvokerPtr value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Invoker = std::move(value);
    return result;
}

auto TServiceBase::TMethodDescriptor::SetInvokerProvider(TInvokerProvider value) const -> TMethodDescriptor
{
    auto result = *this;
    result.InvokerProvider = std::move(value);
    return result;
}

auto TServiceBase::TMethodDescriptor::SetHeavy(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Options.Heavy = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetResponseCodec(NCompression::ECodec value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Options.ResponseCodec = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetQueueSizeLimit(int value) const -> TMethodDescriptor
{
    auto result = *this;
    result.QueueSizeLimit = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetConcurrencyLimit(int value) const -> TMethodDescriptor
{
    auto result = *this;
    result.ConcurrencyLimit = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetSystem(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.System = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetLogLevel(NLogging::ELogLevel value) const -> TMethodDescriptor
{
    auto result = *this;
    result.LogLevel = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetLoggingSuppressionTimeout(TDuration value) const -> TMethodDescriptor
{
    auto result = *this;
    result.LoggingSuppressionTimeout = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetCancelable(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Cancelable = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetGenerateAttachmentChecksums(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.GenerateAttachmentChecksums = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetStreamingEnabled(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.StreamingEnabled = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetPooled(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Pooled = value;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodPerformanceCounters::TMethodPerformanceCounters(
    const NProfiling::TProfiler& profiler,
    const THistogramConfigPtr& histogramConfig)
    : RequestCounter(profiler.Counter("/request_count"))
    , CanceledRequestCounter(profiler.Counter("/canceled_request_count"))
    , FailedRequestCounter(profiler.Counter("/failed_request_count"))
    , TimedOutRequestCounter(profiler.Counter("/timed_out_request_count"))
    , HandlerFiberTimeCounter(profiler.TimeCounter("/request_time/handler_fiber"))
    , TraceContextTimeCounter(profiler.TimeCounter("/request_time/trace_context"))
    , RequestMessageBodySizeCounter(profiler.Counter("/request_message_body_bytes"))
    , RequestMessageAttachmentSizeCounter(profiler.Counter("/request_message_attachment_bytes"))
    , ResponseMessageBodySizeCounter(profiler.Counter("/response_message_body_bytes"))
    , ResponseMessageAttachmentSizeCounter(profiler.Counter("/response_message_attachment_bytes"))
{   
    if (histogramConfig && histogramConfig->CustomBounds) {
        const auto &customBounds = *histogramConfig->CustomBounds;
        ExecutionTimeCounter = profiler.Histogram("/request_time_histogram/execution", customBounds);
        RemoteWaitTimeCounter = profiler.Histogram("/request_time_histogram/remote_wait", customBounds);
        LocalWaitTimeCounter = profiler.Histogram("/request_time_histogram/local_wait", customBounds);
        TotalTimeCounter = profiler.Histogram("/request_time_histogram/total", customBounds);
    } else if (histogramConfig && histogramConfig->ExponentialBounds) {
        const auto &exponentialBounds = *histogramConfig->ExponentialBounds;
        ExecutionTimeCounter = profiler.Histogram("/request_time_histogram/execution", exponentialBounds->Min, exponentialBounds->Max);
        RemoteWaitTimeCounter = profiler.Histogram("/request_time_histogram/remote_wait", exponentialBounds->Min, exponentialBounds->Max);
        LocalWaitTimeCounter = profiler.Histogram("/request_time_histogram/local_wait", exponentialBounds->Min, exponentialBounds->Max);
        TotalTimeCounter = profiler.Histogram("/request_time_histogram/total", exponentialBounds->Min, exponentialBounds->Max);
    } else {
        ExecutionTimeCounter = profiler.Timer("/request_time/execution");
        RemoteWaitTimeCounter = profiler.Timer("/request_time/remote_wait");
        LocalWaitTimeCounter = profiler.Timer("/request_time/local_wait");
        TotalTimeCounter = profiler.Timer("/request_time/total");
    }
}

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    TServiceId serviceId,
    TMethodDescriptor descriptor,
    const NProfiling::TProfiler& profiler)
    : ServiceId(std::move(serviceId))
    , Descriptor(std::move(descriptor))
    , Profiler(profiler.WithTag("method", Descriptor.Method, -1))
    , RequestLoggingAnchor(NLogging::TLogManager::Get()->RegisterDynamicAnchor(
        Format("%v.%v <-", ServiceId.ServiceName, Descriptor.Method)))
    , ResponseLoggingAnchor(NLogging::TLogManager::Get()->RegisterDynamicAnchor(
        Format("%v.%v ->", ServiceId.ServiceName, Descriptor.Method)))
    , LoggingSuppressionFailedRequestThrottler(
        CreateReconfigurableThroughputThrottler(DefaultLoggingSuppressionFailedRequestThrottlerConfig))
    , DefaultRequestQueue("default")
{ }

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        TServiceBasePtr&& service,
        TAcceptedRequest&& acceptedRequest,
        NLogging::TLogger logger)
        : TServiceContextBase(
            std::move(acceptedRequest.Header),
            std::move(acceptedRequest.Message),
            std::move(logger),
            acceptedRequest.RuntimeInfo->LogLevel.load(std::memory_order_relaxed))
        , Service_(service)
        , RequestId_(acceptedRequest.RequestId)
        , ReplyBus_(std::move(acceptedRequest.ReplyBus))
        , RuntimeInfo_(acceptedRequest.RuntimeInfo)
        , TraceContext_(std::move(acceptedRequest.TraceContext))
        , RequestQueue_(acceptedRequest.RequestQueue)
        , PerformanceCounters_(Service_->GetMethodPerformanceCounters(
            RuntimeInfo_,
            {GetAuthenticationIdentity().UserTag, RequestQueue_}))
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
        if (!Replied_ && !CanceledList_.IsFired()) {
            Reply(TError(NRpc::EErrorCode::Unavailable, "Service is unable to complete your request"));
        }

        Finalize();
    }

    TRequestQueue* GetRequestQueue() const
    {
        return RequestQueue_;
    }

    bool IsPooled() const override
    {
        return RuntimeInfo_->Descriptor.Pooled;
    }

    TTcpDispatcherStatistics GetBusStatistics() const override
    {
        return ReplyBus_->GetStatistics();
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return ReplyBus_->GetEndpointAttributes();
    }

    TRuntimeMethodInfo* GetRuntimeInfo() const
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
        const auto& descriptor = RuntimeInfo_->Descriptor;
        // NB: Try to avoid contention on invoker ref-counter.
        IInvoker* invoker = nullptr;
        IInvokerPtr invokerHolder;
        if (descriptor.InvokerProvider) {
            invokerHolder = descriptor.InvokerProvider(RequestHeader());
            invoker = invokerHolder.Get();
        }
        if (!invoker) {
            invoker = descriptor.Invoker.Get();
        }
        if (!invoker) {
            invoker = Service_->DefaultInvoker_.Get();
        }
        invoker->Invoke(BIND(&TServiceContext::DoRun, MakeStrong(this), handler));
    }

    void SubscribeCanceled(const TClosure& callback) override
    {
        CanceledList_.Subscribe(callback);
    }

    void UnsubscribeCanceled(const TClosure& callback) override
    {
        CanceledList_.Unsubscribe(callback);
    }

    void SubscribeReplied(const TClosure& callback) override
    {
        RepliedList_.Subscribe(callback);
    }

    void UnsubscribeReplied(const TClosure& callback) override
    {
        RepliedList_.Unsubscribe(callback);
    }

    bool IsCanceled() override
    {
        return CanceledList_.IsFired();
    }

    void Cancel() override
    {
        if (!CanceledList_.Fire()) {
            return;
        }

        YT_LOG_DEBUG("Request canceled (RequestId: %v)",
            RequestId_);

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto CanceledError = TError("Request canceled");
            AbortStreamsUnlessClosed(CanceledError);
        }

        CancelInstant_ = GetCpuInstant();

        PerformanceCounters_->CanceledRequestCounter.Increment();
    }

    void SetComplete() override
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

        CanceledList_.Fire();
        PerformanceCounters_->TimedOutRequestCounter.Increment();

        // Guards from race with DoGuardedRun.
        // We can only mark as complete those requests that will not be run
        // as there's no guarantee that, if started,  the method handler will respond promptly to cancelation.
        if (!RunLatch_.test_and_set()) {
            SetComplete();
        }
    }

    TInstant GetArriveInstant() const override
    {
        return CpuInstantToInstant(ArriveInstant_);
    }

    std::optional<TInstant> GetRunInstant() const override
    {
        return RunInstant_ == 0 ? std::nullopt : std::make_optional(CpuInstantToInstant(RunInstant_));
    }

    std::optional<TInstant> GetFinishInstant() const override
    {
        if (ReplyInstant_ != 0) {
            return CpuInstantToInstant(ReplyInstant_);
        } else if (CancelInstant_ != 0) {
            return CpuInstantToInstant(CancelInstant_);
        } else {
            return std::nullopt;
        }
    }

    std::optional<TDuration> GetWaitDuration() const override
    {
        return LocalWaitTime_ == TDuration::Zero() ? std::nullopt : std::make_optional(LocalWaitTime_);
    }

    std::optional<TDuration> GetExecutionDuration() const override
    {
        return ExecutionTime_ == TDuration::Zero() ? std::nullopt : std::make_optional(ExecutionTime_);
    }

    TTraceContextPtr GetTraceContext() const override
    {
        return TraceContext_;
    }

    IAsyncZeroCopyInputStreamPtr GetRequestAttachmentsStream() override
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
        }
        CreateRequestAttachmentsStream();
        return RequestAttachmentsStream_;
    }

    void SetResponseCodec(NCompression::ECodec codec) override
    {
        auto guard = Guard(StreamsLock_);
        if (ResponseAttachmentsStream_) {
            THROW_ERROR_EXCEPTION("Cannot update response codec after response attachments stream is accessed");
        }
        TServiceContextBase::SetResponseCodec(codec);
    }

    IAsyncZeroCopyOutputStreamPtr GetResponseAttachmentsStream() override
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
    TRuntimeMethodInfo* const RuntimeInfo_;
    const TTraceContextPtr TraceContext_;
    TRequestQueue* const RequestQueue_;
    TMethodPerformanceCounters* const PerformanceCounters_;

    EMemoryZone ResponseMemoryZone_;

    NCompression::ECodec RequestCodec_;

    TDelayedExecutorCookie TimeoutCookie_;

    bool Cancelable_ = false;
    TSingleShotCallbackList<void()> CanceledList_;

    const NProfiling::TCpuInstant ArriveInstant_;
    NProfiling::TCpuInstant RunInstant_ = 0;
    NProfiling::TCpuInstant ReplyInstant_ = 0;
    NProfiling::TCpuInstant CancelInstant_ = 0;

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
            auto retryStart = FromProto<TInstant>(RequestHeader_->start_time());
            auto now = NProfiling::GetInstant();
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

        Service_->IncrementActiveRequestCount();
        if (Service_->IsStopped()) {
                Reply(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Service is stopped"));
                return;
        }

        BuildGlobalRequestInfo();

        if (IsRegistrable()) {
            Service_->RegisterRequest(this);
        }

        if (RuntimeInfo_->Descriptor.Cancelable && !RequestHeader_->uncancelable()) {
            Cancelable_ = true;
            if (auto timeout = GetTimeout()) {
                TimeoutCookie_ = TDelayedExecutor::Submit(
                    BIND(&TServiceBase::OnRequestTimeout, Service_, RequestId_),
                    *timeout);
            }
        }
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
            TCurrentTraceContextGuard guard(TraceContext_);
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
                if (!CanceledList_.TrySubscribe(std::move(cancelationHandler))) {
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
            handler(this, descriptor.Options);
        }
    }

    void DoAfterRun(TDuration handlerElapsedValue)
    {
        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        PerformanceCounters_->HandlerFiberTimeCounter.Add(handlerElapsedValue);

        if (auto traceContextTime = GetTraceContextTime()) {
            PerformanceCounters_->TraceContextTimeCounter.Add(*traceContextTime);
        }
    }

    std::optional<TDuration> GetTraceContextTime() const override
    {
        if (TraceContext_) {
            FlushCurrentTraceContextTime();
            return TraceContext_->GetElapsedTime();
        } else {
            return std::nullopt;
        }
    }

    void DoReply() override
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

        if (!Error_.IsOK() && TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddErrorTag();
        }

        Finalize();
    }

    void DoFlush() override
    {
        if (TraceContext_) {
            TraceContext_->Finish();
        }
    }

    void HandleLoggingSuppression()
    {
        auto timeout = RequestHeader_->has_logging_suppression_timeout()
            ? FromProto<TDuration>(RequestHeader_->logging_suppression_timeout())
            : RuntimeInfo_->LoggingSuppressionTimeout.load(std::memory_order_relaxed);

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


        RequestQueue_->OnRequestFinished();

        Service_->DecrementActiveRequestCount();
    }


    void LogRequest() override
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
        if (TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddTag(RequestInfoAnnotation, logMessage);
        }
        YT_LOG_EVENT_WITH_ANCHOR(Logger, LogLevel_, RuntimeInfo_->RequestLoggingAnchor, logMessage);
    }

    void LogResponse() override
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

        if (auto traceContextTime = GetTraceContextTime()) {
            delimitedBuilder->AppendFormat("CpuTime: %v", traceContextTime);
        }

        auto logMessage = builder.Flush();
        if (TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddTag(ResponseInfoAnnotation, logMessage);
        }
        YT_LOG_EVENT_WITH_ANCHOR(Logger, LogLevel_, RuntimeInfo_->ResponseLoggingAnchor, logMessage);
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
};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TRequestQueue::TRequestQueue(TString name)
    : Name_(std::move(name))
    , RequestBytesThrottler_(
        CreateReconfigurableThroughputThrottler(DefaultRequestBytesThrottlerConfig))
{ }

const TString& TServiceBase::TRequestQueue::GetName() const
{
    return Name_;
}

bool TServiceBase::TRequestQueue::Register(TServiceBase* service, TRuntimeMethodInfo* runtimeInfo)
{
    // Fast path.
    if (Registered_.load()) {
        YT_ASSERT(service == Service_);
        YT_ASSERT(runtimeInfo == RuntimeInfo_);
        return false;
    }

    // Slow path.
    {
        auto guard = Guard(RegisterLock_);
        if (!Registered_) {
            Service_ = service;
            RuntimeInfo_ = runtimeInfo;
        }
        Registered_.store(true);
    }

    return true;
}

void TServiceBase::TRequestQueue::Configure(const TMethodConfigPtr& config)
{
    auto requestBytesThrottlerConfig = config->RequestBytesThrottler
        ? config->RequestBytesThrottler
        : DefaultRequestBytesThrottlerConfig;
    RequestBytesThrottler_->Reconfigure(requestBytesThrottlerConfig);
    RequestBytesThrottlerSpecified_.store(config->RequestBytesThrottler.operator bool());

    ScheduleRequestsFromQueue();
    SubscribeToRequestBytesThrottler();
}

bool TServiceBase::TRequestQueue::IsQueueLimitSizeExceeded() const
{
    return
        QueueSize_.load(std::memory_order_relaxed) >=
        RuntimeInfo_->QueueSizeLimit.load(std::memory_order_relaxed);
}

int TServiceBase::TRequestQueue::GetQueueSize() const
{
    return QueueSize_.load(std::memory_order_relaxed);
}

int TServiceBase::TRequestQueue::GetConcurrency() const
{
    return Concurrency_.load(std::memory_order_relaxed);
}

void TServiceBase::TRequestQueue::OnRequestArrived(TServiceContextPtr context)
{
    // Fast path.
    auto newConcurrencySemaphore = IncrementConcurrency();
    if (newConcurrencySemaphore <= RuntimeInfo_->ConcurrencyLimit.load(std::memory_order_relaxed) &&
        !IsRequestBytesThrottlerOverdraft())
    {
        RunRequest(std::move(context));
        return;
    }

    // Slow path.
    DecrementConcurrency();
    IncrementQueueSize();
    RequestQueue_.enqueue(std::move(context));
    ScheduleRequestsFromQueue();
}

void TServiceBase::TRequestQueue::OnRequestFinished()
{
    DecrementConcurrency();

    if (QueueSize_.load() > 0) {
        // Slow path.
        ScheduleRequestsFromQueue();
    }
}

// Prevents reentrant invocations.
static thread_local bool ScheduleRequestsLatch;

void TServiceBase::TRequestQueue::ScheduleRequestsFromQueue()
{
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

    // NB: racy, may lead to overcommit in concurrency semaphore and request bytes throttler.
    auto concurrencyLimit = RuntimeInfo_->ConcurrencyLimit.load(std::memory_order_relaxed);
    while (QueueSize_.load() > 0 && Concurrency_.load() < concurrencyLimit) {
        if (IsRequestBytesThrottlerOverdraft()) {
            SubscribeToRequestBytesThrottler();
            break;
        }

        TServiceContextPtr context;
        if (!RequestQueue_.try_dequeue(context)) {
            break;
        }

        DecrementQueueSize();
        IncrementConcurrency();
        RunRequest(std::move(context));
    }
}

void TServiceBase::TRequestQueue::RunRequest(TServiceContextPtr context)
{
    AcquireRequestBytesThrottler(context);

    auto options = RuntimeInfo_->Descriptor.Options;
    options.SetHeavy(RuntimeInfo_->Heavy.load(std::memory_order_relaxed));

    if (options.Heavy) {
        BIND(RuntimeInfo_->Descriptor.HeavyHandler)
            .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
            .Run(context, options)
            .Subscribe(BIND(&TServiceContext::CheckAndRun, std::move(context)));
    } else {
        context->Run(RuntimeInfo_->Descriptor.LiteHandler);
    }
}

int TServiceBase::TRequestQueue::IncrementQueueSize()
{
    return ++QueueSize_;
}

void  TServiceBase::TRequestQueue::DecrementQueueSize()
{
    auto newQueueSize = --QueueSize_;
    YT_ASSERT(newQueueSize >= 0);
}

int TServiceBase::TRequestQueue::IncrementConcurrency()
{
    return ++Concurrency_;
}

void  TServiceBase::TRequestQueue::DecrementConcurrency()
{
    auto newConcurrencySemaphore = --Concurrency_;
    YT_ASSERT(newConcurrencySemaphore >= 0);
}

bool TServiceBase::TRequestQueue::IsRequestBytesThrottlerOverdraft() const
{
    return
        RequestBytesThrottlerSpecified_.load(std::memory_order_relaxed) &&
        RequestBytesThrottler_->IsOverdraft();
}

void TServiceBase::TRequestQueue::AcquireRequestBytesThrottler(const TServiceContextPtr& context)
{
    if (RequestBytesThrottlerSpecified_.load(std::memory_order_relaxed)) {
        // Slow path.
        auto requestSize =
            GetMessageBodySize(context->GetRequestMessage()) +
            GetTotalMessageAttachmentSize(context->GetRequestMessage());
        RequestBytesThrottler_->Acquire(requestSize);
    }
}

void TServiceBase::TRequestQueue::SubscribeToRequestBytesThrottler()
{
    if (RequestBytesThrottlerThrottled_.exchange(true)) {
        return;
    }
    RequestBytesThrottler_->Throttle(1)
        .Subscribe(BIND([=, weakService = MakeWeak(Service_)] (const TError&) {
            if (auto service = weakService.Lock()) {
                RequestBytesThrottlerThrottled_.store(false);
                ScheduleRequestsFromQueue();
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

struct TServiceBase::TRuntimeMethodInfo::TPerformanceCountersKeyEquals
{
    bool operator()(
        const TNonowningPerformanceCountersKey& lhs,
        const TNonowningPerformanceCountersKey& rhs) const
    {
        return lhs == rhs;
    }

    bool operator()(
        const TOwningPerformanceCountersKey& lhs,
        const TNonowningPerformanceCountersKey& rhs) const
    {
        const auto& [lhsUserTag, lhsRequestQueue] = lhs;
        return TNonowningPerformanceCountersKey{lhsUserTag, lhsRequestQueue} == rhs;
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
    , Profiler_(RpcServerProfiler.WithHot().WithTag("yt_service", ServiceId_.ServiceName))
    , AuthenticationTimer_(Profiler_.Timer("/authentication_time"))
{
    YT_VERIFY(DefaultInvoker_);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(TDispatcher::Get()->GetHeavyInvoker())
        .SetSystem(true));

    Profiler_.AddFuncGauge("/authentication_queue_size", MakeStrong(this), [=] {
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

    if (IsStopped()) {
        replyError(TError(
            NRpc::EErrorCode::Unavailable,
            "Service is stopped"));
        return;
    }

    if (auto error = DoCheckRequestCompatibility(*header); !error.IsOK()) {
        replyError(std::move(error));
        return;
    }

    auto* runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        replyError(TError(
            NRpc::EErrorCode::NoSuchMethod,
            "Unknown method"));
        return;
    }

    auto tracingMode = runtimeInfo->TracingMode.load(std::memory_order_relaxed);
    auto traceContext = tracingMode == ERequestTracingMode::Disable
        ? NTracing::TTraceContextPtr()
        : GetOrCreateHandlerTraceContext(*header, tracingMode == ERequestTracingMode::Force);
    if (traceContext && traceContext->IsRecorded()) {
        traceContext->AddTag(EndpointAnnotation, replyBus->GetEndpointDescription());
    }

    auto* requestQueue = GetRequestQueue(runtimeInfo, *header);
    RegisterRequestQueue(runtimeInfo, requestQueue);

    if (requestQueue->IsQueueLimitSizeExceeded()) {
        replyError(TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Request queue size limit exceeded")
            << TErrorAttribute("limit", runtimeInfo->QueueSizeLimit.load())
            << TErrorAttribute("queue", requestQueue->GetName()));
        return;
    }

    TCurrentTraceContextGuard traceContextGuard(traceContext);

    // NOTE: Do not use replyError() after this line.
    TAcceptedRequest acceptedRequest{
        requestId,
        std::move(replyBus),
        std::move(runtimeInfo),
        std::move(traceContext),
        std::move(header),
        std::move(message),
        requestQueue
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

    TAuthenticationContext authenticationContext{
        .Header = acceptedRequest.Header.get(),
        .UserIP = acceptedRequest.ReplyBus->GetEndpointAddress()
    };
    auto asyncAuthResult = Authenticator_->Authenticate(authenticationContext);
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
    TAcceptedRequest&& acceptedRequest,
    const TErrorOr<TAuthenticationResult>& authResultOrError)
{
    AuthenticationTimer_.Record(timer.GetElapsedTime());
    --AuthenticationQueueSize_;

    auto& requestHeader = *acceptedRequest.Header;

    if (authResultOrError.IsOK()) {
        const auto& authResult = authResultOrError.Value();
        const auto& Logger = RpcServerLogger;
        YT_LOG_DEBUG("Request authenticated (RequestId: %v, User: %v, Realm: %v)",
            acceptedRequest.RequestId,
            authResult.User,
            authResult.Realm);
        const auto& authenticatedUser = authResult.User;
        if (requestHeader.has_user()) {
            const auto& user = requestHeader.user();
            if (user != authenticatedUser) {
                ReplyError(
                    TError(
                        NRpc::EErrorCode::AuthenticationError,
                        "Manually specified and authenticated users mismatch")
                        << TErrorAttribute("user", user)
                        << TErrorAttribute("authenticated_user", authenticatedUser),
                    requestHeader,
                    acceptedRequest.ReplyBus);
                return;
            }
        }
        requestHeader.set_user(std::move(authResult.User));

        auto* credentialsExt = requestHeader.MutableExtension(
            NRpc::NProto::TCredentialsExt::credentials_ext);
        if (credentialsExt->user_ticket().empty()) {
            credentialsExt->set_user_ticket(std::move(authResult.UserTicket));
        }
        HandleAuthenticatedRequest(std::move(acceptedRequest));
    } else {
        ReplyError(
            TError(
                NRpc::EErrorCode::AuthenticationError,
                "Request authentication failed")
                << authResultOrError,
            requestHeader,
            acceptedRequest.ReplyBus);
    }
}

bool TServiceBase::IsAuthenticationNeeded(const TAcceptedRequest& acceptedRequest)
{
    return
        Authenticator_.operator bool() &&
        !acceptedRequest.RuntimeInfo->Descriptor.System;
}

void TServiceBase::HandleAuthenticatedRequest(TAcceptedRequest&& acceptedRequest)
{
    auto context = New<TServiceContext>(
        this,
        std::move(acceptedRequest),
        Logger);
    auto* requestQueue = context->GetRequestQueue();
    requestQueue->OnRequestArrived(std::move(context));
}

TServiceBase::TRequestQueue* TServiceBase::GetRequestQueue(
    TRuntimeMethodInfo* runtimeInfo,
    const NRpc::NProto::TRequestHeader& requestHeader)
{
    TRequestQueue* requestQueue = nullptr;
    if (runtimeInfo->Descriptor.RequestQueueProvider) {
        requestQueue = runtimeInfo->Descriptor.RequestQueueProvider(requestHeader);
    }
    if (!requestQueue) {
        requestQueue = &runtimeInfo->DefaultRequestQueue;
    }
    return requestQueue;
}

void TServiceBase::RegisterRequestQueue(
    TRuntimeMethodInfo* runtimeInfo,
    TRequestQueue* requestQueue)
{
    if (!requestQueue->Register(this, runtimeInfo)) {
        return;
    }

    YT_LOG_DEBUG("Request queue registered (Method: %v, Queue: %v)",
        runtimeInfo->Descriptor.Method,
        requestQueue->GetName());

    auto profiler = runtimeInfo->Profiler;
    if (runtimeInfo->Descriptor.RequestQueueProvider) {
        profiler = profiler.WithTag("queue", requestQueue->GetName());
    }
    profiler.AddFuncGauge("/request_queue_size", MakeStrong(this), [=] {
        return requestQueue->GetQueueSize();
    });
    profiler.AddFuncGauge("/concurrency", MakeStrong(this), [=] {
        return requestQueue->GetConcurrency();
    });

    if (auto config = Config_.Load()) {
        if (auto methodIt = config->Methods.find(runtimeInfo->Descriptor.Method)) {
            requestQueue->Configure(methodIt->second);
        }
    }

    {
        auto guard = Guard(runtimeInfo->RequestQueuesLock);
        runtimeInfo->RequestQueues.push_back(requestQueue);
    }
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
                NRpc::EErrorCode::ProtocolError,
                "Server major protocol version differs from client major protocol version: %v != %v",
                requestProtocolVersion.Major,
                ServiceDescriptor_.ProtocolVersion.Major);
        }

        if (ServiceDescriptor_.ProtocolVersion.Minor < requestProtocolVersion.Minor) {
            return TError(
                NRpc::EErrorCode::ProtocolError,
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
                NRpc::EErrorCode::UnsupportedServerFeature,
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
    TRuntimeMethodInfo* runtimeInfo,
    const TRuntimeMethodInfo::TNonowningPerformanceCountersKey& key)
{
    const auto& [userTag, requestQueue] = key;

    auto profiler = runtimeInfo->Profiler.WithSparse();
    if (userTag) {
        profiler = profiler.WithTag("user", TString(userTag));
    }
    if (runtimeInfo->Descriptor.RequestQueueProvider) {
        profiler = profiler.WithTag("queue", requestQueue->GetName());
    }
    const auto config = [&]{
        const auto guard = Guard(HistogramConfigLock_);
        return HistogramTimerProfiling;
    }();
    return New<TMethodPerformanceCounters>(profiler, config);
}

TServiceBase::TMethodPerformanceCounters* TServiceBase::GetMethodPerformanceCounters(
    TRuntimeMethodInfo* runtimeInfo,
    const TRuntimeMethodInfo::TNonowningPerformanceCountersKey& key)
{
    auto [userTag, requestQueue] = key;

    // Fast path.
    if (userTag == RootUserName && requestQueue == &runtimeInfo->DefaultRequestQueue) {
        return runtimeInfo->RootPerformanceCounters.Get();
    }

    // Also fast path.
    if (!EnablePerUserProfiling_.load(std::memory_order_relaxed)) {
        userTag = {};
    }
    auto actualKey = TRuntimeMethodInfo::TNonowningPerformanceCountersKey{userTag, requestQueue};
    return runtimeInfo->PerformanceCountersMap.FindOrInsert(actualKey, [&] {
        return CreateMethodPerformanceCounters(runtimeInfo, actualKey);
    }).first->Get();
}

void TServiceBase::SetActive()
{
    // Fast path.
    if (Active_.load(std::memory_order_relaxed)) {
        return;
    }

    // Slow path.
    Active_.store(true);
}

void TServiceBase::ValidateInactive()
{
    // Failure here means that some service metadata (e.g. registered methods or supported
    // features) are changing after the service has started serving requests.
    YT_VERIFY(!Active_.load(std::memory_order_relaxed));
}

bool TServiceBase::IsStopped()
{
    return Stopped_.load();
}

void TServiceBase::IncrementActiveRequestCount()
{
    ++ActiveRequestCount_;
}

void TServiceBase::DecrementActiveRequestCount()
{
    if (--ActiveRequestCount_ == 0 && IsStopped()) {
        StopResult_.TrySet();
    }
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    ValidateInactive();

    auto runtimeInfo = New<TRuntimeMethodInfo>(ServiceId_, descriptor, Profiler_);

    runtimeInfo->RootPerformanceCounters = CreateMethodPerformanceCounters(
        runtimeInfo.Get(),
        {RootUserName, &runtimeInfo->DefaultRequestQueue});

    runtimeInfo->Heavy.store(descriptor.Options.Heavy);
    runtimeInfo->QueueSizeLimit.store(descriptor.QueueSizeLimit);
    runtimeInfo->ConcurrencyLimit.store(descriptor.ConcurrencyLimit);
    runtimeInfo->LogLevel.store(descriptor.LogLevel);
    runtimeInfo->LoggingSuppressionTimeout.store(descriptor.LoggingSuppressionTimeout);

    // Failure here means that such method is already registered.
    YT_VERIFY(MethodMap_.emplace(descriptor.Method, runtimeInfo).second);

    auto& profiler = runtimeInfo->Profiler;
    profiler.AddFuncGauge("/request_queue_size_limit", MakeStrong(this), [=] {
        return runtimeInfo->QueueSizeLimit.load(std::memory_order_relaxed);
    });
    profiler.AddFuncGauge("/concurrency_limit", MakeStrong(this), [=] {
        return runtimeInfo->ConcurrencyLimit.load(std::memory_order_relaxed);
    });

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

void TServiceBase::DoConfigure(
    const TServiceCommonConfigPtr& configDefaults,
    const TServiceConfigPtr& config)
{
    try {
        YT_LOG_DEBUG("Configuring RPC service (Service: %v)",
            ServiceId_.ServiceName);

        // Validate configuration.
        for (const auto& [methodName, _] : config->Methods) {
            GetMethodInfoOrThrow(methodName);
        }

        EnablePerUserProfiling_.store(config->EnablePerUserProfiling.value_or(configDefaults->EnablePerUserProfiling));
        AuthenticationQueueSizeLimit_.store(config->AuthenticationQueueSizeLimit.value_or(DefaultAuthenticationQueueSizeLimit));
        PendingPayloadsTimeout_.store(config->PendingPayloadsTimeout.value_or(DefaultPendingPayloadsTimeout));
        
        {
            THistogramConfigPtr finalConfig = nullptr;
            if (config->HistogramTimerProfiling) {
                finalConfig = config->HistogramTimerProfiling;
            } else if (configDefaults->HistogramTimerProfiling) {
                 finalConfig = configDefaults->HistogramTimerProfiling;
            }
            if (finalConfig) {
                const auto guard = Guard(HistogramConfigLock_);
                HistogramTimerProfiling = finalConfig;
            }
        }

        for (const auto& [methodName, runtimeInfo] : MethodMap_) {
            auto methodIt = config->Methods.find(methodName);
            auto methodConfig = methodIt ? methodIt->second : New<TMethodConfig>();

            const auto& descriptor = runtimeInfo->Descriptor;
            runtimeInfo->Heavy.store(methodConfig->Heavy.value_or(descriptor.Options.Heavy));
            runtimeInfo->QueueSizeLimit.store(methodConfig->QueueSizeLimit.value_or(descriptor.QueueSizeLimit));
            runtimeInfo->ConcurrencyLimit.store(methodConfig->ConcurrencyLimit.value_or(descriptor.ConcurrencyLimit));
            runtimeInfo->LogLevel.store(methodConfig->LogLevel.value_or(descriptor.LogLevel));
            runtimeInfo->LoggingSuppressionTimeout.store(methodConfig->LoggingSuppressionTimeout.value_or(descriptor.LoggingSuppressionTimeout));

            {
                auto guard = Guard(runtimeInfo->RequestQueuesLock);
                for (auto* requestQueue : runtimeInfo->RequestQueues) {
                    requestQueue->Configure(methodConfig);
                }
            }

            auto loggingSuppressionFailedRequestThrottlerConfig = methodConfig->LoggingSuppressionFailedRequestThrottler
                ? methodConfig->LoggingSuppressionFailedRequestThrottler
                : DefaultLoggingSuppressionFailedRequestThrottlerConfig;
            runtimeInfo->LoggingSuppressionFailedRequestThrottler->Reconfigure(loggingSuppressionFailedRequestThrottlerConfig);

            runtimeInfo->TracingMode.store(
                methodConfig->TracingMode.value_or(
                    config->TracingMode.value_or(
                        configDefaults->TracingMode)));
        }

        Config_.Store(config);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error configuring RPC service %v",
            ServiceId_.ServiceName)
            << ex;
    }
}

void TServiceBase::Configure(
    const TServiceCommonConfigPtr& configDefaults,
    const INodePtr& configNode)
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
    DoConfigure(configDefaults, config);
}

TFuture<void> TServiceBase::Stop()
{
    if (!Stopped_.exchange(true)) {
        if (ActiveRequestCount_.load() == 0) {
            StopResult_.TrySet();
        }
    }
    return StopResult_.ToFuture();
}

TServiceBase::TRuntimeMethodInfo* TServiceBase::FindMethodInfo(const TString& method)
{
    auto it = MethodMap_.find(method);
    return it == MethodMap_.end() ? nullptr : it->second.Get();
}

TServiceBase::TRuntimeMethodInfo* TServiceBase::GetMethodInfoOrThrow(const TString& method)
{
    auto* runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        THROW_ERROR_EXCEPTION("Method %Qv is not registered",
            method);
    }
    return runtimeInfo;
}

const IInvokerPtr& TServiceBase::GetDefaultInvoker() const
{
    return DefaultInvoker_;
}

void TServiceBase::BeforeInvoke(NRpc::IServiceContext* /*context*/)
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

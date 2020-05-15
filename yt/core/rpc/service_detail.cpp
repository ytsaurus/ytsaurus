#include "service_detail.h"
#include "private.h"
#include "config.h"
#include "dispatcher.h"
#include "helpers.h"
#include "message.h"
#include "response_keeper.h"
#include "server_detail.h"
#include "authenticator.h"
#include "stream.h"

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
#include <yt/core/misc/tls_cache.h>
#include <yt/core/misc/finally.h>

#include <yt/core/ytalloc/memory_zone.h>

#include <yt/core/profiling/profile_manager.h>
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

static const auto& Profiler = RpcServerProfiler;

static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    const TString& method,
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Method(method)
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
{ }

TServiceBase::TMethodPerformanceCounters::TMethodPerformanceCounters(const NProfiling::TTagIdList& tagIds)
    : RequestCounter("/request_count", tagIds)
    , CanceledRequestCounter("/canceled_request_count", tagIds)
    , FailedRequestCounter("/failed_request_count", tagIds)
    , TimedOutRequestCounter("/timed_out_request_count", tagIds)
    , ExecutionTimeCounter("/request_time/execution", tagIds)
    , RemoteWaitTimeCounter("/request_time/remote_wait", tagIds)
    , LocalWaitTimeCounter("/request_time/local_wait", tagIds)
    , TotalTimeCounter("/request_time/total", tagIds)
    , HandlerFiberTimeCounter("/request_time/handler_fiber", tagIds)
    , TraceContextTimeCounter("/request_time/trace_context", tagIds)
    , RequestMessageBodySizeCounter("/request_message_body_bytes", tagIds)
    , RequestMessageAttachmentSizeCounter("/request_message_attachment_bytes", tagIds)
    , ResponseMessageBodySizeCounter("/response_message_body_bytes", tagIds)
    , ResponseMessageAttachmentSizeCounter("/response_message_attachment_bytes", tagIds)
{ }

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    const TMethodDescriptor& descriptor,
    const NProfiling::TTagIdList& tagIds)
    : Descriptor(descriptor)
    , TagIds(tagIds)
    , QueueSizeCounter("/request_queue_size", tagIds)
    , QueueSizeLimitCounter("/request_queue_size_limit", tagIds)
    , ConcurrencyCounter("/concurrency", tagIds)
    , ConcurrencyLimitCounter("/concurrency_limit", tagIds)
    , LoggingSuppressionFailedRequestThrottler(
        CreateReconfigurableThroughputThrottler(TMethodConfig::DefaultLoggingSuppressionFailedRequestThrottler))
{ }

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
        , PerformanceCounters_(Service_->GetMethodPerformanceCounters(RuntimeInfo_, User_))
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
        GetInvoker()->Invoke(BIND(&TServiceContext::DoRun, MakeStrong(this), handler));
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

        Profiler.Increment(PerformanceCounters_->CanceledRequestCounter);
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
        Profiler.Increment(PerformanceCounters_->TimedOutRequestCounter);

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

    TSpinLock StreamsLock_;
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
        Profiler.Increment(PerformanceCounters_->RequestCounter);
        Profiler.Increment(
            PerformanceCounters_->RequestMessageBodySizeCounter,
            GetMessageBodySize(RequestMessage_));
        Profiler.Increment(
            PerformanceCounters_->RequestMessageAttachmentSizeCounter,
            GetTotalMessageAttachmentSize(RequestMessage_));

        if (RequestHeader_->has_start_time()) {
            // Decode timing information.
            auto retryStart = FromProto<TInstant>(RequestHeader_->start_time());
            auto now = NProfiling::GetInstant();

            // Make sanity adjustments to account for possible clock skew.
            retryStart = std::min(retryStart, now);

            Profiler.Update(PerformanceCounters_->RemoteWaitTimeCounter, DurationToValue(now - retryStart));
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
            DoAfterRun(timer.GetElapsedValue());
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
        Profiler.Update(PerformanceCounters_->LocalWaitTimeCounter, DurationToValue(LocalWaitTime_));
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
                Profiler.Increment(PerformanceCounters_->TimedOutRequestCounter);
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

        handler.Run(this, descriptor.Options);
    }

    void DoAfterRun(NProfiling::TValue handlerElapsedValue)
    {
        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        Profiler.Increment(PerformanceCounters_->HandlerFiberTimeCounter, handlerElapsedValue);

        if (TraceContext_) {
            FlushCurrentTraceContextTime();
            auto traceContextTime = TraceContext_->GetElapsedTime();
            Profiler.Increment(PerformanceCounters_->TraceContextTimeCounter, DurationToValue(traceContextTime));
        }
    }

    virtual void DoReply() override
    {
        if (TraceContext_) {
            TraceContext_->Finish();
        }

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

        Profiler.Update(PerformanceCounters_->ExecutionTimeCounter, DurationToValue(ExecutionTime_));
        Profiler.Update(PerformanceCounters_->TotalTimeCounter, DurationToValue(TotalTime_));
        if (!Error_.IsOK()) {
            Profiler.Increment(PerformanceCounters_->FailedRequestCounter);
        }

        HandleLoggingSuppression();

        Profiler.Increment(
            PerformanceCounters_->ResponseMessageBodySizeCounter,
            GetMessageBodySize(responseMessage));
        Profiler.Increment(
            PerformanceCounters_->ResponseMessageAttachmentSizeCounter,
            GetTotalMessageAttachmentSize(responseMessage));

        Finalize();
    }

    void HandleLoggingSuppression()
    {
        auto context = GetCurrentTraceContext();
        if (!context) {
            return;
        }

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

        NLogging::TLogManager::Get()->SuppressTrace(context->GetTraceId());
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
    , ServiceId_(descriptor.GetFullServiceName(), realmId)
    , ProtocolVersion_(descriptor.ProtocolVersion)
    , ServiceTagId_(NProfiling::TProfileManager::Get()->RegisterTag("service", ServiceId_.ServiceName))
    , ProfilingExecutor_(New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TServiceBase::OnProfiling, MakeWeak(this)),
        ProfilingPeriod))
    , AuthenticationQueueSizeCounter_("/authentication_queue_size", {ServiceTagId_})
    , AuthenticationTimeCounter_("/authentication_time", {ServiceTagId_})
{
    YT_VERIFY(DefaultInvoker_);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(GetSyncInvoker())
        .SetSystem(true));

    ProfilingExecutor_->Start();
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
    const auto& method = header->method();
    auto requestId = FromProto<TRequestId>(header->request_id());

    TProtocolVersion requestProtocolVersion{
        header->protocol_version_major(),
        header->protocol_version_minor()
    };

    TRuntimeMethodInfoPtr runtimeInfo;
    auto replyError = [&] (TError error) {
        ReplyError(std::move(error), *header, replyBus);
    };

    if (Stopped_) {
        replyError(TError(
            EErrorCode::Unavailable,
            "Service is stopped"));
        return;
    }

    if (requestProtocolVersion.Major != GenericProtocolVersion.Major) {
        if (ProtocolVersion_.Major != requestProtocolVersion.Major) {
            auto error = TError(
                EErrorCode::ProtocolError,
                "Server major protocol version differs from client major protocol version: %v != %v",
                requestProtocolVersion.Major,
                ProtocolVersion_.Major);
            replyError(std::move(error));
            return;
        }

        if (ProtocolVersion_.Minor < requestProtocolVersion.Minor) {
            auto error = TError(
                EErrorCode::ProtocolError,
                "Server minor protocol version is less than minor protocol version required by client: %v < %v",
                ProtocolVersion_.Minor,
                requestProtocolVersion.Minor);
            replyError(std::move(error));
            return;
        }
    }

    runtimeInfo = FindMethodInfo(method);
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

    auto traceContext = CreateHandlerTraceContext(*header);
    if (traceContext) {
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

    if (!Authenticator_) {
        HandleAuthenticatedRequest(std::move(acceptedRequest));
        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    auto authenticationQueueSizeLimit = AuthenticationQueueSizeLimit_;
    if (AuthenticationQueueSize_.load() > authenticationQueueSizeLimit) {
        auto error = TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Authentication request queue size limit exceeded")
            << TErrorAttribute("limit", authenticationQueueSizeLimit);
        ReplyError(error, *acceptedRequest.Header, acceptedRequest.ReplyBus);
        return;
    }
    ++AuthenticationQueueSize_;

    NProfiling::TWallTimer timer;

    TAuthenticationContext context;
    context.Header = acceptedRequest.Header.get();
    context.UserIP = acceptedRequest.ReplyBus->GetEndpointAddress();
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
    auto requestId = FromProto<TRequestId>(header.request_id());
    auto richError = std::move(error)
        << TErrorAttribute("request_id", requestId)
        << TErrorAttribute("realm_id", ServiceId_.RealmId)
        << TErrorAttribute("service", ServiceId_.ServiceName)
        << TErrorAttribute("method", header.method())
        << TErrorAttribute("endpoint", replyBus->GetEndpointDescription());

    NTracing::AddErrorTag();

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
    Profiler.Update(AuthenticationTimeCounter_, timer.GetElapsedValue());
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
    TGuard<TSpinLock> guard(RequestMapLock_);
    auto context = DoFindRequest(requestId);
    if (context) {
        guard.Release();
        context->HandleStreamingPayload(payload);
    } else {
        auto* entry = DoGetOrCreatePendingPayloadsEntry(requestId);
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

void TServiceBase::OnRequestTimeout(TRequestId requestId, bool /*aborted*/)
{
    auto context = FindRequest(requestId);
    if (!context) {
        return;
    }

    context->HandleTimeout();
}

void TServiceBase::OnReplyBusTerminated(IBusPtr bus, const TError& error)
{
    std::vector<TServiceContextPtr> contexts;
    {
        TGuard<TSpinLock> guard(RequestMapLock_);
        auto it = ReplyBusToContexts_.find(bus);
        if (it == ReplyBusToContexts_.end())
            return;

        for (auto* rawContext : it->second) {
            auto context = DangerousGetPtr(rawContext);
            if (context) {
                contexts.push_back(context);
            }
        }

        ReplyBusToContexts_.erase(it);
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

static thread_local bool ScheduleRequestsLatch;

void TServiceBase::ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    // Prevent reentrant invocations.
    if (ScheduleRequestsLatch) {
        return;
    }
    ScheduleRequestsLatch = true;
    auto latchGuard = Finally([&] {
        ScheduleRequestsLatch = false;
    });

    while (TryAcquireRequestSemaphore(runtimeInfo)) {
        TServiceContextPtr context;
        if (!runtimeInfo->RequestQueue.Dequeue(&context)) {
            ReleaseRequestSemaphore(runtimeInfo);
            if (!runtimeInfo->RequestQueue.IsEmpty()) {
                continue;
            }
            break;
        }
        RunRequest(std::move(context));
    }
}

void TServiceBase::RunRequest(const TServiceContextPtr& context)
{
    const auto& runtimeInfo = context->GetRuntimeInfo();
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.Heavy) {
        runtimeInfo->Descriptor.HeavyHandler
            .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
            .Run(context, options)
            .Subscribe(BIND(&TServiceContext::CheckAndRun, context));
    } else {
        context->Run(runtimeInfo->Descriptor.LiteHandler);
    }
}

void TServiceBase::RegisterRequest(TServiceContext* context)
{
    auto requestId = context->GetRequestId();
    const auto& replyBus = context->GetReplyBus();

    bool subscribe = false;
    {
        TGuard<TSpinLock> guard(RequestMapLock_);
        // NB: We're OK with duplicate request ids.
        RequestIdToContext_.insert(std::make_pair(requestId, context));
        auto it = ReplyBusToContexts_.find(context->GetReplyBus());
        if (it == ReplyBusToContexts_.end()) {
            subscribe = true;
            it = ReplyBusToContexts_.insert(std::make_pair(
                context->GetReplyBus(),
                THashSet<TServiceContext*>())).first;
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
    const auto& replyBus = context->GetReplyBus();

    {
        TGuard<TSpinLock> guard(RequestMapLock_);
        // NB: We're OK with duplicate request ids.
        RequestIdToContext_.erase(requestId);
        auto it = ReplyBusToContexts_.find(replyBus);
        // Missing replyBus in ReplyBusToContexts_ is OK; see OnReplyBusTerminated.
        if (it != ReplyBusToContexts_.end()) {
            auto& contexts = it->second;
            contexts.erase(context);
        }
    }
}

TServiceBase::TServiceContextPtr TServiceBase::FindRequest(TRequestId requestId)
{
    TGuard<TSpinLock> guard(RequestMapLock_);
    return DoFindRequest(requestId);
}

TServiceBase::TServiceContextPtr TServiceBase::DoFindRequest(TRequestId requestId)
{
    auto it = RequestIdToContext_.find(requestId);
    return it == RequestIdToContext_.end() ? nullptr : DangerousGetPtr(it->second);
}

TServiceBase::TPendingPayloadsEntry* TServiceBase::DoGetOrCreatePendingPayloadsEntry(TRequestId requestId)
{
    auto& entry = RequestIdToPendingPayloads_[requestId];
    if (!entry.Lease) {
        entry.Lease = NConcurrency::TLeaseManager::CreateLease(
            PendingPayloadsTimeout_,
            BIND(&TServiceBase::OnPendingPayloadsLeaseExpired, MakeWeak(this), requestId));
    }

    return &entry;
}

std::vector<TStreamingPayload> TServiceBase::GetAndErasePendingPayloads(TRequestId requestId)
{
    TPendingPayloadsEntry entry;
    {
        TGuard<TSpinLock> guard(RequestMapLock_);
        auto it = RequestIdToPendingPayloads_.find(requestId);
        if (it == RequestIdToPendingPayloads_.end()) {
            return {};
        }
        entry = std::move(it->second);
        RequestIdToPendingPayloads_.erase(it);
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
    const TString& userName)
{
    struct TCacheTrait
    {
        typedef TTagIdList TKey;
        typedef TServiceBase::TMethodPerformanceCountersPtr TValue;

        static TKey ToKey(const TTagIdList& tags)
        {
            return tags;
        }

        static TValue ToValue(const TTagIdList& tags)
        {
            return New<TMethodPerformanceCounters>(tags);
        }
    };

    auto tagIds = runtimeInfo->TagIds;
    tagIds.push_back(NProfiling::TProfileManager::Get()->RegisterTag("user", userName));
    return GetGloballyCachedValue<TCacheTrait>(tagIds);
}

TServiceBase::TMethodPerformanceCounters* TServiceBase::GetMethodPerformanceCounters(
    const TRuntimeMethodInfoPtr& runtimeInfo,
    const TString& user)
{
    // Fast path.
    if (user == RootUserName) {
        return runtimeInfo->RootPerformanceCounters.Get();
    }

    // Slow path.
    {
        TReaderGuard guard(runtimeInfo->PerformanceCountersLock);
        auto it = runtimeInfo->UserToPerformanceCounters.find(user);
        if (it != runtimeInfo->UserToPerformanceCounters.end()) {
            return it->second.Get();
        }
    }

    auto counters = CreateMethodPerformanceCounters(runtimeInfo, user);
    {
        TWriterGuard guard(runtimeInfo->PerformanceCountersLock);
        auto it = runtimeInfo->UserToPerformanceCounters.find(user);
        if (it == runtimeInfo->UserToPerformanceCounters.end()) {
            it = runtimeInfo->UserToPerformanceCounters.insert(std::make_pair(user, counters)).first;
        }
        return it->second.Get();
    }
}

void TServiceBase::OnProfiling()
{
    Profiler.Update(AuthenticationQueueSizeCounter_, AuthenticationQueueSize_.load());

    {
        TReaderGuard guard(MethodMapLock_);
        for (const auto& [methodName, runtimeInfo] : MethodMap_) {
            Profiler.Update(runtimeInfo->QueueSizeCounter, runtimeInfo->QueueSize.load(std::memory_order_relaxed));
            Profiler.Update(runtimeInfo->QueueSizeLimitCounter, runtimeInfo->Descriptor.QueueSizeLimit);
            Profiler.Update(runtimeInfo->ConcurrencyCounter, runtimeInfo->ConcurrencySemaphore.load(std::memory_order_relaxed));
            Profiler.Update(runtimeInfo->ConcurrencyLimitCounter, runtimeInfo->Descriptor.ConcurrencyLimit);
        }
    }
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    auto* profileManager = NProfiling::TProfileManager::Get();
    NProfiling::TTagIdList tagIds{
        ServiceTagId_,
        profileManager->RegisterTag("method", descriptor.Method)
    };

    auto runtimeInfo = New<TRuntimeMethodInfo>(descriptor, tagIds);
    runtimeInfo->RootPerformanceCounters = CreateMethodPerformanceCounters(runtimeInfo, "root");

    {
        TWriterGuard guard(MethodMapLock_);
        // Failure here means that such method is already registered.
        YT_VERIFY(MethodMap_.insert(std::make_pair(descriptor.Method, runtimeInfo)).second);
        return runtimeInfo;
    }
}

void TServiceBase::Configure(INodePtr configNode)
{
    try {
        auto config = ConvertTo<TServiceConfigPtr>(configNode);

        AuthenticationQueueSizeLimit_ = config->AuthenticationQueueSizeLimit;
        PendingPayloadsTimeout_ = config->PendingPayloadsTimeout;

        for (const auto& pair : config->Methods) {
            const auto& methodName = pair.first;
            const auto& methodConfig = pair.second;
            auto runtimeInfo = FindMethodInfo(methodName);
            if (!runtimeInfo) {
                THROW_ERROR_EXCEPTION("Cannot find RPC method %v.%v to configure",
                    ServiceId_.ServiceName,
                    methodName);
            }

            auto& descriptor = runtimeInfo->Descriptor;
            descriptor.SetHeavy(methodConfig->Heavy);
            descriptor.SetQueueSizeLimit(methodConfig->QueueSizeLimit);
            descriptor.SetConcurrencyLimit(methodConfig->ConcurrencyLimit);
            descriptor.SetLogLevel(methodConfig->LogLevel);
            descriptor.SetLoggingSuppressionTimeout(methodConfig->LoggingSuppressionTimeout);

            runtimeInfo->LoggingSuppressionFailedRequestThrottler->Reconfigure(
                methodConfig->LoggingSuppressionFailedRequestThrottler);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error configuring RPC service %v",
            ServiceId_.ServiceName)
            << ex;
    }
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
    TReaderGuard guard(MethodMapLock_);

    auto it = MethodMap_.find(method);
    return it == MethodMap_.end() ? nullptr : it->second;
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::GetMethodInfo(const TString& method)
{
    auto runtimeInfo = FindMethodInfo(method);
    YT_VERIFY(runtimeInfo);
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

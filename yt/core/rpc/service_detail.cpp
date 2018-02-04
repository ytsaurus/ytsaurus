#include "service_detail.h"
#include "private.h"
#include "config.h"
#include "dispatcher.h"
#include "helpers.h"
#include "message.h"
#include "response_keeper.h"
#include "server_detail.h"

#include <yt/core/bus/bus.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/net/local_address.h>

#include <yt/core/misc/string.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;
using namespace NRpc::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = RpcServerProfiler;

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
{ }

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    const TMethodDescriptor& descriptor,
    const NProfiling::TTagIdList& tagIds)
    : Descriptor(descriptor)
    , TagIds(tagIds)
    , QueueSizeCounter("/request_queue_size", tagIds)
{ }

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        TServiceBasePtr service,
        const TRequestId& requestId,
        NBus::IBusPtr replyBus,
        TRuntimeMethodInfoPtr runtimeInfo,
        const NTracing::TTraceContext& traceContext,
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray requestMessage,
        const NLogging::TLogger& logger,
        NLogging::ELogLevel logLevel)
        : TServiceContextBase(
            std::move(header),
            std::move(requestMessage),
            logger,
            logLevel)
        , Service_(std::move(service))
        , RequestId_(requestId)
        , ReplyBus_(std::move(replyBus))
        , RuntimeInfo_(std::move(runtimeInfo))
        , PerformanceCounters_(Service_->LookupMethodPerformanceCounters(RuntimeInfo_, User_))
        , TraceContext_(traceContext)
        , ArrivalInstant_(GetCpuInstant())
    {
        Y_ASSERT(RequestMessage_);
        Y_ASSERT(ReplyBus_);
        Y_ASSERT(Service_);
        Y_ASSERT(RuntimeInfo_);

        Initialize();
    }

    ~TServiceContext()
    {
        if (!Replied_ && !Canceled_.IsFired()) {
            Reply(TError(NRpc::EErrorCode::Unavailable, "Service is unable to complete your request"));
        }

        Finalize();
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

    void Run(const TErrorOr<TLiteHandler>& handlerOrError)
    {
        if (!handlerOrError.IsOK()) {
            Reply(TError(handlerOrError));
            return;
        }

        const auto& handler = handlerOrError.Value();
        if (!handler) {
            return;
        }

        auto wrappedHandler = BIND(&TServiceContext::DoRun, MakeStrong(this), handler);

        const auto& descriptor = RuntimeInfo_->Descriptor;
        const auto& invoker = descriptor.Invoker ? descriptor.Invoker : Service_->DefaultInvoker_;
        invoker->Invoke(std::move(wrappedHandler));
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
        if (Canceled_.Fire()) {
            LOG_DEBUG("Request canceled (RequestId: %v)",
                RequestId_);
            Profiler.Increment(PerformanceCounters_->CanceledRequestCounter);
        }
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

        LOG_DEBUG("Request timed out, canceling (RequestId: %v)",
            RequestId_);
        Profiler.Increment(PerformanceCounters_->TimedOutRequestCounter);
        Canceled_.Fire();

        // Guards from race with DoGuardedRun.
        // We can only mark as complete those requests that will not be run
        // as there's no guarantee that, if started,  the method handler will respond promptly to cancelation.
        if (!RunLatch_.test_and_set()) {
            SetComplete();
        }
    }

private:
    const TServiceBasePtr Service_;
    const TRequestId RequestId_;
    const IBusPtr ReplyBus_;
    const TRuntimeMethodInfoPtr RuntimeInfo_;
    TMethodPerformanceCounters* const PerformanceCounters_;
    const NTracing::TTraceContext TraceContext_;

    TDelayedExecutorCookie TimeoutCookie_;

    TSingleShotCallbackList<void()> Canceled_;

    const NProfiling::TCpuInstant ArrivalInstant_;
    NProfiling::TCpuInstant StartInstant_ = 0;
    NProfiling::TCpuInstant ReplyInstant_ = 0;

    TDuration ExecutionTime_;
    TDuration TotalTime_;
    TDuration LocalWaitTime_;

    std::atomic_flag CompletedLatch_ = ATOMIC_FLAG_INIT;
    std::atomic_flag TimedOutLatch_ = ATOMIC_FLAG_INIT;
    std::atomic_flag RunLatch_ = ATOMIC_FLAG_INIT;
    bool FinalizeLatch_ = false;

    void Initialize()
    {
        Profiler.Increment(PerformanceCounters_->RequestCounter);

        if (RequestHeader_->has_start_time()) {
            // Decode timing information.
            auto retryStart = FromProto<TInstant>(RequestHeader_->start_time());
            auto now = NProfiling::GetInstant();

            // Make sanity adjustments to account for possible clock skew.
            retryStart = std::min(retryStart, now);

            Profiler.Update(PerformanceCounters_->RemoteWaitTimeCounter, DurationToValue(now - retryStart));
        }

        if (RuntimeInfo_->Descriptor.Cancelable) {
            Service_->RegisterCancelableRequest(this);

            auto timeout = GetTimeout();
            if (timeout) {
                TimeoutCookie_ = TDelayedExecutor::Submit(
                    BIND(&TServiceBase::OnRequestTimeout, Service_, RequestId_),
                    *timeout);
            }
        }

        Profiler.Increment(RuntimeInfo_->QueueSizeCounter, +1);
        ++Service_->ActiveRequestCount_;
    }

    void Finalize()
    {
        // Finalize is called from DoReply and ~TServiceContext.
        // Clearly there could be no race between these two and thus no atomics are needed.
        if (FinalizeLatch_) {
            return;
        }
        FinalizeLatch_ = true;

        if (RuntimeInfo_->Descriptor.Cancelable) {
            Service_->UnregisterCancelableRequest(this);
        }

        DoSetComplete();
    }


    void DoRun(const TLiteHandler& handler)
    {
        DoBeforeRun();

        try {
            NTracing::TTraceContextGuard guard(TraceContext_);
            DoGuardedRun(handler);
        } catch (const std::exception& ex) {
            Reply(ex);
        } catch (const TFiberCanceledException&) {
            // Request canceled; cleanup and rethrow.
            DoAfterRun();
            throw;
        }

        DoAfterRun();
    }

    void DoBeforeRun()
    {
        StartInstant_ = GetCpuInstant();
        LocalWaitTime_ = CpuDurationToDuration(StartInstant_ - ArrivalInstant_);
        Profiler.Update(PerformanceCounters_->LocalWaitTimeCounter, DurationToValue(LocalWaitTime_));
    }

    void DoGuardedRun(const TLiteHandler& handler)
    {
        const auto& descriptor = RuntimeInfo_->Descriptor;

        if (!descriptor.System) {
            Service_->BeforeInvoke(this);
        }

        auto timeout = GetTimeout();
        if (timeout && NProfiling::GetCpuInstant() > ArrivalInstant_ + NProfiling::DurationToCpuDuration(*timeout)) {
            if (!TimedOutLatch_.test_and_set()) {
                LOG_DEBUG("Request dropped due to timeout before being run (RequestId: %v)",
                    RequestId_);
                Profiler.Increment(PerformanceCounters_->TimedOutRequestCounter);
            }
            return;
        }

        if (descriptor.Cancelable) {
            auto canceler = GetCurrentFiberCanceler();
            if (canceler && !Canceled_.TrySubscribe(std::move(canceler))) {
                LOG_DEBUG("Request was canceled before being run (RequestId: %v)",
                    RequestId_);
                return;
            }
        }

        // Guards from race with HandleTimout.
        if (RunLatch_.test_and_set()) {
            return;
        }

        handler.Run(this, descriptor.Options);
    }

    void DoAfterRun()
    {
        TDelayedExecutor::CancelAndClear(TimeoutCookie_);
    }

    virtual void DoReply() override
    {
        TRACE_ANNOTATION(
            TraceContext_,
            Service_->ServiceId_.ServiceName,
            RuntimeInfo_->Descriptor.Method,
            NTracing::ServerSendAnnotation);

        auto responseMessage = GetResponseMessage();

        NBus::TSendOptions busOptions;
        busOptions.TrackingLevel = EDeliveryTrackingLevel::None;
        busOptions.ChecksummedPartCount = RuntimeInfo_->Descriptor.GenerateAttachmentChecksums
            ? NBus::TSendOptions::AllParts
            : 2; // RPC header + response body
        ReplyBus_->Send(std::move(responseMessage), busOptions);

        ReplyInstant_ = GetCpuInstant();
        ExecutionTime_ = StartInstant_ != 0
            ? CpuDurationToDuration(ReplyInstant_ - StartInstant_)
            : TDuration();
        TotalTime_ = CpuDurationToDuration(ReplyInstant_ - ArrivalInstant_);

        Profiler.Update(PerformanceCounters_->ExecutionTimeCounter, DurationToValue(ExecutionTime_));
        Profiler.Update(PerformanceCounters_->TotalTimeCounter, DurationToValue(TotalTime_));
        if (!Error_.IsOK()) {
            Profiler.Increment(PerformanceCounters_->FailedRequestCounter);
        }

        Finalize();
    }

    void DoSetComplete()
    {
        // DoSetComplete could be called from anywhere so it is racy.
        if (CompletedLatch_.test_and_set()) {
            return;
        }

        Profiler.Increment(RuntimeInfo_->QueueSizeCounter, -1);
        if (--Service_->ActiveRequestCount_ == 0 && Service_->Stopped_.load()) {
            Service_->StopResult_.TrySet();
        }

        TServiceBase::ReleaseRequestSemaphore(RuntimeInfo_);
        TServiceBase::ScheduleRequests(RuntimeInfo_);
    }


    virtual void LogRequest() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v:%v <- ",
            GetService(),
            GetMethod());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        if (RequestId_) {
            delimitedBuilder->AppendFormat("RequestId: %v", GetRequestId());
        }

        if (RealmId_) {
            delimitedBuilder->AppendFormat("RealmId: %v", GetRealmId());
        }

        if (User_ != RootUserName) {
            delimitedBuilder->AppendFormat("User: %v", User_);
        }

        auto mutationId = GetMutationId();
        if (mutationId) {
            delimitedBuilder->AppendFormat("MutationId: %v", mutationId);
        }

        delimitedBuilder->AppendFormat("Retry: %v", IsRetry());

        if (RequestHeader_->has_timeout()) {
            delimitedBuilder->AppendFormat("Timeout: %v", FromProto<TDuration>(RequestHeader_->timeout()));
        }

        delimitedBuilder->AppendFormat("BodySize: %v, AttachmentsSize: %v/%v",
            GetMessageBodySize(RequestMessage_),
            GetTotalMesageAttachmentSize(RequestMessage_),
            GetMessageAttachmentCount(RequestMessage_));

        if (RequestInfo_) {
            delimitedBuilder->AppendFormat("%v", RequestInfo_);
        }

        LOG_EVENT(Logger, LogLevel_, builder.Flush());
    }

    virtual void LogResponse() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v:%v -> ",
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
            GetTotalMesageAttachmentSize(responseMessage),
            GetMessageAttachmentCount(responseMessage));

        if (ResponseInfo_) {
            delimitedBuilder->AppendString(ResponseInfo_);
        }

        delimitedBuilder->AppendFormat("ExecutionTime: %v, TotalTime: %v",
            ExecutionTime_,
            TotalTime_);

        LOG_EVENT(Logger, LogLevel_, builder.Flush());
    }
};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    const TRealmId& realmId)
    : Logger(logger)
    , DefaultInvoker_(std::move(defaultInvoker))
    , ServiceId_(descriptor.GetFullServiceName(), realmId)
    , ProtocolVersion_(descriptor.ProtocolVersion)
{
    YCHECK(DefaultInvoker_);

    ServiceTagId_ = NProfiling::TProfileManager::Get()->RegisterTag("service", ServiceId_.ServiceName);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(TDispatcher::Get()->GetLightInvoker())
        .SetSystem(true));
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
    auto requestProtocolVersion = header->protocol_version();

    TRuntimeMethodInfoPtr runtimeInfo;
    auto handleError = [&] (auto&&... args) {
        auto error = TError(std::forward<decltype(args)>(args)...)
            << TErrorAttribute("request_id", requestId)
            << TErrorAttribute("service", ServiceId_.ServiceName)
            << TErrorAttribute("method", method)
            << TErrorAttribute("endpoint", replyBus->GetEndpointDescription());

        auto logLevel = error.GetCode() == EErrorCode::Unavailable
            ? NLogging::ELogLevel::Debug
            : NLogging::ELogLevel::Warning;
        LOG_EVENT(Logger, logLevel, error);

        auto errorMessage = CreateErrorResponseMessage(requestId, error);
        replyBus->Send(errorMessage, NBus::TSendOptions(EDeliveryTrackingLevel::None));
    };

    if (Stopped_) {
        handleError(
            EErrorCode::Unavailable,
            "Service is stopped");
        return;
    }

    if (requestProtocolVersion != GenericProtocolVersion &&
        requestProtocolVersion != ProtocolVersion_)
    {
        handleError(
            EErrorCode::ProtocolError,
            "Protocol version mismatch: expected %v, received %v",
            ProtocolVersion_,
            requestProtocolVersion);
        return;
    }

    runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        handleError(
            EErrorCode::NoSuchMethod,
            "Unknown method");
        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    if (runtimeInfo->QueueSizeCounter.GetCurrent() > runtimeInfo->Descriptor.MaxQueueSize) {
        handleError(
            TError(
                NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
                "Request queue size limit exceeded")
            << TErrorAttribute("limit", runtimeInfo->Descriptor.MaxQueueSize));
        return;
    }

    auto traceContext = GetTraceContext(*header);
    NTracing::TTraceContextGuard traceContextGuard(traceContext);

    auto context = New<TServiceContext>(
        this,
        requestId,
        std::move(replyBus),
        runtimeInfo,
        traceContext,
        std::move(header),
        std::move(message),
        Logger,
        runtimeInfo->Descriptor.LogLevel);

    TRACE_ANNOTATION(
        traceContext,
        "server_host",
        NNet::GetLocalHostName());

    TRACE_ANNOTATION(
        traceContext,
        ServiceId_.ServiceName,
        method,
        NTracing::ServerReceiveAnnotation);

    runtimeInfo->RequestQueue.Enqueue(std::move(context));
    ScheduleRequests(runtimeInfo);
}

void TServiceBase::HandleRequestCancelation(const TRequestId& requestId)
{
    auto context = FindCancelableRequest(requestId);
    if (!context) {
        LOG_DEBUG("Received cancelation for an unknown request, ignored (RequestId: %v)",
            requestId);
        return;
    }

    context->Cancel();
}

void TServiceBase::OnRequestTimeout(const TRequestId& requestId, bool /*aborted*/)
{
    auto context = FindCancelableRequest(requestId);
    if (!context) {
        return;
    }

    context->HandleTimeout();
}

void TServiceBase::OnReplyBusTerminated(IBusPtr bus, const TError& error)
{
    std::vector<TServiceContextPtr> contexts;
    {
        TGuard<TSpinLock> guard(CancelableRequestLock_);
        auto it = ReplyBusToContexts_.find(bus);
        if (it == ReplyBusToContexts_.end())
            return;

        for (auto* rawContext : it->second) {
            auto context = TServiceContext::DangerousGetPtr(rawContext);
            if (context) {
                contexts.push_back(context);
            }
        }

        ReplyBusToContexts_.erase(it);
    }

    for (auto context : contexts) {
        LOG_DEBUG(error, "Reply bus terminated, canceling request (RequestId: %v)",
            context->GetRequestId());
        context->Cancel();
    }
}

bool TServiceBase::TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    auto& semaphore = runtimeInfo->ConcurrencySemaphore;
    auto limit = runtimeInfo->Descriptor.MaxConcurrency;
    while (true) {
        auto current = semaphore.load();
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

static PER_THREAD bool ScheduleRequestsLatch = false;

void TServiceBase::ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    // Prevent reentrant invocations.
    if (ScheduleRequestsLatch) {
        return;
    }
    ScheduleRequestsLatch = true;

    while (true) {
        if (runtimeInfo->RequestQueue.IsEmpty()) {
            break;
        }

        if (!TryAcquireRequestSemaphore(runtimeInfo)) {
            break;
        }

        TServiceContextPtr context;
        if (runtimeInfo->RequestQueue.Dequeue(&context)) {
            RunRequest(std::move(context));
            break;
        }

        ReleaseRequestSemaphore(runtimeInfo);
    }

    ScheduleRequestsLatch = false;
}

void TServiceBase::RunRequest(const TServiceContextPtr& context)
{
    const auto& runtimeInfo = context->GetRuntimeInfo();
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.Heavy) {
        runtimeInfo->Descriptor.HeavyHandler
            .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
            .Run(context, options)
            .Subscribe(BIND(&TServiceContext::Run, context));
    } else {
        context->Run(runtimeInfo->Descriptor.LiteHandler);
    }
}

void TServiceBase::RegisterCancelableRequest(TServiceContext* context)
{
    const auto& requestId = context->GetRequestId();
    const auto& replyBus = context->GetReplyBus();

    bool subscribe = false;
    {
        TGuard<TSpinLock> guard(CancelableRequestLock_);
        // NB: We're OK with duplicate request ids.
        IdToContext_.insert(std::make_pair(requestId, context));
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
}

void TServiceBase::UnregisterCancelableRequest(TServiceContext* context)
{
    const auto& requestId = context->GetRequestId();
    const auto& replyBus = context->GetReplyBus();

    {
        TGuard<TSpinLock> guard(CancelableRequestLock_);
        // NB: We're OK with duplicate request ids.
        IdToContext_.erase(requestId);
        auto it = ReplyBusToContexts_.find(replyBus);
        // Missing replyBus in ReplyBusToContexts_ is OK; see OnReplyBusTerminated.
        if (it != ReplyBusToContexts_.end()) {
            auto& contexts = it->second;
            contexts.erase(context);
        }
    }
}

TServiceBase::TServiceContextPtr TServiceBase::FindCancelableRequest(const TRequestId& requestId)
{
    TGuard<TSpinLock> guard(CancelableRequestLock_);
    auto it = IdToContext_.find(requestId);
    return it == IdToContext_.end() ? nullptr : TServiceContext::DangerousGetPtr(it->second);
}

TServiceBase::TMethodPerformanceCountersPtr TServiceBase::CreateMethodPerformanceCounters(
    const TRuntimeMethodInfoPtr& runtimeInfo,
    const TString& userName)
{
    auto tagIds = runtimeInfo->TagIds;
    tagIds.push_back(NProfiling::TProfileManager::Get()->RegisterTag("user", userName));
    return New<TMethodPerformanceCounters>(tagIds);
}

TServiceBase::TMethodPerformanceCounters* TServiceBase::LookupMethodPerformanceCounters(
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
        YCHECK(MethodMap_.insert(std::make_pair(descriptor.Method, runtimeInfo)).second);
        return runtimeInfo;
    }
}

void TServiceBase::Configure(INodePtr configNode)
{
    try {
        auto config = ConvertTo<TServiceConfigPtr>(configNode);
        for (const auto& pair : config->Methods) {
            const auto& methodName = pair.first;
            const auto& methodConfig = pair.second;
            auto runtimeInfo = FindMethodInfo(methodName);
            if (!runtimeInfo) {
                THROW_ERROR_EXCEPTION("Cannot find RPC method %v:%v to configure",
                    ServiceId_.ServiceName,
                    methodName);
            }

            auto& descriptor = runtimeInfo->Descriptor;
            if (methodConfig->Heavy) {
                descriptor.SetHeavy(*methodConfig->Heavy);
            }
            if (methodConfig->ResponseCodec) {
                descriptor.SetResponseCodec(*methodConfig->ResponseCodec);
            }
            if (methodConfig->MaxQueueSize) {
                descriptor.SetMaxQueueSize(*methodConfig->MaxQueueSize);
            }
            if (methodConfig->MaxConcurrency) {
                descriptor.SetMaxConcurrency(*methodConfig->MaxConcurrency);
            }
            if (methodConfig->LogLevel) {
                descriptor.SetLogLevel(*methodConfig->LogLevel);
            }
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
    YCHECK(runtimeInfo);
    return runtimeInfo;
}

IInvokerPtr TServiceBase::GetDefaultInvoker()
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

} // namespace NRpc
} // namespace NYT

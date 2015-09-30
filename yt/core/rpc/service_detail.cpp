#include "stdafx.h"
#include "service_detail.h"
#include "private.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "message.h"
#include "config.h"
#include "helpers.h"
#include "response_keeper.h"

#include <core/misc/string.h>
#include <core/misc/address.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <core/bus/bus.h>

#include <core/profiling/timing.h>
#include <core/profiling/profile_manager.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NProfiling;
using namespace NRpc::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = RpcServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    const Stroka& method,
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Method(method)
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
{ }

TServiceBase::TMethodPerformanceCounters::TMethodPerformanceCounters(const NProfiling::TTagIdList& tagIds)
    : RequestCounter("/request_count", tagIds)
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
        , ArrivalTime_(GetCpuInstant())
    {
        YASSERT(RequestMessage_);
        YASSERT(ReplyBus_);
        YASSERT(Service_);
        YASSERT(RuntimeInfo_);

        Initialize();
    }

    ~TServiceContext()
    {
        if (!RuntimeInfo_->Descriptor.OneWay && !Replied_ && !Canceled_.IsFired()) {
            if (Started_) {
                Reply(TError(NYT::EErrorCode::Canceled, "Request abandoned"));
            } else {
                Reply(TError(NRpc::EErrorCode::Unavailable, "Service is currently unavailable"));
            }
        }

        Finalize();
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
        if (!handler)
            return;

        auto wrappedHandler = BIND(&TServiceContext::DoRun, MakeStrong(this), handler);

        const auto& descriptor = RuntimeInfo_->Descriptor;
        auto invoker = descriptor.Invoker ? descriptor.Invoker : Service_->DefaultInvoker_;
        if (descriptor.EnableReorder) {
            invoker->Invoke(std::move(wrappedHandler), GetPriority());
        } else {
            invoker->Invoke(std::move(wrappedHandler));
        }
    }

    virtual void SubscribeCanceled(const TClosure& callback) override
    {
        Canceled_.Subscribe(callback);
    }

    virtual void UnsubscribeCanceled(const TClosure& callback) override
    {
        Canceled_.Unsubscribe(callback);
    }

    virtual void Cancel() override
    {
        Canceled_.Fire();
    }

private:
    const TServiceBasePtr Service_;
    const TRequestId RequestId_;
    const TMutationId MutationId_;
    const IBusPtr ReplyBus_;
    const TRuntimeMethodInfoPtr RuntimeInfo_;
    TMethodPerformanceCounters* const PerformanceCounters_;
    const NTracing::TTraceContext TraceContext_;

    TDelayedExecutorCookie TimeoutCookie_;

    TSpinLock SpinLock_;
    bool Started_ = false;
    bool RunningSync_ = false;
    bool Completed_ = false;
    TSingleShotCallbackList<void()> Canceled_;
    bool Finalized_ = false;
    NProfiling::TCpuInstant ArrivalTime_;
    NProfiling::TCpuInstant StartTime_ = -1;


    void Initialize()
    {
        Profiler.Increment(PerformanceCounters_->RequestCounter, +1);

        if (RequestHeader_->has_request_start_time() && RequestHeader_->has_retry_start_time()) {
            // Decode timing information.
            auto requestStart = TInstant(RequestHeader_->request_start_time());
            auto retryStart = TInstant(RequestHeader_->retry_start_time());
            auto now = CpuInstantToInstant(GetCpuInstant());

            // Make sanity adjustments to account for possible clock skew.
            retryStart = std::min(retryStart, now);
            requestStart = std::min(requestStart, retryStart);

            Profiler.Update(PerformanceCounters_->RemoteWaitTimeCounter, (now - requestStart).MicroSeconds());
        }

        if (!RuntimeInfo_->Descriptor.OneWay) {
            if (RuntimeInfo_->Descriptor.Cancelable) {
                Service_->RegisterCancelableRequest(this);
            }

            Profiler.Increment(RuntimeInfo_->QueueSizeCounter, +1);
        }
    }

    void Finalize()
    {
        if (RuntimeInfo_->Descriptor.OneWay || Finalized_)
            return;

        if (RuntimeInfo_->Descriptor.Cancelable) {
            Service_->UnregisterCancelableRequest(this);
        }

        // NB: This counter is also used to track queue size limit so
        // it must be maintained even if the profiler is OFF.
        Profiler.Increment(RuntimeInfo_->QueueSizeCounter, -1);

        TServiceBase::ReleaseRequestSemaphore(RuntimeInfo_);
        TServiceBase::ScheduleRequests(RuntimeInfo_);

        Finalized_ = true;
    }


    void DoRun(const TLiteHandler& handler)
    {
        DoBeforeRun();

        try {
            NTracing::TTraceContextGuard guard(TraceContext_);
            DoGuardedRun(handler);
        } catch (const std::exception& ex) {
            if (!RuntimeInfo_->Descriptor.OneWay) {
                Reply(ex);
            }
        } catch (const TFiberCanceledException&) {
            // Request canceled; cleanup and rethrow.
            DoAfterRun();
            throw;
        }

        DoAfterRun();
    }

    void DoBeforeRun()
    {
        // No need for a lock here.
        RunningSync_ = true;
        Started_ = true;
        StartTime_ = GetCpuInstant();

        if (Profiler.GetEnabled()) {
            auto value = CpuDurationToValue(StartTime_ - ArrivalTime_);
            Profiler.Update(PerformanceCounters_->LocalWaitTimeCounter, value);
        }
    }

    void DoGuardedRun(const TLiteHandler& handler)
    {
        const auto& descriptor = RuntimeInfo_->Descriptor;

        if (!descriptor.System) {
            Service_->BeforeInvoke();
        }

        auto timeout = GetLocalTimeout();
        if (timeout == TDuration::Zero()) {
            LOG_DEBUG("Request dropped due to timeout before being run (RequestId: %v)",
                RequestId_);
            return;
        }

        if (descriptor.Cancelable) {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Canceled_.IsFired()) {
                LOG_DEBUG("Request was canceled before being run (RequestId: %v)",
                    RequestId_);
                return;
            }

            Canceled_.Subscribe(GetCurrentFiberCanceler());

            if (timeout != TDuration::Max()) {
                LOG_TRACE("Setting up server-side request timeout (RequestId: %v, Timeout: %v)",
                    RequestId_,
                    timeout);
                TimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(&TServiceBase::OnRequestTimeout, Service_, RequestId_),
                    timeout);
            }
        }

        handler.Run(this, descriptor.Options);
    }

    void DoAfterRun()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        YASSERT(RunningSync_);
        RunningSync_ = false;

        if (Profiler.GetEnabled() && RuntimeInfo_->Descriptor.OneWay) {
            auto value = CpuDurationToValue(GetCpuInstant() - ArrivalTime_);
            Profiler.Update(PerformanceCounters_->TotalTimeCounter, value);
        }
    }


    //! Returns TDuration::Zero() if the request has already timed out.
    //! Returns TDuration::Max() if the request has no associated timeout.
    TDuration GetLocalTimeout() const
    {
        auto timeout = GetTimeout();
        if (!timeout) {
            return TDuration::Max();
        }

        auto deadlineTime = ArrivalTime_ + DurationToCpuDuration(*timeout);
        if (deadlineTime < StartTime_) {
            return TDuration::Zero();
        }

        return CpuDurationToDuration(deadlineTime - StartTime_);
    }


    virtual void DoReply() override
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);

            TRACE_ANNOTATION(
                TraceContext_,
                Service_->ServiceId_.ServiceName,
                RuntimeInfo_->Descriptor.Method,
                NTracing::ServerSendAnnotation);

            YASSERT(!Completed_);
            Completed_ = true;

            auto responseMessage = GetResponseMessage();

            ReplyBus_->Send(std::move(responseMessage), EDeliveryTrackingLevel::None);

            if (Profiler.GetEnabled()) {
                auto now = GetCpuInstant();

                {
                    i64 value = 0;
                    if (Started_) {
                        value = CpuDurationToValue(now - StartTime_);
                    }
                    Profiler.Update(PerformanceCounters_->ExecutionTimeCounter, value);
                }

                {
                    auto value = CpuDurationToValue(now - ArrivalTime_);
                    Profiler.Update(PerformanceCounters_->TotalTimeCounter, value);
                }
            }
        }

        Finalize();
    }


    virtual void LogRequest() override
    {
        TStringBuilder builder;

        if (RequestId_) {
            AppendInfo(&builder, "RequestId: %v", GetRequestId());
        }

        if (RealmId_) {
            AppendInfo(&builder, "RealmId: %v", GetRealmId());
        }

        if (User_ != RootUserName) {
            AppendInfo(&builder, "User: %v", User_);
        }

        auto mutationId = GetMutationId(*RequestHeader_);
        if (mutationId) {
            AppendInfo(&builder, "MutationId: %v", mutationId);
        }

        AppendInfo(&builder, "Retry: %v", IsRetry());

        if (RequestHeader_->has_timeout()) {
            AppendInfo(&builder, "Timeout: %v", TDuration(RequestHeader_->timeout()));
        }

        if (!RequestInfo_.empty()) {
            AppendInfo(&builder, "%v", RequestInfo_);
        }

        LOG_EVENT(Logger, LogLevel_, "%v <- %v",
            GetMethod(),
            builder.Flush());
    }

    virtual void LogResponse(const TError& error) override
    {
        TStringBuilder builder;

        if (RequestId_) {
            AppendInfo(&builder, "RequestId: %v", RequestId_);
        }

        AppendInfo(&builder, "Error: %v", error);

        if (!ResponseInfo_.empty()) {
            AppendInfo(&builder, "%v", ResponseInfo_);
        }

        if (Profiler.GetEnabled()) {
            AppendInfo(&builder, "ExecutionTime: %v, TotalTime: %v",
                ValueToDuration(PerformanceCounters_->ExecutionTimeCounter.Current),
                ValueToDuration(PerformanceCounters_->TotalTimeCounter.Current));
        }

        LOG_EVENT(Logger, LogLevel_, "%v -> %v",
            GetMethod(),
            builder.Flush());
    }

};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IPrioritizedInvokerPtr defaultInvoker,
    const TServiceId& serviceId,
    const NLogging::TLogger& logger,
    int protocolVersion)
{
    Initialize(
        defaultInvoker,
        serviceId,
        logger,
        protocolVersion);
}

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const TServiceId& serviceId,
    const NLogging::TLogger& logger,
    int protocolVersion)
{
    Initialize(
        CreateFakePrioritizedInvoker(defaultInvoker),
        serviceId,
        logger,
        protocolVersion);
}

void TServiceBase::Initialize(
    IPrioritizedInvokerPtr defaultInvoker,
    const TServiceId& serviceId,
    const NLogging::TLogger& logger,
    int protocolVersion)
{
    YCHECK(defaultInvoker);

    DefaultInvoker_ = defaultInvoker;
    ServiceId_ = serviceId;
    Logger = logger;
    ProtocolVersion_ = protocolVersion;

    ServiceTagId_ = NProfiling::TProfileManager::Get()->RegisterTag("service", ServiceId_.ServiceName);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(TDispatcher::Get()->GetInvoker())
        .SetSystem(true));
}

TServiceId TServiceBase::GetServiceId() const
{
    return ServiceId_;
}

void TServiceBase::HandleRequest(
    std::unique_ptr<NProto::TRequestHeader> header,
    TSharedRefArray message,
    IBusPtr replyBus)
{
    const auto& method = header->method();
    bool oneWay = header->one_way();
    auto requestId = FromProto<TRequestId>(header->request_id());
    auto requestProtocolVersion = header->protocol_version();

    TRuntimeMethodInfoPtr runtimeInfo;
    try {
        if (requestProtocolVersion != TProxyBase::GenericProtocolVersion &&
            requestProtocolVersion != ProtocolVersion_)
        {
            THROW_ERROR_EXCEPTION(
                EErrorCode::ProtocolError,
                "Protocol version mismatch for service %v: expected %v, received %v",
                ServiceId_.ServiceName,
                ProtocolVersion_,
                requestProtocolVersion);
        }

        runtimeInfo = FindMethodInfo(method);
        if (!runtimeInfo) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuchMethod,
                "Unknown method %v:%v",
                ServiceId_.ServiceName,
                method);
        }

        if (runtimeInfo->Descriptor.OneWay != oneWay) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::ProtocolError,
                "One-way flag mismatch for method %v:%v: expected %v, actual %v",
                ServiceId_.ServiceName,
                method,
                runtimeInfo->Descriptor.OneWay,
                oneWay);
        }

        // Not actually atomic but should work fine as long as some small error is OK.
        if (runtimeInfo->QueueSizeCounter.Current > runtimeInfo->Descriptor.MaxQueueSize) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::Unavailable,
                "Request queue limit %v reached for method %v:%v",
                runtimeInfo->Descriptor.MaxQueueSize,
                ServiceId_.ServiceName,
                runtimeInfo->Descriptor.Method);
        }
    } catch (const std::exception& ex) {
        auto error = TError(ex)
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!oneWay) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage, EDeliveryTrackingLevel::None);
        }
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
        TAddressResolver::Get()->GetLocalHostName());

    TRACE_ANNOTATION(
        traceContext,
        ServiceId_.ServiceName,
        method,
        NTracing::ServerReceiveAnnotation);

    if (oneWay) {
        RunRequest(std::move(context));
        return;
    }

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

void TServiceBase::OnRequestTimeout(const TRequestId& requestId)
{
    auto context = FindCancelableRequest(requestId);
    if (context) {
        LOG_DEBUG("Server-side timeout occurred, canceling request (RequestId: %v)",
            requestId);
        context->Cancel();
    }
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
        LOG_DEBUG(error, "Reply bus terminated, canceling request (RequestId: %v, ReplyBus: %p)",
            context->GetRequestId(),
            bus.Get());
        context->Cancel();
    }
}

bool TServiceBase::TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    auto& semaphore = runtimeInfo->RunningRequestSemaphore;
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
    --runtimeInfo->RunningRequestSemaphore;
}

static PER_THREAD bool ScheduleRequestsRunning = false;

void TServiceBase::ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo)
{
    // Prevent reeentarant invocations.
    if (ScheduleRequestsRunning)
        return;
    ScheduleRequestsRunning = true;

    while (true) {
        if (runtimeInfo->RequestQueue.IsEmpty())
            break;

        if (!TryAcquireRequestSemaphore(runtimeInfo))
            break;

        TServiceContextPtr context;
        if (runtimeInfo->RequestQueue.Dequeue(&context)) {
            RunRequest(std::move(context));
            break;
        }

        ReleaseRequestSemaphore(runtimeInfo);
    }

    ScheduleRequestsRunning = false;
}

void TServiceBase::RunRequest(const TServiceContextPtr& context)
{
    const auto& runtimeInfo = context->GetRuntimeInfo();
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.HeavyRequest) {
        runtimeInfo->Descriptor.HeavyHandler
            .AsyncVia(TDispatcher::Get()->GetInvoker())
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
    int requestsPerBus;
    {
        TGuard<TSpinLock> guard(CancelableRequestLock_);
        // NB: We're OK with duplicate request ids.
        IdToContext_.insert(std::make_pair(requestId, context));
        auto it = ReplyBusToContexts_.find(context->GetReplyBus());
        if (it == ReplyBusToContexts_.end()) {
            subscribe = true;
            it = ReplyBusToContexts_.insert(std::make_pair(
                context->GetReplyBus(),
                yhash_set<TServiceContext*>())).first;
        }
        auto& contexts = it->second;
        contexts.insert(context);
        requestsPerBus = contexts.size();
    }

    if (subscribe) {
        replyBus->SubscribeTerminated(BIND(&TServiceBase::OnReplyBusTerminated, MakeWeak(this), replyBus));
    }

    LOG_TRACE("Cancelable request registered (RequestId: %v, ReplyBus: %p, Subscribe: %v, RequestsPerBus: %v)",
        requestId,
        replyBus.Get(),
        subscribe,
        requestsPerBus);
}

void TServiceBase::UnregisterCancelableRequest(TServiceContext* context)
{
    const auto& requestId = context->GetRequestId();
    const auto& replyBus = context->GetReplyBus();

    int requestsPerBus;
    {
        TGuard<TSpinLock> guard(CancelableRequestLock_);
        // NB: We're OK with duplicate request ids.
        IdToContext_.erase(requestId);
        auto it = ReplyBusToContexts_.find(replyBus);
        if (it == ReplyBusToContexts_.end()) {
            // This is OK as well; see OnReplyBusTerminated.
            requestsPerBus = 0;
        } else {
            auto& contexts = it->second;
            contexts.erase(context);
            requestsPerBus = contexts.size();
        }
    }

    LOG_TRACE("Cancelable request unregistered (RequestId: %v, ReplyBus: %p, RequestsPerBus: %v)",
        requestId,
        replyBus.Get(),
        requestsPerBus);
}

TServiceBase::TServiceContextPtr TServiceBase::FindCancelableRequest(const TRequestId& requestId)
{
    TGuard<TSpinLock> guard(CancelableRequestLock_);
    auto it = IdToContext_.find(requestId);
    return it == IdToContext_.end() ? nullptr : TServiceContext::DangerousGetPtr(it->second);
}

TServiceBase::TMethodPerformanceCountersPtr TServiceBase::CreateMethodPerformanceCounters(
    const TRuntimeMethodInfoPtr& runtimeInfo,
    const Stroka& userName)
{
    auto tagIds = runtimeInfo->TagIds;
    tagIds.push_back(NProfiling::TProfileManager::Get()->RegisterTag("user", userName));
    return New<TMethodPerformanceCounters>(tagIds);
}

TServiceBase::TMethodPerformanceCounters* TServiceBase::LookupMethodPerformanceCounters(
    const TRuntimeMethodInfoPtr& runtimeInfo,
    const Stroka& user)
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
    NProfiling::TTagIdList tagIds;
    tagIds.push_back(ServiceTagId_);
    tagIds.push_back(NProfiling::TProfileManager::Get()->RegisterTag("method", descriptor.Method));

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
            if (methodConfig->RequestHeavy) {
                descriptor.SetRequestHeavy(*methodConfig->RequestHeavy);
            }
            if (methodConfig->ResponseHeavy) {
                descriptor.SetResponseHeavy(*methodConfig->ResponseHeavy);
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

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::FindMethodInfo(const Stroka& method)
{
    TReaderGuard guard(MethodMapLock_);

    auto it = MethodMap_.find(method);
    return it == MethodMap_.end() ? nullptr : it->second;
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::GetMethodInfo(const Stroka& method)
{
    auto runtimeInfo = FindMethodInfo(method);
    YCHECK(runtimeInfo);
    return runtimeInfo;
}

IPrioritizedInvokerPtr TServiceBase::GetDefaultInvoker()
{
    return DefaultInvoker_;
}

void TServiceBase::BeforeInvoke()
{ }

bool TServiceBase::IsUp(TCtxDiscoverPtr /*context*/)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

std::vector<Stroka> TServiceBase::SuggestAddresses()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return std::vector<Stroka>();
}

DEFINE_RPC_SERVICE_METHOD(TServiceBase, Discover)
{
    context->SetRequestInfo();

    response->set_up(IsUp(context));
    ToProto(response->mutable_suggested_addresses(), SuggestAddresses());

    context->SetResponseInfo("Up: %v, SuggestedAddresses: [%v]",
        response->up(),
        JoinToString(response->suggested_addresses()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

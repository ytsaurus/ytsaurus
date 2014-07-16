#include "stdafx.h"
#include "service_detail.h"
#include "private.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "message.h"
#include "config.h"
#include "helpers.h"

#include <core/misc/string.h>
#include <core/misc/address.h>

#include <core/concurrency/thread_affinity.h>

#include <core/bus/bus.h>

#include <core/profiling/timing.h>
#include <core/profiling/profiling_manager.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NProfiling;
using namespace NRpc::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcServerLogger;
static auto& Profiler = RpcServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    const Stroka& method,
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Method(method)
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
    , OneWay(false)
    , MaxQueueSize(100000)
    , EnableReorder(false)
    , System(false)
{ }

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    const TMethodDescriptor& descriptor,
    const NProfiling::TTagIdList& tagIds)
    : Descriptor(descriptor)
    , RequestCounter("/request_count", tagIds)
    , QueueSizeCounter("/request_queue_size", tagIds)
    , SyncTimeCounter("/request_time/sync", tagIds)
    , AsyncTimeCounter("/request_time/async", tagIds)
    , RemoteWaitTimeCounter("/request_time/remote_wait", tagIds)
    , LocalWaitTimeCounter("/request_time/local_wait", tagIds)
    , TotalTimeCounter("/request_time/total", tagIds)
{ }

TServiceBase::TActiveRequest::TActiveRequest(
    const TRequestId& id,
    IBusPtr replyBus,
    TRuntimeMethodInfoPtr runtimeInfo,
    const NTracing::TTraceContext& traceContext)
    : Id(id)
    , ReplyBus(std::move(replyBus))
    , RuntimeInfo(std::move(runtimeInfo))
    , RunningSync(false)
    , Completed(false)
    , ArrivalTime(GetCpuInstant())
    , SyncStartTime(-1)
    , SyncStopTime(-1)
    , TraceContext(traceContext)
{ }

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        TServiceBasePtr service,
        TActiveRequestPtr activeRequest,
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray requestMessage,
        IBusPtr replyBus,
        const Stroka& loggingCategory)
        : TServiceContextBase(
            std::move(header),
            std::move(requestMessage),
            NLog::TLogger(loggingCategory))
        , Service_(std::move(service))
        , ActiveRequest_(std::move(activeRequest))
        , ReplyBus_(std::move(replyBus))
    {
        YASSERT(RequestMessage_);
        YASSERT(ReplyBus_);
        YASSERT(Service_);
    }

    ~TServiceContext()
    {
        if (!IsOneWay() && !Replied_) {
            Reply(TError(NRpc::EErrorCode::Unavailable, "Request canceled"));
        }
    }

private:
    TServiceBasePtr Service_;
    TActiveRequestPtr ActiveRequest_;
    IBusPtr ReplyBus_;


    virtual void DoReply() override
    {
        Service_->OnResponse(ActiveRequest_, GetResponseMessage());
    }

    virtual void LogRequest() override
    {
        TStringBuilder builder;

        if (RequestId_ != NullRequestId) {
            AppendInfo(&builder, "RequestId: %v", RequestId_);
        }

        if (RealmId_ != NullRealmId) {
            AppendInfo(&builder, "RealmId: %v", RealmId_);
        }

        auto user = FindAuthenticatedUser(*RequestHeader_);
        if (user) {
            AppendInfo(&builder, "User: %v", *user);
        }

        AppendInfo(&builder, "%v", RequestInfo_);

        LOG_DEBUG("%v <- %v",
            GetMethod(),
            builder.Flush());
    }

    virtual void LogResponse(const TError& error) override
    {
        TStringBuilder builder;

        if (RequestId_ != NullRequestId) {
            AppendInfo(&builder, "RequestId: %v", RequestId_);
        }

        AppendInfo(&builder, "Error: %v", error);

        AppendInfo(&builder, "%v", ResponseInfo_);

        LOG_DEBUG("%v -> %v",
            GetMethod(),
            builder.Flush());
    }

};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IPrioritizedInvokerPtr defaultInvoker,
    const TServiceId& serviceId,
    const Stroka& loggingCategory)
{
    Init(
        defaultInvoker,
        serviceId,
        loggingCategory);
}

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const TServiceId& serviceId,
    const Stroka& loggingCategory)
{
    Init(
        CreateFakePrioritizedInvoker(defaultInvoker),
        serviceId,
        loggingCategory);
}

void TServiceBase::Init(
    IPrioritizedInvokerPtr defaultInvoker,
    const TServiceId& serviceId,
    const Stroka& loggingCategory)
{
    YCHECK(defaultInvoker);

    DefaultInvoker_ = defaultInvoker;
    ServiceId_ = serviceId;
    LoggingCategory_ = loggingCategory;

    ServiceTagId_ = NProfiling::TProfilingManager::Get()->RegisterTag("service", ServiceId_.ServiceName);
    
    {
        NProfiling::TTagIdList tagIds;
        tagIds.push_back(ServiceTagId_);
        RequestCounter_ = TRateCounter("/request_rate", tagIds);
    }

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(TDispatcher::Get()->GetPoolInvoker())
        .SetSystem(true));
}

TServiceId TServiceBase::GetServiceId() const
{
    return ServiceId_;
}

void TServiceBase::OnRequest(
    std::unique_ptr<NProto::TRequestHeader> header,
    TSharedRefArray message,
    IBusPtr replyBus)
{
    Profiler.Increment(RequestCounter_);

    const auto& method = header->method();
    bool oneWay = header->one_way();
    auto requestId = FromProto<TRequestId>(header->request_id());

    auto runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        auto error = TError(
            EErrorCode::NoSuchMethod,
            "Unknown method %s:%s",
            ~ServiceId_.ServiceName,
            ~method)
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!oneWay) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage, EDeliveryTrackingLevel::None);
        }
        return;
    }

    if (runtimeInfo->Descriptor.OneWay != oneWay) {
        auto error = TError(
            EErrorCode::ProtocolError,
            "One-way flag mismatch for method %s:%s: expected %s, actual %s",
            ~ServiceId_.ServiceName,
            ~method,
            ~FormatBool(runtimeInfo->Descriptor.OneWay),
            ~FormatBool(oneWay))
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!header->one_way()) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage, EDeliveryTrackingLevel::None);
        }
        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    if (runtimeInfo->QueueSizeCounter.Current > runtimeInfo->Descriptor.MaxQueueSize) {
        auto error = TError(
            EErrorCode::Unavailable,
            "Request queue limit %d reached",
            runtimeInfo->Descriptor.MaxQueueSize)
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!header->one_way()) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage, EDeliveryTrackingLevel::None);
        }
        return;
    }

    Profiler.Increment(runtimeInfo->RequestCounter, +1);

    if (header->has_request_start_time() && header->has_retry_start_time()) {
        // Decode timing information.
        auto requestStart = TInstant(header->request_start_time());
        auto retryStart = TInstant(header->retry_start_time());
        auto now = CpuInstantToInstant(GetCpuInstant());

        // Make sanity adjustments to account for possible clock skew.
        retryStart = std::min(retryStart, now);
        requestStart = std::min(requestStart, retryStart);

        // TODO(babenko): make some use of retryStart
        Profiler.Aggregate(runtimeInfo->RemoteWaitTimeCounter, (now - requestStart).MicroSeconds());
    }

    auto traceContext = GetTraceContext(*header);

    TRACE_ANNOTATION(
        traceContext,
        "server_host",
        TAddressResolver::Get()->GetLocalHostName());

    NTracing::TTraceContextGuard traceContextGuard(traceContext);

    auto activeRequest = New<TActiveRequest>(
        requestId,
        replyBus,
        runtimeInfo,
        traceContext);

    TRACE_ANNOTATION(
        traceContext,
        ServiceId_.ServiceName,
        method,
        NTracing::ServerReceiveAnnotation);

    auto context = New<TServiceContext>(
        this,
        activeRequest,
        std::move(header),
        message,
        replyBus,
        LoggingCategory_);

    if (!oneWay) {
        Profiler.Increment(runtimeInfo->QueueSizeCounter, +1);
    }

    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.HeavyRequest) {
        runtimeInfo->Descriptor.HeavyHandler
            .AsyncVia(TDispatcher::Get()->GetPoolInvoker())
            .Run(context, options)
            .Subscribe(BIND(
                &TServiceBase::OnInvocationPrepared,
                MakeStrong(this),
                std::move(activeRequest),
                context));
    } else {
        OnInvocationPrepared(
            std::move(activeRequest),
            std::move(context),
            runtimeInfo->Descriptor.LiteHandler);
    }
}

void TServiceBase::OnInvocationPrepared(
    TActiveRequestPtr activeRequest,
    IServiceContextPtr context,
    TLiteHandler handler)
{
    if (!handler)
        return;

    auto wrappedHandler = BIND([=] () {
        const auto& runtimeInfo = activeRequest->RuntimeInfo;

        // No need for a lock here.
        activeRequest->RunningSync = true;

        if (Profiler.GetEnabled()) {
            activeRequest->SyncStartTime = GetCpuInstant();
            auto value = CpuDurationToValue(activeRequest->SyncStartTime - activeRequest->ArrivalTime);
            Profiler.Aggregate(runtimeInfo->LocalWaitTimeCounter, value);
        }

        try {
            NTracing::TTraceContextGuard guard(activeRequest->TraceContext);
            if (!runtimeInfo->Descriptor.System) {
                BeforeInvoke();
            }
            handler.Run(context, runtimeInfo->Descriptor.Options);
        } catch (const std::exception& ex) {
            context->Reply(ex);
        }

        {
            TGuard<TSpinLock> guard(activeRequest->SpinLock);

            YASSERT(activeRequest->RunningSync);
            activeRequest->RunningSync = false;

            if (Profiler.GetEnabled()) {
                if (!activeRequest->Completed) {
                    activeRequest->SyncStopTime = GetCpuInstant();
                    auto value = CpuDurationToValue(activeRequest->SyncStopTime - activeRequest->SyncStartTime);
                    Profiler.Aggregate(runtimeInfo->SyncTimeCounter, value);
                }

                if (runtimeInfo->Descriptor.OneWay) {
                    auto value = CpuDurationToValue(activeRequest->SyncStopTime - activeRequest->ArrivalTime);
                    Profiler.Aggregate(runtimeInfo->TotalTimeCounter, value);
                }
            }
        }
    });

    const auto& runtimeInfo = activeRequest->RuntimeInfo;
    auto invoker = runtimeInfo->Descriptor.Invoker;
    if (!invoker) {
        invoker = DefaultInvoker_;
    }

    if (runtimeInfo->Descriptor.EnableReorder) {
        invoker->Invoke(std::move(wrappedHandler), context->GetPriority());
    } else {
        invoker->Invoke(std::move(wrappedHandler));
    }
}

void TServiceBase::OnResponse(TActiveRequestPtr activeRequest, TSharedRefArray message)
{
    TGuard<TSpinLock> guard(activeRequest->SpinLock);

    const auto& runtimeInfo = activeRequest->RuntimeInfo;

    TRACE_ANNOTATION(
        activeRequest->TraceContext,
        ServiceId_.ServiceName,
        runtimeInfo->Descriptor.Method,
        NTracing::ServerSendAnnotation);

    YASSERT(!activeRequest->Completed);
    activeRequest->Completed = true;

    activeRequest->ReplyBus->Send(std::move(message), EDeliveryTrackingLevel::None);

    // NB: This counter is also used to track queue size limit so
    // it must be maintained even if the profiler is OFF.
    Profiler.Increment(runtimeInfo->QueueSizeCounter, -1);

    if (Profiler.GetEnabled()) {
        auto now = GetCpuInstant();
    
        if (activeRequest->RunningSync) {
            activeRequest->SyncStopTime = now;
            auto value = CpuDurationToValue(activeRequest->SyncStopTime - activeRequest->SyncStartTime);
            Profiler.Aggregate(runtimeInfo->SyncTimeCounter, value);
        }

        {
            auto value = CpuDurationToValue(now - activeRequest->SyncStopTime);
            Profiler.Aggregate(runtimeInfo->AsyncTimeCounter, value);
        }

        {
            auto value = CpuDurationToValue(now - activeRequest->ArrivalTime);
            Profiler.Aggregate(runtimeInfo->TotalTimeCounter, value);
        }
    }
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    NProfiling::TTagIdList tagIds;
    tagIds.push_back(ServiceTagId_);
    tagIds.push_back(NProfiling::TProfilingManager::Get()->RegisterTag("method", descriptor.Method));
    auto runtimeInfo = New<TRuntimeMethodInfo>(descriptor, tagIds);

    TWriterGuard guard(MethodMapLock_);
    // Failure here means that such method is already registered.
    YCHECK(MethodMap_.insert(std::make_pair(descriptor.Method, runtimeInfo)).second);

    return runtimeInfo;
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
                THROW_ERROR_EXCEPTION("Cannot find RPC method %s:%s to configure",
                    ~ServiceId_.ServiceName,
                    ~methodName);
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
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error configuring RPC service %s",
            ~ServiceId_.ServiceName)
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

bool TServiceBase::IsUp() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

std::vector<Stroka> TServiceBase::SuggestAddresses() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return std::vector<Stroka>();
}

DEFINE_RPC_SERVICE_METHOD(TServiceBase, Discover)
{
    context->SetRequestInfo();

    response->set_up(IsUp());
    ToProto(response->mutable_suggested_addresses(), SuggestAddresses());

    context->SetResponseInfo("Up: %s, SuggestedAddresses: [%s]",
        ~FormatBool(response->up()),
        ~JoinToString(response->suggested_addresses()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

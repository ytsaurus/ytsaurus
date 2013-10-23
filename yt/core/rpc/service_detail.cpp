#include "stdafx.h"
#include "service_detail.h"
#include "private.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "message.h"
#include "config.h"
#include "helpers.h"

#include <core/misc/string.h>

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
    const Stroka& verb,
    THandler handler)
    : Verb(verb)
    , Handler(std::move(handler))
    , OneWay(false)
    , MaxQueueSize(100000)
    , EnableReorder(false)
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
    TRuntimeMethodInfoPtr runtimeInfo)
    : Id(id)
    , ReplyBus(std::move(replyBus))
    , RuntimeInfo(std::move(runtimeInfo))
    , RunningSync(false)
    , Completed(false)
    , ArrivalTime(GetCpuInstant())
    , SyncStartTime(-1)
    , SyncStopTime(-1)
{ }

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        TServiceBasePtr service,
        TActiveRequestPtr activeRequest,
        const NProto::TRequestHeader& header,
        TSharedRefArray requestMessage,
        IBusPtr replyBus,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , Service(std::move(service))
        , ActiveRequest(std::move(activeRequest))
        , ReplyBus(std::move(replyBus))
        , Logger(loggingCategory)
    {
        YCHECK(RequestMessage);
        YCHECK(ReplyBus);
        YCHECK(Service);
    }

private:
    TServiceBasePtr Service;
    TActiveRequestPtr ActiveRequest;
    IBusPtr ReplyBus;
    NLog::TLogger Logger;

    virtual void DoReply(TSharedRefArray responseMessage) override
    {
        Service->OnResponse(ActiveRequest, std::move(responseMessage));
    }

    virtual void LogRequest() override
    {
        Stroka str;
        str.reserve(1024); // should be enough for typical request message

        if (RequestId != NullRequestId) {
            AppendInfo(str, Sprintf("RequestId: %s", ~ToString(RequestId)));
        }

        auto user = FindAuthenticatedUser(RequestHeader_);
        if (user) {
            AppendInfo(str, Sprintf("User: %s", ~*user));
        }

        AppendInfo(str, RequestInfo);

        LOG_DEBUG("%s <- %s",
            ~GetVerb(),
            ~str);
    }

    virtual void LogResponse(const TError& error) override
    {
        Stroka str;
        str.reserve(1024); // should be enough for typical response message

        if (RequestId != NullRequestId) {
            AppendInfo(str, Sprintf("RequestId: %s", ~ToString(RequestId)));
        }

        AppendInfo(str, Sprintf("Error: %s", ~ToString(error)));

        AppendInfo(str, ResponseInfo);

        LOG_DEBUG("%s -> %s",
            ~GetVerb(),
            ~str);
    }

};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IPrioritizedInvokerPtr defaultInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
{
    Init(
        defaultInvoker,
        serviceName,
        loggingCategory);
}

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
{
    Init(
        CreateFakePrioritizedInvoker(defaultInvoker),
        serviceName,
        loggingCategory);
}

void TServiceBase::Init(
    IPrioritizedInvokerPtr defaultInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
{
    YCHECK(defaultInvoker);

    DefaultInvoker = defaultInvoker;
    ServiceName = serviceName;
    LoggingCategory = loggingCategory;

    ServiceTagId = NProfiling::TProfilingManager::Get()->RegisterTag("service", ServiceName);
    
    {
        NProfiling::TTagIdList tagIds;
        tagIds.push_back(ServiceTagId);
        RequestCounter = TRateCounter("/request_rate", tagIds);
    }
}

Stroka TServiceBase::GetServiceName() const
{
    return ServiceName;
}

void TServiceBase::OnRequest(
    const TRequestHeader& header,
    TSharedRefArray message,
    IBusPtr replyBus)
{
    Profiler.Increment(RequestCounter);

    const auto& verb = header.verb();
    bool oneWay = header.one_way();
    auto requestId = FromProto<TRequestId>(header.request_id());

    TGuard<TSpinLock> guard(SpinLock);

    auto runtimeInfo = FindMethodInfo(verb);
    if (!runtimeInfo) {
        guard.Release();

        auto error = TError(
            EErrorCode::NoSuchVerb,
            "Unknown verb %s:%s",
            ~ServiceName,
            ~verb)
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!oneWay) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage);
        }

        return;
    }

    if (runtimeInfo->Descriptor.OneWay != oneWay) {
        guard.Release();

        auto error = TError(
            EErrorCode::ProtocolError,
            "One-way flag mismatch for verb %s:%s: expected %s, actual %s",
            ~ServiceName,
            ~verb,
            ~FormatBool(runtimeInfo->Descriptor.OneWay),
            ~FormatBool(oneWay))
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!header.one_way()) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage);
        }

        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    if (runtimeInfo->QueueSizeCounter.Current > runtimeInfo->Descriptor.MaxQueueSize) {
        guard.Release();

        auto error = TError(
            EErrorCode::Unavailable,
            "Request queue limit %d reached",
            runtimeInfo->Descriptor.MaxQueueSize)
            << TErrorAttribute("request_id", requestId);
        LOG_WARNING(error);
        if (!header.one_way()) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage);
        }

        return;
    }

    Profiler.Increment(runtimeInfo->RequestCounter, +1);

    if (header.has_request_start_time() && header.has_retry_start_time()) {
        // Decode timing information.
        auto requestStart = TInstant(header.request_start_time());
        auto retryStart = TInstant(header.retry_start_time());
        auto now = CpuInstantToInstant(GetCpuInstant());

        // Make sanity adjustments to account for possible clock skew.
        retryStart = std::min(retryStart, now);
        requestStart = std::min(requestStart, retryStart);

        // TODO(babenko): make some use of retryStart
        Profiler.Aggregate(runtimeInfo->RemoteWaitTimeCounter, (now - requestStart).MicroSeconds());
    }

    auto activeRequest = New<TActiveRequest>(
        requestId,
        replyBus,
        runtimeInfo);

    auto context = New<TServiceContext>(
        this,
        activeRequest,
        header,
        message,
        replyBus,
        LoggingCategory);

    if (!oneWay) {
        YCHECK(ActiveRequests.insert(activeRequest).second);
        Profiler.Increment(runtimeInfo->QueueSizeCounter, +1);
    }

    guard.Release();

    auto handler = runtimeInfo->Descriptor.Handler;
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.HeavyRequest) {
        auto invoker = TDispatcher::Get()->GetPoolInvoker();
        handler
            .AsyncVia(std::move(invoker))
            .Run(context, options)
            .Subscribe(BIND(
                &TServiceBase::OnInvocationPrepared,
                MakeStrong(this),
                std::move(activeRequest),
                context));
    } else {
        auto preparedHandler = handler.Run(context, options);
        if (!preparedHandler)
            return;
        OnInvocationPrepared(
            std::move(activeRequest),
            std::move(context),
            std::move(preparedHandler));
    }
}

void TServiceBase::OnInvocationPrepared(
    TActiveRequestPtr activeRequest,
    IServiceContextPtr context,
    TClosure handler)
{
    if (!handler)
        return;

    auto preparedHandler = PrepareHandler(std::move(handler));
    auto wrappedHandler = BIND([=] () {
        const auto& runtimeInfo = activeRequest->RuntimeInfo;

        // No need for a lock here.
        activeRequest->RunningSync = true;
        activeRequest->SyncStartTime = GetCpuInstant();

        {
            auto value = CpuDurationToValue(activeRequest->SyncStartTime - activeRequest->ArrivalTime);
            Profiler.Aggregate(runtimeInfo->LocalWaitTimeCounter, value);
        }

        try {
            preparedHandler.Run();
        } catch (const std::exception& ex) {
            context->Reply(ex);
        }

        {
            TGuard<TSpinLock> guard(activeRequest->SpinLock);

            YCHECK(activeRequest->RunningSync);
            activeRequest->RunningSync = false;

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
    });

    const auto& runtimeInfo = activeRequest->RuntimeInfo;
    auto invoker = runtimeInfo->Descriptor.Invoker;
    if (!invoker) {
        invoker = DefaultInvoker;
    }

    bool result = runtimeInfo->Descriptor.EnableReorder
        ? invoker->Invoke(std::move(wrappedHandler), context->GetPriority())
        : invoker->Invoke(std::move(wrappedHandler));

    if (!result) {
        context->Reply(TError(EErrorCode::Unavailable, "Service unavailable"));
    }
}

TClosure TServiceBase::PrepareHandler(TClosure handler)
{
    return std::move(handler);
}

void TServiceBase::OnResponse(TActiveRequestPtr activeRequest, TSharedRefArray message)
{
    const auto& runtimeInfo = activeRequest->RuntimeInfo;
    
    bool active;
    {
        TGuard<TSpinLock> guard(SpinLock);

        active = ActiveRequests.erase(activeRequest) == 1;
    }

    {
        TGuard<TSpinLock> guard(activeRequest->SpinLock);

        YCHECK(!activeRequest->Completed);
        activeRequest->Completed = true;

        if (active) {
            Profiler.Increment(activeRequest->RuntimeInfo->QueueSizeCounter, -1);
            activeRequest->ReplyBus->Send(std::move(message));
        }

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
    TGuard<TSpinLock> guard(SpinLock);

    NProfiling::TTagIdList tagIds;
    tagIds.push_back(0);
    tagIds.push_back(NProfiling::TProfilingManager::Get()->RegisterTag("verb", descriptor.Verb));
    auto runtimeInfo = New<TRuntimeMethodInfo>(descriptor, tagIds);
    // Failure here means that such verb is already registered.
    YCHECK(RuntimeMethodInfos.insert(std::make_pair(descriptor.Verb, runtimeInfo)).second);
    return runtimeInfo;
}

void TServiceBase::Configure(INodePtr configNode)
{
    try {
        auto config = ConvertTo<TServiceConfigPtr>(configNode);
        FOREACH (const auto& pair, config->Methods) {
            const auto& methodName = pair.first;
            const auto& methodConfig = pair.second;
            auto runtimeInfo = FindMethodInfo(methodName);
            if (!runtimeInfo) {
                THROW_ERROR_EXCEPTION("Cannot find RPC method %s to configure",
                    ~methodName.Quote());
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
            ~ServiceName.Quote())
            << ex;
    }
}

void TServiceBase::CancelActiveRequests(const TError& error)
{
    yhash_set<TActiveRequestPtr> requestsToCancel;
    {
        TGuard<TSpinLock> guard(SpinLock);
        requestsToCancel.swap(ActiveRequests);
    }

    FOREACH (auto activeRequest, requestsToCancel) {
        Profiler.Increment(activeRequest->RuntimeInfo->QueueSizeCounter, -1);

        auto errorMessage = CreateErrorResponseMessage(activeRequest->Id, error);
        activeRequest->ReplyBus->Send(errorMessage);
    }
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::FindMethodInfo(const Stroka& method)
{
    auto it = RuntimeMethodInfos.find(method);
    return it == RuntimeMethodInfos.end() ? NULL : it->second;
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::GetMethodInfo(const Stroka& method)
{
    auto runtimeInfo = FindMethodInfo(method);
    YCHECK(runtimeInfo);
    return runtimeInfo;
}

IPrioritizedInvokerPtr TServiceBase::GetDefaultInvoker()
{
    return DefaultInvoker;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

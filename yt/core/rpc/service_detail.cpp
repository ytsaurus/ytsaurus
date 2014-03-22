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
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Verb(verb)
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
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
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray requestMessage,
        IBusPtr replyBus,
        const Stroka& loggingCategory)
        : TServiceContextBase(
            std::move(header),
            std::move(requestMessage))
        , Service(std::move(service))
        , ActiveRequest(std::move(activeRequest))
        , ReplyBus(std::move(replyBus))
        , Logger(loggingCategory)
    {
        YASSERT(RequestMessage_);
        YASSERT(ReplyBus);
        YASSERT(Service);
    }

    ~TServiceContext()
    {
        if (!IsOneWay() && !Replied_) {
            Reply(TError(NRpc::EErrorCode::Unavailable, "Request canceled"));
        }
    }

private:
    TServiceBasePtr Service;
    TActiveRequestPtr ActiveRequest;
    IBusPtr ReplyBus;
    NLog::TLogger Logger;


    virtual void DoReply() override
    {
        Service->OnResponse(ActiveRequest, GetResponseMessage());
    }

    virtual void LogRequest() override
    {
        if (!Logger.IsEnabled(NLog::ELogLevel::Debug))
            return;

        Stroka str;
        str.reserve(1024); // should be enough for a typical request message

        if (RequestId_ != NullRequestId) {
            AppendInfo(str, Sprintf("RequestId: %s", ~ToString(RequestId_)));
        }

        if (RealmId_ != NullRealmId) {
            AppendInfo(str, Sprintf("RealmId: %s", ~ToString(RealmId_)));
        }

        auto user = FindAuthenticatedUser(*RequestHeader_);
        if (user) {
            AppendInfo(str, Sprintf("User: %s", ~*user));
        }

        AppendInfo(str, RequestInfo_);

        LOG_DEBUG("%s <- %s",
            ~GetVerb(),
            ~str);
    }

    virtual void LogResponse(const TError& error) override
    {
        if (!Logger.IsEnabled(NLog::ELogLevel::Debug))
            return;

        Stroka str;
        str.reserve(1024); // should be enough for typical response message

        if (RequestId_ != NullRequestId) {
            AppendInfo(str, Sprintf("RequestId: %s", ~ToString(RequestId_)));
        }

        AppendInfo(str, Sprintf("Error: %s", ~ToString(error)));

        AppendInfo(str, ResponseInfo_);

        LOG_DEBUG("%s -> %s",
            ~GetVerb(),
            ~str);
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

    const auto& verb = header->verb();
    bool oneWay = header->one_way();
    auto requestId = FromProto<TRequestId>(header->request_id());

    auto runtimeInfo = FindMethodInfo(verb);
    if (!runtimeInfo) {
        auto error = TError(
            EErrorCode::NoSuchVerb,
            "Unknown verb %s:%s",
            ~ServiceId_.ServiceName,
            ~verb)
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
            "One-way flag mismatch for verb %s:%s: expected %s, actual %s",
            ~ServiceId_.ServiceName,
            ~verb,
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

    auto activeRequest = New<TActiveRequest>(
        requestId,
        replyBus,
        runtimeInfo);

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
            BeforeInvoke();
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
    tagIds.push_back(0);
    tagIds.push_back(NProfiling::TProfilingManager::Get()->RegisterTag("verb", descriptor.Verb));
    auto runtimeInfo = New<TRuntimeMethodInfo>(descriptor, tagIds);

    TWriterGuard guard(MethodMapLock_);
    // Failure here means that such verb is already registered.
    YCHECK(MethodMap_.insert(std::make_pair(descriptor.Verb, runtimeInfo)).second);

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
                THROW_ERROR_EXCEPTION("Cannot find RPC method %s in service %s to configure",
                    ~methodName.Quote(),
                    ~ServiceId_.ServiceName.Quote());
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
            ~ServiceId_.ServiceName.Quote())
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

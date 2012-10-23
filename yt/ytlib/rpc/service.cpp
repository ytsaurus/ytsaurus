#include "stdafx.h"
#include "service.h"
#include "private.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "message.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/ypath_client.h>

#include <ytlib/bus/bus.h>

#include <ytlib/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcServerLogger;
static NProfiling::TProfiler& Profiler = RpcServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    const Stroka& verb,
    THandler handler)
    : Verb(verb)
    , Handler(MoveRV(handler))
    , OneWay(false)
{ }

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    const TMethodDescriptor& descriptor,
    const TYPath& profilingPath)
    : Descriptor(descriptor)
    , ProfilingPath(profilingPath)
    , RequestCounter(profilingPath + "/request_rate")
{ }

TServiceBase::TActiveRequest::TActiveRequest(
    const TRequestId& id,
    IBusPtr replyBus,
    TRuntimeMethodInfoPtr runtimeInfo,
    const NProfiling::TTimer& timer)
    : Id(id)
    , ReplyBus(MoveRV(replyBus))
    , RuntimeInfo(runtimeInfo)
    , Timer(timer)
    , RunningSync(false)
    , Completed(false)
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
        IMessagePtr requestMessage,
        IBusPtr replyBus,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , Service(MoveRV(service))
        , ActiveRequest(MoveRV(activeRequest))
        , ReplyBus(MoveRV(replyBus))
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

    virtual void DoReply(IMessagePtr responseMessage) override
    {
        Service->OnResponse(ActiveRequest, MoveRV(responseMessage));
    }

    virtual void LogRequest() override
    {
        Stroka str;
        AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
        AppendInfo(str, RequestInfo);
        LOG_DEBUG("%s <- %s",
            ~Verb,
            ~str);
    }

    virtual void LogResponse(const TError& error) override
    {
        Stroka str;
        AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
        AppendInfo(str, Sprintf("Error: %s", ~ToString(error)));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s -> %s",
            ~Verb,
            ~str);
    }

};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : DefaultInvoker(MoveRV(defaultInvoker))
    , ServiceName(serviceName)
    , LoggingCategory(loggingCategory)
    , RequestCounter("/services/" + ServiceName + "/request_rate")
{
    YCHECK(DefaultInvoker);
}

TServiceBase::~TServiceBase()
{ }

Stroka TServiceBase::GetServiceName() const
{
    return ServiceName;
}

void TServiceBase::OnRequest(
    const TRequestHeader& header,
    IMessagePtr message,
    IBusPtr replyBus)
{
    Profiler.Increment(RequestCounter);

    const auto& verb = header.verb();
    bool oneWay = header.one_way();
    auto requestId = TRequestId::FromProto(header.request_id());

    TGuard<TSpinLock> guard(SpinLock);

    auto methodIt = RuntimeMethodInfos.find(verb);
    if (methodIt == RuntimeMethodInfos.end()) {
        guard.Release();

        auto error = TError(
            EErrorCode::NoSuchVerb,
            "Unknown verb %s:%s (RequestId: %s)",
            ~ServiceName,
            ~verb,
            ~ToString(requestId));
        LOG_WARNING(error);
        if (!oneWay) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage);
        }

        return;
    }

    auto runtimeInfo = methodIt->second;
    if (runtimeInfo->Descriptor.OneWay != oneWay) {
        guard.Release();

        auto error = TError(
            EErrorCode::ProtocolError,
            "One-way flag mismatch for verb %s:%s: expected %s, actual %s (RequestId: %s)",
            ~ServiceName,
            ~verb,
            ~FormatBool(runtimeInfo->Descriptor.OneWay),
            ~FormatBool(oneWay),
            ~ToString(requestId));
        LOG_WARNING(error);
        if (!header.one_way()) {
            auto errorMessage = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(errorMessage);
        }

        return;
    }

    Profiler.Increment(runtimeInfo->RequestCounter);
    auto timer = Profiler.TimingStart(runtimeInfo->ProfilingPath + "/time");

    auto activeRequest = New<TActiveRequest>(
        requestId,
        replyBus,
        runtimeInfo,
        timer);

    auto context = New<TServiceContext>(
        this,
        activeRequest,
        header,
        message,
        replyBus,
        LoggingCategory);

    if (!oneWay) {
        YCHECK(ActiveRequests.insert(activeRequest).second);
    }

    guard.Release();

    auto handler = runtimeInfo->Descriptor.Handler;
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.HeavyRequest) {
        auto invoker = TDispatcher::Get()->GetPoolInvoker();
        handler
            .AsyncVia(MoveRV(invoker))
            .Run(context, options)
            .Subscribe(BIND(
                &TServiceBase::OnInvocationPrepared,
                MakeStrong(this),
                MoveRV(activeRequest),
                context));
    } else {
        auto preparedHandler = handler.Run(context, options);
        OnInvocationPrepared(
            MoveRV(activeRequest),
            MoveRV(context),
            MoveRV(preparedHandler));
    }
}

void TServiceBase::OnInvocationPrepared(
    TActiveRequestPtr activeRequest,
    IServiceContextPtr context,
    TClosure handler)
{
    auto guardedHandler = context->Wrap(MoveRV(handler));

    auto wrappedHandler = BIND([=] () {
        auto& timer = activeRequest->Timer;
        auto& runtimeInfo = activeRequest->RuntimeInfo;

        {
            // No need for a lock here.
            activeRequest->RunningSync = true;
            Profiler.TimingCheckpoint(timer, "wait");
        }

        guardedHandler.Run();

        {
            TGuard<TSpinLock> guard(activeRequest->SpinLock);

            YCHECK(activeRequest->RunningSync);
            activeRequest->RunningSync = false;

            if (!activeRequest->Completed) {
                Profiler.TimingCheckpoint(timer, "sync");
            }

            if (runtimeInfo->Descriptor.OneWay) {
                Profiler.TimingStop(timer);
            }
        }
    });

    auto invoker = activeRequest->RuntimeInfo->Descriptor.Invoker;
    if (!invoker) {
        invoker = DefaultInvoker;
    }

    InvokerHandler(context, invoker, wrappedHandler);
}

void TServiceBase::InvokerHandler(
    IServiceContextPtr context,
    IInvokerPtr invoker,
    TClosure handler)
{
    if (!invoker->Invoke(MoveRV(handler))) {
        context->Reply(TError(
            EErrorCode::Unavailable,
            "Service unavailable"));
    }
}

void TServiceBase::OnResponse(TActiveRequestPtr activeRequest, IMessagePtr message)
{
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
            activeRequest->ReplyBus->Send(MoveRV(message));
        }

        auto& timer = activeRequest->Timer;

        if (activeRequest->RunningSync) {
            Profiler.TimingCheckpoint(timer, "sync");
        }
        Profiler.TimingCheckpoint(timer, "async");
        Profiler.TimingStop(timer);
    }
}

void TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    TGuard<TSpinLock> guard(SpinLock);

    auto path = "/services/" + ServiceName + "/methods/" +  descriptor.Verb;
    auto info = New<TRuntimeMethodInfo>(descriptor, path);
    // Failure here means that such verb is already registered.
    YCHECK(RuntimeMethodInfos.insert(MakePair(descriptor.Verb, info)).second);
}

void TServiceBase::CancelActiveRequests(const TError& error)
{
    yhash_set<TActiveRequestPtr> requestsToCancel;
    {
        TGuard<TSpinLock> guard(SpinLock);
        requestsToCancel.swap(ActiveRequests);
    }

    FOREACH (auto activeRequest, requestsToCancel) {
        auto errorMessage = CreateErrorResponseMessage(activeRequest->Id, error);
        activeRequest->ReplyBus->Send(errorMessage);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

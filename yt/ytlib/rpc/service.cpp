#include "stdafx.h"
#include "service.h"
#include "private.h"
#include "rpc_dispatcher.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/ypath_client.h>

#include <ytlib/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcServerLogger;
static NProfiling::TProfiler& Profiler = RpcServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    const TMethodDescriptor& descriptor,
    const NYTree::TYPath& profilingPath)
    : Descriptor(descriptor)
    , ProfilingPath(profilingPath)
    , RequestCounter(profilingPath + "/request_rate")
{ }

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : DefaultInvoker(defaultInvoker)
    , ServiceName(serviceName)
    , LoggingCategory(loggingCategory)
    , RequestCounter("/services/" + ServiceName + "/request_rate")
{
    YASSERT(defaultInvoker);
}

TServiceBase::~TServiceBase()
{ }

Stroka TServiceBase::GetServiceName() const
{
    return ServiceName;
}

Stroka TServiceBase::GetLoggingCategory() const
{
    return LoggingCategory;
}

void TServiceBase::OnBeginRequest(IServiceContextPtr context)
{
    YASSERT(context);

    Profiler.Increment(RequestCounter);

    Stroka verb = context->GetVerb();

    TGuard<TSpinLock> guard(SpinLock);

    auto methodIt = RuntimeMethodInfos.find(verb);
    if (methodIt == RuntimeMethodInfos.end()) {
        guard.Release();

        Stroka message = Sprintf("Unknown verb %s:%s",
            ~ServiceName,
            ~verb);
        LOG_WARNING("%s", ~message);
        if (!context->IsOneWay()) {
            context->Reply(TError(EErrorCode::NoSuchVerb, message));
        }

        return;
    }

    auto runtimeInfo = methodIt->second;
    if (runtimeInfo->Descriptor.OneWay != context->IsOneWay()) {
        guard.Release();

        Stroka message = Sprintf("One-way flag mismatch for verb %s:%s: expected %s, actual %s",
            ~ServiceName,
            ~verb,
            ~FormatBool(runtimeInfo->Descriptor.OneWay).Quote(),
            ~FormatBool(context->IsOneWay()).Quote());
        LOG_WARNING("%s", ~message);
        if (!context->IsOneWay()) {
            context->Reply(TError(EErrorCode::NoSuchVerb, message));
        }

        return;
    }

    Profiler.Increment(runtimeInfo->RequestCounter);
    auto timer = Profiler.TimingStart(runtimeInfo->ProfilingPath + "/time");

    auto activeRequest = New<TActiveRequest>(context, runtimeInfo, timer);

    if (!context->IsOneWay()) {
        YCHECK(ActiveRequests.insert(MakePair(context, activeRequest)).second);
    }

    guard.Release();

    auto handler = runtimeInfo->Descriptor.Handler;
    const auto& options = runtimeInfo->Descriptor.Options;
    if (options.HeavyRequest) {
        auto invoker = TRpcDispatcher::Get()->GetPoolInvoker();
        handler
            .AsyncVia(invoker)
            .Run(context, options)
            .Subscribe(BIND(&TServiceBase::OnInvocationPrepared, MakeStrong(this), activeRequest));
    } else {
        auto preparedHandler = handler.Run(context, options);
        OnInvocationPrepared(activeRequest, preparedHandler);
    }
}

void TServiceBase::OnInvocationPrepared(
    TActiveRequestPtr activeRequest,
    TClosure handler)
{
    auto guardedHandler = activeRequest->Context->Wrap(handler);

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

            YASSERT(activeRequest->RunningSync);
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

    if (!invoker->Invoke(wrappedHandler)) {
        activeRequest->Context->Reply(TError(
            EErrorCode::Unavailable,
            "Service unavailable"));
    }
}

void TServiceBase::OnEndRequest(IServiceContextPtr context)
{
    YASSERT(context);
    YASSERT(!context->IsOneWay());

    TGuard<TSpinLock> guard(SpinLock);

    auto it = ActiveRequests.find(context);
    if (it == ActiveRequests.end())
        return;

    auto& activeRequest = it->second;

    {
        TGuard<TSpinLock> guard(activeRequest->SpinLock);

        YASSERT(!activeRequest->Completed);
        activeRequest->Completed = true;

        auto& timer = activeRequest->Timer;

        if (activeRequest->RunningSync) {
            Profiler.TimingCheckpoint(timer, "sync");
        }
        Profiler.TimingCheckpoint(timer, "async");
        Profiler.TimingStop(timer);
    }

    ActiveRequests.erase(it);
}

void TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    auto path = "/services/" + ServiceName + "/methods/" +  descriptor.Verb;
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto info = New<TRuntimeMethodInfo>(descriptor, path);
        // Failure here means that such verb is already registered.
        YCHECK(RuntimeMethodInfos.insert(MakePair(descriptor.Verb, info)).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#include "stdafx.h"
#include "service.h"

#include "../logging/log.h"
#include "../misc/assert.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TServiceContext::TServiceContext(
    IService::TPtr service,
    TRequestId requestId,
    const Stroka& methodName,
    IMessage::TPtr message,
    IBus::TPtr replyBus)
    : Service(service)
    , RequestId(requestId)
    , MethodName(methodName)
    , ReplyBus(replyBus)
    , ServiceLogger(service->GetLoggingCategory()) // TODO: move this to body
    , Replied(false)
{
    YASSERT(~service != NULL);
    YASSERT(~message != NULL);
    YASSERT(~replyBus != NULL);

    RequestBody = message->GetParts().at(1);
    RequestAttachments = yvector<TSharedRef>(message->GetParts().begin() + 2, message->GetParts().end());
}

void TServiceContext::Reply(EErrorCode errorCode /* = EErrorCode::OK */)
{
    Reply(TError(errorCode));
}

void TServiceContext::Reply(const TError& error)
{
    // Failure here means that #Reply is called twice.
    YASSERT(!Replied);

    Replied = true;
    LogResponseInfo(error);
    Service->OnEndRequest(this);

    IMessage::TPtr message;
    if (error.IsOK()) {
        message = New<TRpcResponseMessage>(
            RequestId,
            error,
            &ResponseBody,
            ResponseAttachments);
    } else {
        message = New<TRpcErrorResponseMessage>(
            RequestId,
            error);
    }

    ReplyBus->Send(message);
    
}

bool TServiceContext::IsReplied() const
{
    return Replied;
}

TSharedRef TServiceContext::GetRequestBody() const
{
    return RequestBody;
}

const yvector<TSharedRef>& TServiceContext::GetRequestAttachments() const
{
    return RequestAttachments;
}

void TServiceContext::SetResponseBody(TBlob* responseBody)
{
    ResponseBody.swap(*responseBody);
}

void TServiceContext::SetResponseAttachments(yvector<TSharedRef>* attachments)
{
    ResponseAttachments.swap(*attachments);
}

Stroka TServiceContext::GetServiceName() const
{
    return Service->GetServiceName();
}

Stroka TServiceContext::GetMethodName() const
{
    return MethodName;
}

const TRequestId& TServiceContext::GetRequestId() const
{
    return RequestId;
}

void TServiceContext::SetRequestInfo(const Stroka& info)
{
    RequestInfo = info;
    LogRequestInfo();
}

Stroka TServiceContext::GetRequestInfo() const
{
    return RequestInfo;
}

void TServiceContext::SetResponseInfo(const Stroka& info)
{
    ResponseInfo = info;
}

Stroka TServiceContext::GetResponseInfo()
{
    return ResponseInfo;
}

IAction::TPtr TServiceContext::Wrap(IAction::TPtr action)
{
    return FromMethod(
        &TServiceContext::WrapThunk,
        TPtr(this),
        action);
}

void TServiceContext::WrapThunk(IAction::TPtr action) throw()
{
    try {
        action->Do();
    } catch (const TServiceException& ex) {
        Reply(ex.GetError());
    } catch (...) {
        auto errorMessage = CurrentExceptionMessage();
        Reply(TError(EErrorCode::ServiceError, errorMessage));

        Stroka str;
        AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
        AppendInfo(str, ResponseInfo);
        LOG_FATAL("Unhandled exception in service method (%s): %s",
            ~str,
            ~errorMessage);
    }
}

void TServiceContext::LogRequestInfo()
{
    Stroka str;
    AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
    AppendInfo(str, RequestInfo);
    LOG_EVENT(
        ServiceLogger,
        NLog::ELogLevel::Debug,
        "%s <- %s",
        ~MethodName,
        ~str);
}

void TServiceContext::LogResponseInfo(const TError& error)
{
    Stroka str;
    AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
    AppendInfo(str, Sprintf("Error: %s", ~error.ToString()));
    AppendInfo(str, ResponseInfo);
    LOG_EVENT(
        ServiceLogger,
        NLog::ELogLevel::Debug,
        "%s -> %s",
        ~MethodName,
        ~str);
}

void TServiceContext::AppendInfo(Stroka& lhs, const Stroka& rhs)
{
    if (!rhs.Empty()) {
        if (!lhs.Empty()) {
            lhs.append(", ");
        }
        lhs.append(rhs);
    }
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IInvoker::TPtr defaultServiceInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : DefaultServiceInvoker(defaultServiceInvoker)
    , ServiceName(serviceName)
    , ServiceLogger(loggingCategory)
{
    YASSERT(~defaultServiceInvoker != NULL);
}

void TServiceBase::RegisterMethod(
    const TMethodDescriptor& descriptor,
    IInvoker::TPtr invoker)
{
    YASSERT(~invoker != NULL);

    TGuard<TSpinLock> guard(SpinLock);

    if (!RuntimeMethodInfos.insert(MakePair(
        descriptor.MethodName,
        TRuntimeMethodInfo(descriptor, invoker))).Second()) {
        ythrow yexception() << Sprintf("Method is already registered (ServiceName: %s, MethodName: %s)",
            ~ServiceName,
            ~descriptor.MethodName);
    }
}

void TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    RegisterMethod(descriptor, DefaultServiceInvoker);
}

void TServiceBase::OnBeginRequest(IServiceContext* context)
{
    YASSERT(context != NULL);

    TGuard<TSpinLock> guard(SpinLock);

    Stroka methodName = context->GetMethodName();
    auto methodIt = RuntimeMethodInfos.find(methodName);
    TRuntimeMethodInfo* runtimeInfo =
        methodIt == RuntimeMethodInfos.end()
        ? NULL
        : &methodIt->Second();

    YVERIFY(ActiveRequests.insert(MakePair(
        context,
        TActiveRequest(runtimeInfo, TInstant::Now()))).Second());

    if (runtimeInfo == NULL) {
        LOG_WARNING("Unknown method (ServiceName: %s, MethodName: %s)",
            ~ServiceName,
            ~methodName);
        context->Reply(EErrorCode::NoMethod);
    } else {
        auto handler = runtimeInfo->Descriptor.Handler;
        auto wrappedHandler = context->Wrap(handler->Bind(context));
        runtimeInfo->Invoker->Invoke(wrappedHandler);
    }
}

void TServiceBase::OnEndRequest(IServiceContext* context)
{
    YASSERT(context != NULL);

    TGuard<TSpinLock> guard(SpinLock);
    auto it = ActiveRequests.find(context);
    YASSERT(it != ActiveRequests.end());
    auto& request = it->Second();
    if (request.RuntimeInfo != NULL) {
        request.RuntimeInfo->ExecutionTime.AddDelta(request.StartTime);       
    }
    ActiveRequests.erase(it);
}

Stroka TServiceBase::GetServiceName() const
{
    return ServiceName;
}

Stroka TServiceBase::GetLoggingCategory() const
{
    return ServiceLogger.GetCategory();
}

Stroka TServiceBase::GetDebugInfo() const
{
    TGuard<TSpinLock> guard(SpinLock);

    Stroka info = "Service " + ServiceName + ":\n";
    FOREACH(const auto& pair, RuntimeMethodInfos) {
        info += Sprintf("Method %s: %s\n",
            ~pair.First(),
            ~pair.Second().ExecutionTime.GetDebugInfo());
    }
    return info;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

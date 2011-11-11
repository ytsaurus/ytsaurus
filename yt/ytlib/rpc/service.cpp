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
    IService* service,
    const TRequestId& requestId,
    const Stroka& path,
    const Stroka& verb,
    IMessage* requestMessage)
    : Service(service)
    , RequestId(requestId)
    , Path(path)
    , Verb(verb)
    , ServiceLogger(service->GetLoggingCategory()) // TODO: move this to body
    , Replied(false)
{
    YASSERT(service != NULL);
    YASSERT(requestMessage != NULL);

    RequestBody = requestMessage->GetParts().at(1);
    RequestAttachments = yvector<TSharedRef>(
        requestMessage->GetParts().begin() + 2,
        requestMessage->GetParts().end());
}

void TServiceContext::Reply(const TError& error)
{
    // Failure here means that #Reply is called twice.
    YASSERT(!Replied);

    Replied = true;
    LogResponseInfo(error);

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

    Service->OnEndRequest(this, ~message);
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

Stroka TServiceContext::GetPath() const
{
    return Path;
}

Stroka TServiceContext::GetVerb() const
{
    return Verb;
}

TRequestId TServiceContext::GetRequestId() const
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

IAction::TPtr TServiceContext::Wrap(IAction* action)
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
        AppendInfo(str, Sprintf("Path: %s", ~Path));
        AppendInfo(str, Sprintf("Verb: %s", ~Verb));
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
        ~Verb,
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
        ~Verb,
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
    IInvoker* defaultServiceInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : DefaultServiceInvoker(defaultServiceInvoker)
    , ServiceName(serviceName)
    , ServiceLogger(loggingCategory)
{
    YASSERT(defaultServiceInvoker != NULL);
}

void TServiceBase::RegisterMethod(
    const TMethodDescriptor& descriptor,
    IInvoker* invoker)
{
    YASSERT(invoker != NULL);

    TGuard<TSpinLock> guard(SpinLock);

    if (!RuntimeMethodInfos.insert(MakePair(
        descriptor.Verb,
        TRuntimeMethodInfo(descriptor, invoker))).Second()) {
        ythrow yexception() << Sprintf("Verb is already registered (ServiceName: %s, Verb: %s)",
            ~ServiceName,
            ~descriptor.Verb);
    }
}

void TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    RegisterMethod(descriptor, ~DefaultServiceInvoker);
}

void TServiceBase::OnBeginRequest(IServiceContext* context, IBus* replyBus)
{
    YASSERT(context != NULL);

    TGuard<TSpinLock> guard(SpinLock);

    Stroka verb = context->GetVerb();
    auto methodIt = RuntimeMethodInfos.find(verb);
    TRuntimeMethodInfo* runtimeInfo =
        methodIt == RuntimeMethodInfos.end()
        ? NULL
        : &methodIt->Second();

    TActiveRequest activeRequest(
        replyBus,
        runtimeInfo,
        TInstant::Now());
    YVERIFY(ActiveRequests.insert(MakePair(context, activeRequest)).Second());

    if (runtimeInfo == NULL) {
        LOG_WARNING("Unknown method (ServiceName: %s, Verb: %s)",
            ~ServiceName,
            ~verb);
        context->Reply(TError(EErrorCode::NoSuchMethod));
    } else {
        auto handler = runtimeInfo->Descriptor.Handler;
        auto wrappedHandler = context->Wrap(~handler->Bind(context));
        runtimeInfo->Invoker->Invoke(wrappedHandler);
    }
}

void TServiceBase::OnEndRequest(IServiceContext* context, IMessage* responseMessage)
{
    YASSERT(context != NULL);

    TGuard<TSpinLock> guard(SpinLock);
    auto it = ActiveRequests.find(context);
    YASSERT(it != ActiveRequests.end());
    auto& request = it->Second();
    request.ReplyBus->Send(responseMessage);
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

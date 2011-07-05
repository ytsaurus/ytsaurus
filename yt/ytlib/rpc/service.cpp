#include "service.h"

#include "../misc/string.h"
#include "../logging/log.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TRpcManager::Get()->GetLogger();

////////////////////////////////////////////////////////////////////////////////

TServiceContext::TServiceContext(
    IService::TPtr service,
    TRequestId requestId,
    Stroka methodName,
    IMessage::TPtr message,
    IBus::TPtr replyBus)
    : Service(service)
    , RequestId(requestId)
    , MethodName(methodName)
    , State(S_Received)
    , ReplyBus(replyBus)
    , RequestBody(message->GetParts().at(1))
    , RequestAttachments(message->GetParts().begin() + 2, message->GetParts().end())
    , ServiceLogger(service->GetLoggingCategory())
{ }

void TServiceContext::Reply(EErrorCode errorCode /* = EErrorCode::OK */)
{
    // TODO: move to a separate method LogResponseInfo
    Stroka str;
    AppendInfo(str, Sprintf("RequestId: %s", ~StringFromGuid(RequestId)));
    AppendInfo(str, Sprintf("ErrorCode: %s", ~errorCode.ToString()));
    AppendInfo(str, ResponseInfo);
    LOG_EVENT(
        ServiceLogger,
        NLog::ELogLevel::Debug,
        "%s -> %s",
        ~MethodName,
        ~str);

    DoReply(errorCode);
}

void TServiceContext::DoReply(EErrorCode errorCode /* = EErrorCode::OK */)
{
    // Failure here means that Reply is called twice.
    YASSERT(State == S_Received);

    IMessage::TPtr message;
    if (errorCode.IsRpcError()) {
        message = new TRpcErrorResponseMessage(
            RequestId,
            errorCode);
    } else {
        message = new TRpcResponseMessage(
            RequestId,
            errorCode,
            ResponseBody,
            ResponseAttachments);
    }

    ReplyBus->Send(message);
    State = S_Replied;
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

IBus::TPtr TServiceContext::GetReplyBus() const
{
    return ReplyBus;
}

IAction::TPtr TServiceContext::Wrap(IAction::TPtr action)
{
    return FromMethod(&TServiceContext::WrapThunk, TPtr(this), action);
}

void TServiceContext::WrapThunk(IAction::TPtr action) throw()
{
    try {
        action->Do();
    } catch (const TServiceException& e) {
        DoReply(e.GetErrorCode());
        LogException(NLog::ELogLevel::Debug, e.GetErrorCode(), e.what());
    } catch (const NStl::exception& e) {
        DoReply(EErrorCode::ServiceError);
        LogException(NLog::ELogLevel::Fatal, EErrorCode::ServiceError, e.what());
    }
}

void TServiceContext::LogException(
    NLog::ELogLevel level,
    EErrorCode errorCode,
    Stroka what)
{
    Stroka str;
    AppendInfo(str, Sprintf("RequestId: %s", ~StringFromGuid(RequestId)));
    AppendInfo(str, Sprintf("ErrorCode: %s", ~errorCode.ToString()));
    AppendInfo(str, ResponseInfo);
    AppendInfo(str, Sprintf("What: %s", what));
    LOG_EVENT(
        ServiceLogger,
        level,
        "%s -> %s",
        ~MethodName,
        ~str);
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(Stroka serviceName, Stroka loggingCategory)
    : ServiceName(serviceName)
    , ServiceLogger(loggingCategory)
{ }

void TServiceBase::RegisterHandler(
    Stroka methodName,
    THandler::TPtr handler)
{
    bool inserted = Handlers.insert(MakePair(methodName, handler)).Second();
    if (!inserted) {
        LOG_FATAL("Method is already registered (ServiceName: %s, MethodName: %s)",
            ~ServiceName, ~methodName);
    }
}

void TServiceBase::OnRequest(TServiceContext::TPtr context)
{
    Stroka methodName = context->GetMethodName();
    THandlerMap::iterator it = Handlers.find(methodName);
    if (it == Handlers.end()) {
        LOG_WARNING("Unknown method (ServiceName: %s, MethodName: %s)",
            ~ServiceName, ~methodName);
        IMessage::TPtr errorMessage = new TRpcErrorResponseMessage(
            context->GetRequestId(),
            EErrorCode::NoMethod);
        context->GetReplyBus()->Send(errorMessage);
        return;
    }

    THandler::TPtr handler = it->Second();
    context->Wrap(handler->Bind(context))->Do();
}

Stroka TServiceBase::GetServiceName() const
{
    return ServiceName;
}

Stroka TServiceBase::GetLoggingCategory() const
{
    return ServiceLogger.GetCategory();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

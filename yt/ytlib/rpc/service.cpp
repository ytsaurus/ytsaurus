#include "service.h"

#include "../misc/string.h"
#include "../logging/log.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TRpcManager::Get()->GetLogger();

////////////////////////////////////////////////////////////////////////////////

TServiceContext::TServiceContext(
    TRequestId requestId,
    Stroka serviceName,
    Stroka methodName,
    IMessage::TPtr message,
    IBus::TPtr replyBus,
    NLog::TLogger& serviceLogger)
    : RequestId(requestId)
    , ServiceName(serviceName)
    , MethodName(methodName)
    , State(S_Received)
    , ReplyBus(replyBus)
    , RequestBody(message->GetParts().at(1))
    , RequestAttachments(message->GetParts().begin() + 2, message->GetParts().end())
    , ServiceLogger(serviceLogger)
{ }

void TServiceContext::Reply(EErrorCode errorCode /* = EErrorCode::OK */)
{
    if (State != S_Received) {
        // Reply is called twice.
        YASSERT(false);
    }

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

    // TODO: move to a separate method
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
    return ServiceName;
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
        LOG_ERROR("Exception occurred while executing request (ServiceName: %s, MethodName: %s): %s",
                    ~ServiceName, ~MethodName, e.what());
        Reply(e.GetErrorCode());
    } catch (const yexception& e) {
        LOG_ERROR("Exception occurred while executing request (ServiceName, %s, MethodName: %s): %s",
                    ~ServiceName, ~MethodName, e.what());
        Reply(EErrorCode::ServiceError);
    }
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

NLog::TLogger& TServiceBase::GetLogger() 
{
    return ServiceLogger;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

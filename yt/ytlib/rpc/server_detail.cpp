#include "stdafx.h"
#include "server_detail.h"

#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/rpc/message.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TServiceContextBase::TServiceContextBase(
    const TRequestHeader& header,
    IMessagePtr requestMessage)
    : Header(header)
    , RequestMessage(requestMessage)
    , RequestId(header.has_request_id()
        ? FromProto<TRequestId>(header.request_id())
        : NullRequestId)
    , Replied(false)
    , ResponseAttributes_(CreateEphemeralAttributes())
{
    YASSERT(requestMessage);

    const auto& parts = requestMessage->GetParts();
    YASSERT(parts.size() >= 2);
    RequestBody = parts[1];
    RequestAttachments_ = std::vector<TSharedRef>(parts.begin() + 2, parts.end());
    RequestAttributes_ =
        header.has_attributes()
        ? FromProto(header.attributes())
        : CreateEphemeralAttributes();
}

void TServiceContextBase::Reply(const TError& error)
{
    CheckRepliable();

    Error = error;
    Replied = true;

    LogResponse(error);

    auto responseMessage = CreateResponseMessage(this);
    DoReply(responseMessage);
}

void TServiceContextBase::Reply(IMessagePtr responseMessage)
{
    CheckRepliable();

    auto parts = responseMessage->GetParts();
    YASSERT(!parts.empty());

    TResponseHeader header;
    YCHECK(DeserializeFromProto(&header, parts[0]));

    Error = FromProto(header.error());
    ResponseBody = TSharedRef();
    ResponseAttachments_.clear();

    if (Error.IsOK()) {
        YASSERT(parts.size() >= 2);
        ResponseBody = parts[1];
        ResponseAttachments_.insert(
            ResponseAttachments_.end(),
            parts.begin() + 2,
            parts.end());
    }

    Replied = true;

    LogResponse(Error);

    DoReply(responseMessage);
}

bool TServiceContextBase::IsOneWay() const
{
    return Header.one_way();
}

bool TServiceContextBase::IsReplied() const
{
    return Replied;
}

const TError& TServiceContextBase::GetError() const
{
    YASSERT(Replied);
    return Error;
}

TSharedRef TServiceContextBase::GetRequestBody() const
{
    return RequestBody;
}

std::vector<TSharedRef>& TServiceContextBase::RequestAttachments()
{
    return RequestAttachments_;
}

IAttributeDictionary& TServiceContextBase::RequestAttributes()
{
    return *RequestAttributes_;
}

TSharedRef TServiceContextBase::GetResponseBody()
{
    return ResponseBody;
}

void TServiceContextBase::SetResponseBody(const TSharedRef& responseBody)
{
    CheckRepliable();
    ResponseBody = responseBody;
}

std::vector<TSharedRef>& TServiceContextBase::ResponseAttachments()
{
    YASSERT(!IsOneWay());
    return ResponseAttachments_;
}

IAttributeDictionary& TServiceContextBase::ResponseAttributes()
{
    return *ResponseAttributes_;
}

IMessagePtr TServiceContextBase::GetRequestMessage() const
{
    return RequestMessage;
}

const TRequestId& TServiceContextBase::GetRequestId() const
{
    return RequestId;
}

TNullable<TInstant> TServiceContextBase::GetRequestStartTime() const
{
    return Header.has_request_start_time() ? TNullable<TInstant>(TInstant(Header.request_start_time())) : Null;
}

TNullable<TInstant> TServiceContextBase::GetRetryStartTime() const
{
    return Header.has_retry_start_time() ? TNullable<TInstant>(TInstant(Header.retry_start_time())) : Null;
}

i64 TServiceContextBase::GetPriority() const
{
    return Header.has_request_start_time() ? -Header.request_start_time() : 0;
}

const Stroka& TServiceContextBase::GetPath() const
{
    return Header.path();
}

const Stroka& TServiceContextBase::GetVerb() const
{
    return Header.verb();
}

void TServiceContextBase::SetRequestInfo(const Stroka& info)
{
    RequestInfo = info;
    LogRequest();
}

Stroka TServiceContextBase::GetRequestInfo() const
{
    return RequestInfo;
}

void TServiceContextBase::SetResponseInfo(const Stroka& info)
{
    CheckRepliable();
    ResponseInfo = info;
}

Stroka TServiceContextBase::GetResponseInfo()
{
    return ResponseInfo;
}

TClosure TServiceContextBase::Wrap(const TClosure& action)
{
    return BIND(
        &TServiceContextBase::WrapThunk,
        MakeStrong(this),
        action);
}

void TServiceContextBase::WrapThunk(const TClosure& action)
{
    try {
        action.Run();
    } catch (const std::exception& ex) {
        OnException(ex);
    }
}

void TServiceContextBase::OnException(const TError& error)
{
    if (IsOneWay()) {
        // We are unable to send a reply but let's just log something.
        LogResponse(error);
    } else {
        Reply(error);
    }
}

void TServiceContextBase::CheckRepliable() const
{
    // Failure here means that the request is already replied.
    YASSERT(!Replied);

    // Failure here indicates an attempt to reply to a one-way request.
    YASSERT(!IsOneWay());
}

void TServiceContextBase::AppendInfo(Stroka& lhs, const Stroka& rhs)
{
    if (!rhs.Empty()) {
        if (!lhs.Empty()) {
            lhs.append(", ");
        }
        lhs.append(rhs);
    }
}

////////////////////////////////////////////////////////////////////////////////

TServiceContextWrapper::TServiceContextWrapper(IServiceContextPtr underlyingContext)
    : UnderlyingContext(std::move(underlyingContext))
{ }

IMessagePtr TServiceContextWrapper::GetRequestMessage() const
{
    return UnderlyingContext->GetRequestMessage();
}

const TRequestId& TServiceContextWrapper::GetRequestId() const
{
    return UnderlyingContext->GetRequestId();
}

TNullable<TInstant> TServiceContextWrapper::GetRequestStartTime() const
{
    return UnderlyingContext->GetRequestStartTime();
}

TNullable<TInstant> TServiceContextWrapper::GetRetryStartTime() const
{
    return UnderlyingContext->GetRetryStartTime();
}

i64 TServiceContextWrapper::GetPriority() const
{
    return UnderlyingContext->GetPriority();
}

const Stroka& TServiceContextWrapper::GetPath() const
{
    return UnderlyingContext->GetPath();
}

const Stroka& TServiceContextWrapper::GetVerb() const
{
    return UnderlyingContext->GetVerb();
}

bool TServiceContextWrapper::IsOneWay() const
{
    return UnderlyingContext->IsOneWay();
}

bool TServiceContextWrapper::IsReplied() const
{
    return UnderlyingContext->IsReplied();
}

void TServiceContextWrapper::Reply(const TError& error)
{
    UnderlyingContext->Reply(error);
}

void TServiceContextWrapper::Reply(IMessagePtr responseMessage)
{
    UnderlyingContext->Reply(responseMessage);
}

const TError& TServiceContextWrapper::GetError() const
{
    return UnderlyingContext->GetError();
}

TSharedRef TServiceContextWrapper::GetRequestBody() const
{
    return UnderlyingContext->GetRequestBody();
}

TSharedRef TServiceContextWrapper::GetResponseBody()
{
    return UnderlyingContext->GetResponseBody();
}

void TServiceContextWrapper::SetResponseBody(const TSharedRef& responseBody)
{
    UnderlyingContext->SetResponseBody(responseBody);
}

std::vector<TSharedRef>& TServiceContextWrapper::RequestAttachments()
{
    return UnderlyingContext->RequestAttachments();
}

std::vector<TSharedRef>& TServiceContextWrapper::ResponseAttachments()
{
    return UnderlyingContext->ResponseAttachments();
}

IAttributeDictionary& TServiceContextWrapper::RequestAttributes()
{
    return UnderlyingContext->RequestAttributes();
}

IAttributeDictionary& TServiceContextWrapper::ResponseAttributes()
{
    return UnderlyingContext->ResponseAttributes();
}

void TServiceContextWrapper::SetRequestInfo(const Stroka& info)
{
    UnderlyingContext->SetRequestInfo(info);
}

Stroka TServiceContextWrapper::GetRequestInfo() const
{
    return UnderlyingContext->GetRequestInfo();
}

void TServiceContextWrapper::SetResponseInfo(const Stroka& info)
{
    UnderlyingContext->SetResponseInfo(info);
}

Stroka TServiceContextWrapper::GetResponseInfo()
{
    return UnderlyingContext->GetRequestInfo();
}

TClosure TServiceContextWrapper::Wrap(const TClosure& action)
{
    return UnderlyingContext->Wrap(action);
}

////////////////////////////////////////////////////////////////////////////////

TReplyInterceptorContext::TReplyInterceptorContext(
    IServiceContextPtr underlyingContext,
    TClosure onReply)
    : TServiceContextWrapper(std::move(underlyingContext))
    , OnReply(std::move(onReply))
{ }

void TReplyInterceptorContext::Reply(const TError& error)
{
    TServiceContextWrapper::Reply(error);
    OnReply.Run();
}

void TReplyInterceptorContext::Reply(IMessagePtr responseMessage)
{
    TServiceContextWrapper::Reply(std::move(responseMessage));
    OnReply.Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#include "stdafx.h"
#include "server_detail.h"

#include <core/ytree/attribute_helpers.h>

#include <core/rpc/message.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

TServiceContextBase::TServiceContextBase(
    const TRequestHeader& header,
    IMessagePtr requestMessage)
    : RequestHeader_(header)
    , RequestMessage(requestMessage)
    , RequestId(header.has_request_id()
        ? FromProto<TRequestId>(header.request_id())
        : NullRequestId)
    , RealmId(header.has_realm_id()
        ? FromProto<TRealmId>(header.realm_id())
        : NullRealmId)
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
    YASSERT(!Replied);

    Error = error;
    Replied = true;

    if (IsOneWay()) {
        // Cannot reply OK to a one-way request.
        YCHECK(!error.IsOK());
    } else {
        auto responseMessage = CreateResponseMessage(this);
        DoReply(responseMessage);
    }

    LogResponse(error);
}

void TServiceContextBase::Reply(IMessagePtr responseMessage)
{
    YASSERT(!Replied);
    YASSERT(!IsOneWay());

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
    } else {
        ResponseBody = TSharedRef();
        ResponseAttachments_.clear();
    }

    Replied = true;

    DoReply(responseMessage);
    
    LogResponse(Error);
}

bool TServiceContextBase::IsOneWay() const
{
    return RequestHeader_.one_way();
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
    YASSERT(!Replied);
    YASSERT(!IsOneWay());

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

TRequestId TServiceContextBase::GetRequestId() const
{
    return RequestId;
}

TNullable<TInstant> TServiceContextBase::GetRequestStartTime() const
{
    return
        RequestHeader_.has_request_start_time()
        ? TNullable<TInstant>(TInstant(RequestHeader_.request_start_time()))
        : Null;
}

TNullable<TInstant> TServiceContextBase::GetRetryStartTime() const
{
    return
        RequestHeader_.has_retry_start_time()
        ? TNullable<TInstant>(TInstant(RequestHeader_.retry_start_time()))
        : Null;
}

i64 TServiceContextBase::GetPriority() const
{
    return
        RequestHeader_.has_request_start_time()
        ? -RequestHeader_.request_start_time()
        : 0;
}

const Stroka& TServiceContextBase::GetService() const
{
    return RequestHeader_.service();
}

const Stroka& TServiceContextBase::GetVerb() const
{
    return RequestHeader_.verb();
}

const TRealmId& TServiceContextBase::GetRealmId() const
{
    return RealmId;
}

const TRequestHeader& TServiceContextBase::RequestHeader() const
{
    return RequestHeader_;
}

TRequestHeader& TServiceContextBase::RequestHeader()
{
    return RequestHeader_;
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
    YASSERT(!Replied);
    YASSERT(!IsOneWay());

    ResponseInfo = info;
}

Stroka TServiceContextBase::GetResponseInfo()
{
    return ResponseInfo;
}

void TServiceContextBase::AppendInfo(Stroka& lhs, const Stroka& rhs)
{
    if (!rhs.empty()) {
        if (!lhs.empty()) {
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

TRequestId TServiceContextWrapper::GetRequestId() const
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

const Stroka& TServiceContextWrapper::GetService() const
{
    return UnderlyingContext->GetService();
}

const Stroka& TServiceContextWrapper::GetVerb() const
{
    return UnderlyingContext->GetVerb();
}

const TRealmId& TServiceContextWrapper::GetRealmId() const 
{
    return UnderlyingContext->GetRealmId();
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

const NProto::TRequestHeader& TServiceContextWrapper::RequestHeader() const 
{
    return UnderlyingContext->RequestHeader();
}

NProto::TRequestHeader& TServiceContextWrapper::RequestHeader()
{
    return UnderlyingContext->RequestHeader();
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

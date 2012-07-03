#include "stdafx.h"
#include "server_detail.h"

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
    : RequestId(header.has_request_id() ? TRequestId::FromProto(header.request_id()) : NullRequestId)
    , Path(header.path())
    , Verb(header.verb())
    , RequestMessage(requestMessage)
    , OneWay(header.has_one_way() ? header.one_way() : false)
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

    TResponseHeader header;
    *header.mutable_request_id() = RequestId.ToProto();
    *header.mutable_error() = Error.ToProto();
    ToProto(header.mutable_attributes(), *ResponseAttributes_);

    IMessagePtr responseMessage;
    if (error.IsOK()) {
        responseMessage = CreateResponseMessage(
            header,
            MoveRV(ResponseBody),
            ResponseAttachments_);
    } else {
        responseMessage = CreateErrorResponseMessage(header);
    }

    DoReply(error, responseMessage);
}

bool TServiceContextBase::IsOneWay() const
{
    return OneWay;
}

bool TServiceContextBase::IsReplied() const
{
    return Replied;
}

TError TServiceContextBase::GetError() const
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

void TServiceContextBase::SetResponseBody(const TSharedRef& responseBody)
{
    CheckRepliable();
    ResponseBody = responseBody;
}

std::vector<TSharedRef>& TServiceContextBase::ResponseAttachments()
{
    YASSERT(!OneWay);
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

const Stroka& TServiceContextBase::GetPath() const
{
    return Path;
}

const Stroka& TServiceContextBase::GetVerb() const
{
    return Verb;
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

TClosure TServiceContextBase::Wrap(TClosure action)
{
    return BIND(
        &TServiceContextBase::WrapThunk,
        MakeStrong(this),
        action);
}

void TServiceContextBase::WrapThunk(TClosure action)
{
    try {
        action.Run();
    } catch (const TServiceException& ex) {
        OnException(ex.GetError());
    } catch (const std::exception& ex) {
        OnException(TError(EErrorCode::ServiceError, ex.what()));
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
    YASSERT(!OneWay);
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

} // namespace NRpc
} // namespace NYT

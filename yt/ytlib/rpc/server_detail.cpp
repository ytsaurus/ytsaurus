#include "stdafx.h"
#include "server_detail.h"

#include <ytlib/misc/assert.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TServiceContextBase::TServiceContextBase(
    const TRequestHeader& header,
    IMessage* requestMessage)
    : RequestId(header.has_request_id() ? TRequestId::FromProto(header.request_id()) : NullRequestId)
    , Path(header.path())
    , Verb(header.verb())
    , RequestMessage(requestMessage)
    , OneWay(header.has_one_way() ? header.one_way() : false)
    , Replied(false)
{
    YASSERT(requestMessage);

    const auto& parts = requestMessage->GetParts();
    YASSERT(parts.size() >= 2);
    RequestBody = parts[1];
    RequestAttachments_ = yvector<TSharedRef>(parts.begin() + 2, parts.end());
}

void TServiceContextBase::Reply(const TError& error)
{
    CheckRepliable();

    Error = error;
    Replied = true;

    LogResponse(error);

    IMessage::TPtr responseMessage;
    if (error.IsOK()) {
        TResponseHeader header;
        header.set_request_id(RequestId.ToProto());
        header.set_error_code(TError::OK);
        responseMessage = CreateResponseMessage(
            header,
            MoveRV(ResponseBody),
            ResponseAttachments_);
    } else {
        responseMessage = CreateErrorResponseMessage(
            RequestId,
            error);
    }

    DoReply(error, ~responseMessage);
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

const yvector<TSharedRef>& TServiceContextBase::RequestAttachments() const
{
    return RequestAttachments_;
}

void TServiceContextBase::SetResponseBody(const TSharedRef& responseBody)
{
    CheckRepliable();
    ResponseBody = responseBody;
}

yvector<TSharedRef>& TServiceContextBase::ResponseAttachments()
{
    YASSERT(!OneWay);
    return ResponseAttachments_;
}

IMessage::TPtr TServiceContextBase::GetRequestMessage() const
{
    return RequestMessage;
}

Stroka TServiceContextBase::GetPath() const
{
    return Path;
}

Stroka TServiceContextBase::GetVerb() const
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

IAction::TPtr TServiceContextBase::Wrap(IAction* action)
{
    return FromMethod(
        &TServiceContextBase::WrapThunk,
        TPtr(this),
        action);
}

void TServiceContextBase::WrapThunk(IAction::TPtr action) throw()
{
    try {
        action->Do();
    } catch (const TServiceException& ex) {
        Reply(ex.GetError());
    } catch (...) {
        auto message = CurrentExceptionMessage();
        Reply(TError(EErrorCode::ServiceError, message));
        LogException(message);
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

#include "stdafx.h"
#include "../misc/assert.h"
#include "client.h"
#include "message.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(IChannel* channel, const Stroka& serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
{
    YASSERT(channel);
}

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannel* channel,
    const Stroka& path,
    const Stroka& verb)
    : Path_(path)
    , Verb_(verb)
    , RequestId_(TRequestId::Create())
    , Channel(channel)
{
    YASSERT(channel);
}

IMessage::TPtr TClientRequest::Serialize() const
{
    auto bodyData = SerializeBody();

    TRequestHeader header;
    header.set_request_id(RequestId_.ToProto());
    header.set_path(Path_);
    header.set_verb(Verb_);

    return CreateRequestMessage(
        header,
        MoveRV(bodyData),
        Attachments_);
}

void TClientRequest::DoInvoke(
    IClientResponseHandler* responseHandler,
    TDuration timeout)
{
    Channel->Send(this, responseHandler, timeout);
}

////////////////////////////////////////////////////////////////////////////////

TClientResponseBase::TClientResponseBase(const TRequestId& requestId)
    : RequestId_(requestId)
    , StartTime_(TInstant::Now())
    , State(EState::Sent)
{ }

bool TClientResponseBase::IsOK() const
{
    return Error_.IsOK();
}

int TClientResponseBase::GetErrorCode() const
{
    return Error_.GetCode();
}

void TClientResponseBase::OnError(const TError& error)
{
    LOG_DEBUG("Request failed (RequestId: %s)\n%s",
        ~RequestId_.ToString(),
        ~error.ToString());

    {
        TGuard<TSpinLock> guard(&SpinLock);
        if (State == EState::Done) {
            // Ignore the error.
            // Most probably this is a late timeout.
            return;
        }
        State = EState::Done;
    }

    Error_  = error;
    FireCompleted();
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(const TRequestId& requestId)
    : TClientResponseBase(requestId)
{ }

IMessage::TPtr TClientResponse::GetResponseMessage() const
{
    YASSERT(ResponseMessage);
    return ResponseMessage;
}

void TClientResponse::Deserialize(IMessage* responseMessage)
{
    YASSERT(responseMessage);
    YASSERT(!ResponseMessage);

    ResponseMessage = responseMessage;

    const auto& parts = responseMessage->GetParts();
    YASSERT(parts.ysize() >= 2);

    DeserializeBody(parts[1]);
    
    Attachments_.clear();
    std::copy(
        parts.begin() + 2,
        parts.end(),
        std::back_inserter(Attachments_));
}

void TClientResponse::OnAcknowledgement()
{
    LOG_DEBUG("Request acknowledged (RequestId: %s)", ~RequestId_.ToString());

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent) {
        State = EState::Ack;
    }
}

void TClientResponse::OnResponse(IMessage* message)
{
    LOG_DEBUG("Response received (RequestId: %s)",
        ~RequestId_.ToString());

    {
        TGuard<TSpinLock> guard(&SpinLock);
        YASSERT(State == EState::Sent || State == EState::Ack);
        State = EState::Done;
    }

    Deserialize(message);
    FireCompleted();
}

////////////////////////////////////////////////////////////////////////////////

TOneWayClientResponse::TOneWayClientResponse(const TRequestId& requestId)
    : TClientResponseBase(requestId)
    , AsyncResult(New< TFuture<TPtr> >())
{ }

void TOneWayClientResponse::OnAcknowledgement()
{
    LOG_DEBUG("Request acknowledged (RequestId: %s)", ~RequestId_.ToString());

    {
        TGuard<TSpinLock> guard(&SpinLock);
        if (State == EState::Done) {
            // Ignore the ack.
            return;
        }
        State = EState::Done;
    }

    FireCompleted();
}

void TOneWayClientResponse::OnResponse(IMessage* message)
{
    UNUSED(message);
    YUNREACHABLE();
}

TFuture<TOneWayClientResponse::TPtr>::TPtr TOneWayClientResponse::GetAsyncResult()
{
    return AsyncResult;
}

void TOneWayClientResponse::FireCompleted()
{
    AsyncResult->Set(this);
    AsyncResult.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

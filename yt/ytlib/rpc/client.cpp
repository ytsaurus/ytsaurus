#include "stdafx.h"
#include "../misc/assert.h"
#include "client.h"
#include "message.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(IChannel* channel, const Stroka& serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
{
    YASSERT(channel != NULL);
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
    YASSERT(channel != NULL);
}

IMessage::TPtr TClientRequest::Serialize() const
{
    TBlob bodyData;
    if (!SerializeBody(&bodyData)) {
        LOG_FATAL("Error serializing request body");
    }

    return CreateRequestMessage(
        RequestId_,
        Path_,
        Verb_,
        MoveRV(bodyData),
        Attachments_);
}

void TClientRequest::DoInvoke(
    TClientResponse* response,
    TDuration timeout)
{
    Channel->Send(this, response, timeout);
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(const TRequestId& requestId)
    : RequestId_(requestId)
    , StartTime_(TInstant::Now())
    , State(EState::Sent)
{ }

IMessage::TPtr TClientResponse::GetResponseMessage() const
{
    YASSERT(~ResponseMessage != NULL);
    return ResponseMessage;
}

bool TClientResponse::IsOK() const
{
    return Error_.IsOK();
}

int TClientResponse::GetErrorCode() const
{
    return Error_.GetCode();
}

void TClientResponse::Deserialize(IMessage* responseMessage)
{
    YASSERT(responseMessage != NULL);
    YASSERT(~ResponseMessage == NULL);

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

void TClientResponse::OnError(const TError& error)
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

} // namespace NRpc
} // namespace NYT

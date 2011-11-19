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

TProxyBase::TProxyBase(IChannel::TPtr channel, const Stroka& serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
{
    YASSERT(~channel != NULL);
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

TFuture<TError>::TPtr TClientRequest::DoInvoke(
    TClientResponse* response,
    TDuration timeout)
{
    return Channel->Send(this, response, timeout);
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
    return Error_.GetCode() == EErrorCode::OK;
}

EErrorCode TClientResponse::GetErrorCode() const
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

void TClientResponse::OnAcknowledgement(IBus::ESendResult sendResult)
{
    LOG_DEBUG("Request acknowledged (RequestId: %s, Result: %s)",
        ~RequestId_.ToString(),
        ~sendResult.ToString());

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent) {
        switch (sendResult) {
            case IBus::ESendResult::OK:
                State = EState::Ack;
                break;

            case IBus::ESendResult::Failed:
                Complete(TError(EErrorCode::TransportError));
                break;

            default:
                YUNREACHABLE();
        }
    }
}

void TClientResponse::OnTimeout()
{
    LOG_DEBUG("Request timed out (RequestId: %s)",
        ~RequestId_.ToString());

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent || State == EState::Ack) {
        Complete(TError(EErrorCode::Timeout));
    }
}

void TClientResponse::OnResponse(const TError& error, IMessage* message)
{
    LOG_DEBUG("Response received (RequestId: %s)",
        ~RequestId_.ToString());

    if (error.GetCode() == EErrorCode::OK) {
        Deserialize(message);
    }

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent || State == EState::Ack) {
        Complete(error);
    }
}

void TClientResponse::Complete(const TError& error)
{
    LOG_DEBUG("Request complete (RequestId: %s, Error: %s)",
        ~RequestId_.ToString(),
        ~error.ToString());

    Error_ = error;
    State = EState::Done;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

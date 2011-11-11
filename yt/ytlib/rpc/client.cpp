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
    : Channel(channel)
    , Path(path)
    , Verb(verb)
    , RequestId(TRequestId::Create())
{
    YASSERT(channel != NULL);
}

IMessage::TPtr TClientRequest::Serialize() const
{
    TBlob bodyData;
    if (!SerializeBody(&bodyData)) {
        LOG_FATAL("Error serializing request body");
    }

    return New<TRpcRequestMessage>(
        RequestId,
        Path,
        Verb,
        &bodyData,
        Attachments_);
}

TFuture<TError>::TPtr TClientRequest::DoInvoke(
    TClientResponse* response,
    TDuration timeout)
{
    return Channel->Send(this, response, timeout);
}

yvector<TSharedRef>& TClientRequest::Attachments()
{
    return Attachments_;
}

NYT::NRpc::TRequestId TClientRequest::GetRequestId() const
{
    return RequestId;
}

Stroka TClientRequest::GetVerb() const
{
    return Verb;
}

Stroka TClientRequest::GetPath() const
{
    return Path;
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(
    const TRequestId& requestId,
    IChannel::TPtr channel)
    : RequestId(requestId)
    , Channel(channel)
    , State(EState::Sent)
    , StartTime(TInstant::Now())
{
    YASSERT(~channel != NULL);
}

void TClientResponse::Deserialize(IMessage::TPtr message)
{
    YASSERT(~message != NULL);
    const yvector<TSharedRef>& parts = message->GetParts();
    if (parts.ysize() > 1) {
        DeserializeBody(parts[1]);
        MyAttachments.clear();
        NStl::copy(
            parts.begin() + 2,
            parts.end(),
            NStl::back_inserter(MyAttachments));
    }
}

void TClientResponse::OnAcknowledgement(IBus::ESendResult sendResult)
{
    LOG_DEBUG("Request acknowledged (RequestId: %s, Result: %s)",
        ~RequestId.ToString(),
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
        ~RequestId.ToString());

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent || State == EState::Ack) {
        Complete(TError(EErrorCode::Timeout));
    }
}

void TClientResponse::OnResponse(const TError& error, IMessage::TPtr message)
{
    LOG_DEBUG("Response received (RequestId: %s)",
        ~RequestId.ToString());

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
        ~RequestId.ToString(),
        ~error.ToString());

    Error = error;
    State = EState::Done;
}

bool TClientResponse::IsOK() const
{
    return Error.GetCode() == EErrorCode::OK;
}

yvector<TSharedRef>& TClientResponse::Attachments()
{
    return MyAttachments;
}

TError TClientResponse::GetError() const
{
    return Error;
}

EErrorCode TClientResponse::GetErrorCode() const
{
    return Error.GetCode();
}

TInstant TClientResponse::GetStartTime() const
{
    return StartTime;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

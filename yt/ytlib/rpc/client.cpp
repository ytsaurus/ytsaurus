#include "client.h"
#include "message.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(IChannel::TPtr channel, Stroka serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
{ }

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannel::TPtr channel,
    Stroka serviceName,
    Stroka methodName)
    : Channel(channel)
    , ServiceName(serviceName)
    , MethodName(methodName)
    , RequestId(TRequestId::Create())
{ }

IMessage::TPtr TClientRequest::Serialize()
{
    TBlob bodyData;
    if (!SerializeBody(&bodyData)) {
        LOG_FATAL("Error serializing request body");
    }

    return new TRpcRequestMessage(
        RequestId,
        ServiceName,
        MethodName,
        bodyData,
        Attachments_);
}

TAsyncResult<TVoid>::TPtr TClientRequest::DoInvoke(
    TClientResponse::TPtr response,
    TDuration timeout)
{
    return Channel->Send(this, response, timeout);
}

yvector<TSharedRef>& TClientRequest::Attachments()
{
    return Attachments_;
}

NYT::NRpc::TRequestId TClientRequest::GetRequestId()
{
    return RequestId;
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(
    const TRequestId& requestId,
    IChannel::TPtr channel)
    : RequestId(requestId)
    , Channel(channel)
    , State(EState::Sent)
    , ErrorCode(EErrorCode::OK)
{ }

void TClientResponse::Deserialize(IMessage::TPtr message)
{
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
                Complete(EErrorCode::TransportError);
                break;

            default:
                YASSERT(false);
                break;
        }
    }
}

void TClientResponse::OnTimeout()
{
    LOG_DEBUG("Request timed out (RequestId: %s)",
        ~RequestId.ToString());

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent || State == EState::Ack) {
        Complete(EErrorCode::Timeout);
    }
}

void TClientResponse::OnResponse(EErrorCode errorCode, IMessage::TPtr message)
{
    LOG_DEBUG("Response received (RequestId: %s)",
        ~RequestId.ToString());

    Deserialize(message);

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent || State == EState::Ack) {
        Complete(errorCode);
    }
}

void TClientResponse::Complete(EErrorCode errorCode)
{
    LOG_DEBUG("Request complete (RequestId: %s, ErrorCode: %s)",
        ~RequestId.ToString(),
        ~errorCode.ToString());

    ErrorCode = errorCode;
    State = EState::Done;
}

bool TClientResponse::IsOK() const
{
    return ErrorCode == EErrorCode::OK;
}

bool TClientResponse::IsRpcError() const
{
    return ErrorCode < 0;
}

bool TClientResponse::IsServiceError() const
{
    return ErrorCode > 0;
}

yvector<TSharedRef>& TClientResponse::Attachments()
{
    return MyAttachments;
}

EErrorCode TClientResponse::GetErrorCode() const
{
    return ErrorCode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

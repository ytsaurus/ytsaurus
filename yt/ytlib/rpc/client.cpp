#include "client.h"

#include "../misc/serialize.h"
#include "../misc/string.h"
#include "../logging/log.h"

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TChannel::TChannel(TBusClient::TPtr client)
    : Bus(client->CreateBus(this))
{ }

void TChannel::OnMessage(IMessage::TPtr message, IBus::TPtr replyBus)
{
    UNUSED(replyBus);

    const yvector<TSharedRef>& parts = message->GetParts();
    if (parts.ysize() == 0) {
        LOG_ERROR("Missing header part");
        return;
    }

    TResponseHeader header;
    DeserializeMessage(&header, parts[0]);

    TRequestId requestId = GuidFromProtoGuid(header.GetRequestId());
    TClientResponse::TPtr response = GetResponse(requestId);
    if (~response == NULL) {
        LOG_WARNING("Response for an incorrect or obsolete request received (RequestId: %s)",
            ~StringFromGuid(requestId));
        return;
    }

    response->OnResponse(header.GetErrorCode(), message);
}

TIntrusivePtr<TClientResponse> TChannel::GetResponse(TRequestId id)
{
    TGuard<TSpinLock> guard(&SpinLock);
    TRequestMap::iterator i = ResponseMap.find(id);
    if (i == ResponseMap.end()) {
        return NULL;
    } else {
        return i->Second();
    }
}

void TChannel::Send(
    TClientRequest::TPtr request,
    TClientResponse::TPtr response,
    TDuration timeout)
{
    TRequestId requestId = RegisterResponse(response);
    response->Prepare(requestId, timeout);
    IMessage::TPtr requestMessage = request->Serialize(requestId);
    Bus->Send(requestMessage)->Subscribe(FromMethod(
        &TClientResponse::OnAcknowledgment,
        response));

    LOG_DEBUG("Request sent (ServiceName: %s, MethodName:%s, RequestId: %s)",
        ~request->ServiceName,
        ~request->MethodName,
        ~StringFromGuid(requestId));
}

TRequestId TChannel::RegisterResponse(TClientResponse::TPtr response)
{
    TRequestId requestId;
    CreateGuid(&requestId);
    {
        TGuard<TSpinLock> guard(&SpinLock);
        ResponseMap.insert(MakePair(requestId, response));
    }
    LOG_DEBUG("Request registered (RequestId: %s)",
        ~StringFromGuid(requestId));
    return requestId;
}

void TChannel::UnregisterResponse(TRequestId requestId)
{
    {
        TGuard<TSpinLock> guard(&SpinLock);
        ResponseMap.erase(requestId);
    }
    LOG_DEBUG("Request unregistered (RequestId: %s)",
        ~StringFromGuid(requestId));
}

////////////////////////////////////////////////////////////////////////////////

TChannel::TPtr TChannelCache::GetChannel( Stroka address )
{
    TChannelMap::iterator it = ChannelMap.find(address);
    if (it != ChannelMap.end()) {
        return it->second;
    }
    TBusClient::TPtr client = new TBusClient(address);
    TChannel::TPtr channel = new TChannel(client);
    ChannelMap[address] = channel;
    return channel;
}

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(TChannel::TPtr channel, Stroka serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
{ }

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(TChannel::TPtr channel, Stroka serviceName, Stroka methodName)
    : Channel(channel)
    , ServiceName(serviceName)
    , MethodName(methodName)
{ }

IMessage::TPtr TClientRequest::Serialize(TRequestId requestId)
{
    TBlob bodyData;
    if (!SerializeBody(&bodyData)) {
        LOG_FATAL("Error serializing request body");
    }

    return new TRpcRequestMessage(
        requestId,
        ServiceName,
        MethodName,
        bodyData,
        Attachments_);
}

void TClientRequest::DoInvoke(TIntrusivePtr<TClientResponse> response, TDuration timeout)
{
    Channel->Send(this, response, timeout);
}

yvector<TSharedRef>& TClientRequest::Attachments()
{
    return Attachments_;
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(TChannel::TPtr channel)
    : Channel(channel)
    , State(S_Sent)
    , ErrorCode(EErrorCode::OK)
{}

void TClientResponse::Prepare(TRequestId requestId, TDuration timeout)
{
    YASSERT(RequestId == TGUID());
    RequestId = requestId;
    if (timeout != TDuration::Zero()) {
        TimeoutCookie = TDelayedInvoker::Get()->Submit(FromMethod(
            &TClientResponse::OnTimeout,
            TPtr(this)),
            timeout);
    }
}

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

void TClientResponse::OnAcknowledgment(IBus::ESendResult sendResult)
{
    // TODO: ToString
    LOG_DEBUG("Request acknowledged (RequestId: %s, Result: %d)",
        ~StringFromGuid(RequestId),
        static_cast<int>(sendResult));

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == S_Sent) {
        switch (sendResult) {
            case IBus::OK:
                State = S_Ack;
                break;

            case IBus::Failed:
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
        ~StringFromGuid(RequestId));

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == S_Sent || State == S_Ack) {
        Complete(EErrorCode::Timeout);
    }
}

void TClientResponse::OnResponse(EErrorCode errorCode, IMessage::TPtr message)
{
    LOG_DEBUG("Response received (RequestId: %s)",
        ~StringFromGuid(RequestId));

    Deserialize(message);

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == S_Sent || State == S_Ack) {
        Complete(errorCode);
    }
}

void TClientResponse::Complete(EErrorCode errorCode)
{
    LOG_DEBUG("Request complete (RequestId: %s, ErrorCode: %s)",
        ~StringFromGuid(RequestId),
        ~errorCode.ToString());

    if (errorCode != EErrorCode::Timeout && TimeoutCookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(TimeoutCookie);
    }

    Channel->UnregisterResponse(RequestId);
    ErrorCode = errorCode;
    State = S_Done;
    TimeoutCookie = TDelayedInvoker::TCookie();
    SetReady();
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

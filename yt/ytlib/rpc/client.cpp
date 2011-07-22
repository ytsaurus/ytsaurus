#include "client.h"

#include "../misc/serialize.h"
#include "../logging/log.h"

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TChannel::TChannel(TBusClient::TPtr client)
{
    Bus = client->CreateBus(this);
}

TChannel::TChannel(Stroka address)
{
    TBusClient::TPtr client = new TBusClient(address);
    Bus = client->CreateBus(this);
}

void TChannel::OnMessage(
    IMessage::TPtr message,
    IBus::TPtr replyBus)
{
    UNUSED(replyBus);

    const yvector<TSharedRef>& parts = message->GetParts();
    if (parts.ysize() == 0) {
        LOG_ERROR("Missing header part");
        return;
    }

    TResponseHeader header;
    DeserializeMessage(&header, parts[0]);

    TRequestId requestId = TGuid::FromProto(header.GetRequestId());
    TEntry::TPtr entry = FindEntry(requestId);
    if (~entry == NULL || !Unregister(entry->RequestId)) {
        LOG_WARNING("Response for an incorrect or obsolete request received (RequestId: %s)",
            ~requestId.ToString());
        return;
    }

    entry->Response->OnResponse(header.GetErrorCode(), message);
    entry->Ready->Set(TVoid());
}

TChannel::TEntry::TPtr TChannel::FindEntry(const TRequestId& id)
{
    TGuard<TSpinLock> guard(&SpinLock);
    TEntries::iterator it = Entries.find(id);
    if (it == Entries.end()) {
        return NULL;
    } else {
        return it->Second();
    }
}

TAsyncResult<TVoid>::TPtr TChannel::Send(
    TClientRequest::TPtr request,
    TClientResponse::TPtr response,
    TDuration timeout)
{
    TRequestId requestId = TGuid::Create();

    TEntry::TPtr entry = new TEntry();
    entry->RequestId = requestId;
    entry->Response = response;
    entry->Ready = new TAsyncResult<TVoid>();

    if (timeout != TDuration::Zero()) {
        entry->TimeoutCookie = TDelayedInvoker::Get()->Submit(FromMethod(
            &TChannel::OnTimeout,
            TPtr(this),
            entry),
            timeout);
    }

    {
        TGuard<TSpinLock> guard(&SpinLock);
        YVERIFY(Entries.insert(MakePair(requestId, entry)).Second());
    }
    
    IMessage::TPtr requestMessage = request->Serialize(requestId);
    Bus->Send(requestMessage)->Subscribe(FromMethod(
        &TChannel::OnAcknowledgement,
        TPtr(this),
        entry));

    LOG_DEBUG("Request sent (ServiceName: %s, MethodName:%s, RequestId: %s)",
        ~request->ServiceName,
        ~request->MethodName,
        ~requestId.ToString());

    return entry->Ready;
}

void TChannel::OnAcknowledgement(
    IBus::ESendResult sendResult,
    TEntry::TPtr entry)
{
    if (sendResult == IBus::ESendResult::Failed) {
        if (!Unregister(entry->RequestId))
            return;
        entry->Response->OnAcknowledgement(sendResult);
        entry->Ready->Set(TVoid());
    } else {
        entry->Response->OnAcknowledgement(sendResult);
    }
}

void TChannel::OnTimeout(TEntry::TPtr entry)
{
    if (!Unregister(entry->RequestId))
        return;

    entry->Response->OnTimeout();
    entry->Ready->Set(TVoid());
}

bool TChannel::Unregister(const TRequestId& requestId)
{
    // TODO: cancel timeout cookie
    {
        TGuard<TSpinLock> guard(&SpinLock);
        if (Entries.erase(requestId) == 0)
            return false;
    }

    LOG_DEBUG("Request unregistered (RequestId: %s)",
        ~requestId.ToString());
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TChannel::TPtr TChannelCache::GetChannel(Stroka address)
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

TProxyBase::TProxyBase(IChannel::TPtr channel, Stroka serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
{ }

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(IChannel::TPtr channel, Stroka serviceName, Stroka methodName)
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

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(IChannel::TPtr channel)
    : Channel(channel)
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

void TClientResponse::SetRequestId(const TRequestId& requestId)
{
    RequestId = requestId;
}

TRequestId TClientResponse::GetRequestId() const
{
    return RequestId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

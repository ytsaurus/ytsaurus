#include "channel.h"
#include "client.h"
#include "message.h"

#include "../misc/assert.h"

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
    Bus = New<TBusClient>(address)->CreateBus(this);
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

    TRequestId requestId = TRequestId::FromProto(header.GetRequestId());
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
    auto it = Entries.find(id);
    if (it == Entries.end()) {
        return NULL;
    } else {
        return it->Second();
    }
}

TFuture<TVoid>::TPtr TChannel::Send(
    TClientRequest::TPtr request,
    TClientResponse::TPtr response,
    TDuration timeout)
{
    TRequestId requestId = request->GetRequestId();

    TEntry::TPtr entry = New<TEntry>();
    entry->RequestId = requestId;
    entry->Response = response;
    entry->Ready = New< TFuture<TVoid> >();

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
    
    IMessage::TPtr requestMessage = request->Serialize();
    Bus->Send(requestMessage)->Subscribe(FromMethod(
        &TChannel::OnAcknowledgement,
        TPtr(this),
        entry));

    LOG_DEBUG("Request sent (RequestId: %s, ServiceName: %s, MethodName: %s)",
        ~requestId.ToString(),
        ~request->ServiceName,
        ~request->MethodName);

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
    TEntry::TPtr entry;

    {
        TGuard<TSpinLock> guard(&SpinLock);
        auto it = Entries.find(requestId);
        if (it == Entries.end())
            return false;

        entry = it->Second();
        Entries.erase(it);
    }

    if (entry->TimeoutCookie != TDelayedInvoker::TCookie()) {
        TDelayedInvoker::Get()->Cancel(entry->TimeoutCookie);
        entry->TimeoutCookie = TDelayedInvoker::TCookie();
    }

    LOG_DEBUG("Request unregistered (RequestId: %s)", ~requestId.ToString());
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TChannel::TPtr TChannelCache::GetChannel(Stroka address)
{
    auto it = ChannelMap.find(address);
    if (it == ChannelMap.end()) {
        it = ChannelMap.insert(MakePair(address, New<TChannel>(address))).First();
    }
    return it->Second();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#pragma once

#include "common.h"

#include "../bus/bus_client.h"
#include "../actions/async_result.h"
#include "../misc/delayed_invoker.h"


namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TClientRequest;
class TClientResponse;

struct IChannel
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IChannel> TPtr;

    virtual TAsyncResult<TVoid>::TPtr Send(
        TIntrusivePtr<TClientRequest> request,
        TIntrusivePtr<TClientResponse> response,
        TDuration timeout) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IChannel
    , public NBus::IMessageHandler
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel(NBus::TBusClient::TPtr client);
    TChannel(Stroka address);

    virtual TAsyncResult<TVoid>::TPtr Send(
        TIntrusivePtr<TClientRequest> request,
        TIntrusivePtr<TClientResponse> response,
        TDuration timeout);

private:
    friend class TClientRequest;
    friend class TClientResponse;

    struct TEntry
        : public TRefCountedBase
    {
        typedef TIntrusivePtr<TEntry> TPtr;

        TRequestId RequestId;
        TIntrusivePtr<TClientResponse> Response;
        TAsyncResult<TVoid>::TPtr Ready;
        TDelayedInvoker::TCookie TimeoutCookie;
    };

    typedef yhash_map<TRequestId, TEntry::TPtr, TRequestIdHash> TEntries;

    NBus::IBus::TPtr Bus;
    TSpinLock SpinLock;
    TEntries Entries;

    void OnAcknowledgement(
        NBus::IBus::ESendResult sendResult,
        TEntry::TPtr entry);

    bool Unregister(const TRequestId& requestId);

    TEntry::TPtr FindEntry(const TRequestId& id);

    virtual void OnMessage(
        NBus::IMessage::TPtr message,
        NBus::IBus::TPtr replyBus);

    void OnTimeout(TEntry::TPtr entry);
};          

////////////////////////////////////////////////////////////////////////////////

class TChannelCache
    : private TNonCopyable
{
public:
    TChannel::TPtr GetChannel(Stroka address);

private:
    typedef yhash_map<Stroka, TChannel::TPtr> TChannelMap;

    TChannelMap ChannelMap;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

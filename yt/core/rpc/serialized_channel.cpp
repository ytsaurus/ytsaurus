#include "stdafx.h"
#include "serialized_channel.h"
#include "client.h"

#include <queue>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TSerializedChannel
    : public IChannel
{
public:
    explicit TSerializedChannel(IChannelPtr underlyingChannel);

    virtual TNullable<TDuration> GetDefaultTimeout() const override;

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override;

    virtual TFuture<void> Terminate(const TError& error) override;

    void OnRequestCompleted();

private:
    IChannelPtr UnderlyingChannel;

    struct TEntry
        : public TIntrinsicRefCounted
    {
        TEntry(IClientRequestPtr request, IClientResponseHandlerPtr handler, TNullable<TDuration> timeout)
            : Request(std::move(request))
            , Handler(std::move(handler))
            , Timeout(timeout)
        { }

        IClientRequestPtr Request;
        IClientResponseHandlerPtr Handler;
        TNullable<TDuration> Timeout;
    };

    typedef TIntrusivePtr<TEntry> TEntryPtr;

    TSpinLock SpinLock;
    std::queue<TEntryPtr> Queue;
    bool RequestInProgress;

    void TrySendQueuedRequests();

};

typedef TIntrusivePtr<TSerializedChannel> TSerializedChannelPtr;

IChannelPtr CreateSerializedChannel(IChannelPtr underlyingChannel)
{
    YCHECK(underlyingChannel);

    return New<TSerializedChannel>(std::move(underlyingChannel));
}

////////////////////////////////////////////////////////////////////////////////

class TSerializedResponseHandler
    : public IClientResponseHandler
{
public:
    TSerializedResponseHandler(
        IClientResponseHandlerPtr underlyingHandler,
        TSerializedChannelPtr channel)
        : UnderlyingHandler(std::move(underlyingHandler))
        , Channel(std::move(channel))
    { }

    virtual void OnAcknowledgement() override
    {
        UnderlyingHandler->OnAcknowledgement();
    }

    virtual void OnResponse(TSharedRefArray message) override
    {
        UnderlyingHandler->OnResponse(std::move(message));
        Channel->OnRequestCompleted();
    }

    virtual void OnError(const TError& error) override
    {
        UnderlyingHandler->OnError(error);
        Channel->OnRequestCompleted();
    }

private:
    IClientResponseHandlerPtr UnderlyingHandler;
    TSerializedChannelPtr Channel;

};

TSerializedChannel::TSerializedChannel(IChannelPtr underlyingChannel)
    : UnderlyingChannel(std::move(underlyingChannel))
    , RequestInProgress(false)
{ }

TNullable<TDuration> TSerializedChannel::GetDefaultTimeout() const
{
    return UnderlyingChannel->GetDefaultTimeout();
}

void TSerializedChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout)
{
    auto entry = New<TEntry>(request, responseHandler, timeout);

    {
        TGuard<TSpinLock> guard(SpinLock);
        Queue.push(entry);
    }

    TrySendQueuedRequests();
}

TFuture<void> TSerializedChannel::Terminate(const TError& error)
{
    UNUSED(error);
    YUNREACHABLE();
}

void TSerializedChannel::TrySendQueuedRequests()
{
    TGuard<TSpinLock> guard(SpinLock);
    while (!RequestInProgress && !Queue.empty()) {
        auto entry = Queue.front();
        Queue.pop();
        RequestInProgress = true;
        guard.Release();

        auto serializedHandler = New<TSerializedResponseHandler>(entry->Handler, this);
        UnderlyingChannel->Send(entry->Request, serializedHandler, entry->Timeout);
        entry->Request.Reset();
        entry->Handler.Reset();
    }
}

void TSerializedChannel::OnRequestCompleted()
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        YCHECK(RequestInProgress);
        RequestInProgress = false;
    }

    TrySendQueuedRequests();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

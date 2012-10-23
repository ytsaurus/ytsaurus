#include "stdafx.h"
#include "scoped_channel.h"
#include "client.h"

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSerializedChannel
    : public IChannel
{
public:
    explicit TSerializedChannel(IChannelPtr underlyingChannel);

    virtual TNullable<TDuration> GetDefaultTimeout() const override;

    virtual bool GetRetryEnabled() const override;

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override;

    virtual void Terminate(const TError& error) override;

    void OnRequestCompleted();

private:
    IChannelPtr UnderlyingChannel;

    DECLARE_ENUM(EEntryState,
        (Waiting)
        (Sent)
        (TimedOut)
    );

    struct TEntry
        : public TIntrinsicRefCounted
    {
        TEntry(IClientRequestPtr request, IClientResponseHandlerPtr handler, TNullable<TInstant> deadline)
            : EnqueueTime(TInstant::Now())
            , State(EEntryState::Waiting)
            , Request(MoveRV(request))
            , Handler(MoveRV(handler))
            , Deadline(deadline)
        { }

        TInstant EnqueueTime;
        TAtomic State;
        IClientRequestPtr Request;
        IClientResponseHandlerPtr Handler;
        TNullable<TInstant> Deadline;
    };

    typedef TIntrusivePtr<TEntry> TEntryPtr;

    TLockFreeQueue<TEntryPtr> Queue;
    TAtomic QueueSize;

    void SendQueuedRequest();
    void OnQueuedRequestTimeout(TEntryPtr entry);

};

typedef TIntrusivePtr<TSerializedChannel> TSerializedChannelPtr;

class TSerializedResponseHandler
    : public IClientResponseHandler
{
public:
    TSerializedResponseHandler(
        IClientResponseHandlerPtr underlyingHandler,
        TSerializedChannelPtr channel)
        : UnderlyingHandler(MoveRV(underlyingHandler))
        , Channel(MoveRV(channel))
    { }

    virtual void OnAcknowledgement() override
    {
        UnderlyingHandler->OnAcknowledgement();
    }
    
    virtual void OnResponse(NBus::IMessagePtr message) override
    {
        UnderlyingHandler->OnResponse(MoveRV(message));
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
    : UnderlyingChannel(MoveRV(underlyingChannel))
    , QueueSize(0)
{ }

TNullable<TDuration> TSerializedChannel::GetDefaultTimeout() const
{
    return UnderlyingChannel->GetDefaultTimeout();
}

bool TSerializedChannel::GetRetryEnabled() const
{
    return UnderlyingChannel->GetRetryEnabled();
}

void TSerializedChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout)
{
    auto deadline = timeout ? MakeNullable(timeout.Get().ToDeadLine()) : Null;
    auto entry = New<TEntry>(request, responseHandler, deadline);
    Queue.Enqueue(entry);

    if (timeout) {
        TDelayedInvoker::Submit(
            BIND(&TSerializedChannel::OnQueuedRequestTimeout, MakeStrong(this), entry),
            timeout.Get());
    }

    if (AtomicIncrement(QueueSize) == 1) {
        SendQueuedRequest();
    }
}

void TSerializedChannel::Terminate(const TError& error)
{
    UNUSED(error);
    YUNREACHABLE();
}

void TSerializedChannel::SendQueuedRequest()
{
    auto now = TInstant::Now();

    TEntryPtr entry;
    if (!Queue.Dequeue(&entry))
        return;

    AtomicDecrement(QueueSize);

    if ((!entry->Deadline || entry->Deadline.Get() > now) &&
        AtomicCas(&entry->State, EEntryState::Sent, EEntryState::Waiting))
    {
        auto timeout = entry->Deadline ? MakeNullable(entry->Deadline.Get() - now) : Null;
        auto serializedHandler = New<TSerializedResponseHandler>(entry->Handler, this);
        UnderlyingChannel->Send(entry->Request, serializedHandler, timeout);
        entry->Request.Reset();
        entry->Handler.Reset();
    }
}

void TSerializedChannel::OnQueuedRequestTimeout(TEntryPtr entry)
{
    if (AtomicCas(&entry->State, EEntryState::Waiting, EEntryState::TimedOut)) {
        entry->Handler->OnError(TError(
            EErrorCode::Timeout,
            "Request timed out while waiting in serialized channel queue"));
        entry->Request.Reset();
        entry->Handler.Reset();
    }
}

void TSerializedChannel::OnRequestCompleted()
{
    SendQueuedRequest();
}

} // namespace

IChannelPtr CreateSerializedChannel(IChannelPtr underlyingChannel)
{
    YCHECK(underlyingChannel);

    return New<TSerializedChannel>(underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

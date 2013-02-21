#include "stdafx.h"
#include "scoped_channel.h"
#include "client.h"

#include <queue>

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
        (InProgress)
        (TimedOut)
    );

    struct TEntry
        : public TIntrinsicRefCounted
    {
        TEntry(IClientRequestPtr request, IClientResponseHandlerPtr handler, TNullable<TInstant> deadline)
            : EnqueueTime(TInstant::Now())
            , State(EEntryState::Waiting)
            , Request(std::move(request))
            , Handler(std::move(handler))
            , Deadline(deadline)
        { }

        TInstant EnqueueTime;
        EEntryState State;
        IClientRequestPtr Request;
        IClientResponseHandlerPtr Handler;
        TNullable<TInstant> Deadline;
    };

    typedef TIntrusivePtr<TEntry> TEntryPtr;

    TSpinLock SpinLock;
    std::queue<TEntryPtr> Queue;
    bool RequestInProgress;

    void TrySendQueuedRequests();
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
        : UnderlyingHandler(std::move(underlyingHandler))
        , Channel(std::move(channel))
    { }

    virtual void OnAcknowledgement() override
    {
        UnderlyingHandler->OnAcknowledgement();
    }

    virtual void OnResponse(NBus::IMessagePtr message) override
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

    if (timeout) {
        TDelayedInvoker::Submit(
            BIND(&TSerializedChannel::OnQueuedRequestTimeout, MakeStrong(this), entry),
            timeout.Get());
    }

    {
        TGuard<TSpinLock> guard(SpinLock);
        Queue.push(entry);
    }

    TrySendQueuedRequests();
}

void TSerializedChannel::Terminate(const TError& error)
{
    UNUSED(error);
    YUNREACHABLE();
}

void TSerializedChannel::TrySendQueuedRequests()
{
    auto now = TInstant::Now();

    TGuard<TSpinLock> guard(SpinLock);
    while (!RequestInProgress && !Queue.empty()) {
        auto entry = Queue.front();
        Queue.pop();

        if ((!entry->Deadline || entry->Deadline.Get() > now) && entry->State == EEntryState::Waiting) {
            entry->State = EEntryState::InProgress;
            RequestInProgress = true;
            guard.Release();

            auto timeout = entry->Deadline ? MakeNullable(entry->Deadline.Get() - now) : Null;
            auto serializedHandler = New<TSerializedResponseHandler>(entry->Handler, this);
            UnderlyingChannel->Send(entry->Request, serializedHandler, timeout);
            entry->Request.Reset();
            entry->Handler.Reset();

            break;
        }
    }
}

void TSerializedChannel::OnQueuedRequestTimeout(TEntryPtr entry)
{
    TGuard<TSpinLock> guard(SpinLock);
    
    if (entry->State != EEntryState::Waiting)
        return;

    entry->State = EEntryState::TimedOut;
   
    guard.Release();

    entry->Handler->OnError(TError(
        EErrorCode::Timeout,
        "Request timed out while waiting in serialized channel queue"));
    entry->Request.Reset();
    entry->Handler.Reset();
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

} // namespace

IChannelPtr CreateSerializedChannel(IChannelPtr underlyingChannel)
{
    YCHECK(underlyingChannel);

    return New<TSerializedChannel>(underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

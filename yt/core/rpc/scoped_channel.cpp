#include "stdafx.h"
#include "scoped_channel.h"
#include "client.h"

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TScopedChannel
    : public IChannel
{
public:
    explicit TScopedChannel(IChannelPtr underlyingChannel);

    TNullable<TDuration> GetDefaultTimeout() const override;

    void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override;

    TFuture<void> Terminate(const TError& error) override;

    void OnRequestCompleted();

private:
    IChannelPtr UnderlyingChannel;

    TSpinLock SpinLock;
    bool Terminated;
    TError TerminationError;
    int OutstandingRequestCount;
    TPromise<void> OutstandingRequestsCompleted;

};

typedef TIntrusivePtr<TScopedChannel> TScopedChannelPtr;

class TScopedResponseHandler
    : public IClientResponseHandler
{
public:
    TScopedResponseHandler(
        IClientResponseHandlerPtr underlyingHandler,
        TScopedChannelPtr channel)
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
    TScopedChannelPtr Channel;

};

TScopedChannel::TScopedChannel(IChannelPtr underlyingChannel)
    : UnderlyingChannel(std::move(underlyingChannel))
    , Terminated(false)
    , OutstandingRequestCount(0)
    , OutstandingRequestsCompleted(NewPromise())
{ }

TNullable<TDuration> TScopedChannel::GetDefaultTimeout() const
{
    return UnderlyingChannel->GetDefaultTimeout();
}

void TScopedChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout)
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (Terminated) {
            guard.Release();
            responseHandler->OnError(TerminationError);
            return;
        }
        ++OutstandingRequestCount;
    }
    auto scopedHandler = New<TScopedResponseHandler>(std::move(responseHandler), this);
    UnderlyingChannel->Send(request, std::move(scopedHandler), timeout);
}

TFuture<void> TScopedChannel::Terminate(const TError& error)
{
    TGuard<TSpinLock> guard(SpinLock);
    
    if (!Terminated) {
        Terminated = true;
        TerminationError = error;
    }

    if (OutstandingRequestCount == 0) {
        return MakeFuture();
    }

    return OutstandingRequestsCompleted;
}

void TScopedChannel::OnRequestCompleted()
{
    TGuard<TSpinLock> guard(SpinLock);
    --OutstandingRequestCount;
    if (Terminated && OutstandingRequestCount == 0) {
        guard.Release();
        OutstandingRequestsCompleted.Set();
    }
}

} // anonymous namespace

IChannelPtr CreateScopedChannel(IChannelPtr underlyingChannel)
{
    YCHECK(underlyingChannel);

    return New<TScopedChannel>(underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#include "stdafx.h"
#include "roaming_channel.h"
#include "client.h"

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

class TRoamingChannel
    : public IChannel
{
public:
    explicit TRoamingChannel(TChannelProducer producer)
        : Producer(std::move(producer))
        , Terminated(false)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout;
    }

    void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout = timeout;
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        YASSERT(request);
        YASSERT(responseHandler);

        TPromise< TErrorOr<IChannelPtr> > channelPromise;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Terminated) {
                guard.Release();
                responseHandler->OnError(TError(EErrorCode::TransportError, "Channel terminated"));
                return;
            }

            channelPromise = ChannelPromise;
            if (!channelPromise) {
                channelPromise = ChannelPromise = NewPromise< TErrorOr<IChannelPtr> >();
                guard.Release();

                Producer.Run().Subscribe(BIND(
                    &TRoamingChannel::OnEndpointDiscovered,
                    MakeStrong(this),
                    channelPromise));
            }
        }

        channelPromise.Subscribe(BIND(
            &TRoamingChannel::OnGotChannel,
            MakeStrong(this),
            request,
            responseHandler,
            timeout,
            requestAck));
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        YCHECK(!error.IsOK());

        TNullable< TErrorOr<IChannelPtr> > channel;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Terminated) {
                return MakeFuture();
            }

            channel = ChannelPromise ? ChannelPromise.TryGet() : Null;
            ChannelPromise.Reset();
            TerminationError = error;
            Terminated = true;
        }

        if (channel && channel->IsOK()) {
            return channel->GetValue()->Terminate(error);
        }
        return MakeFuture();
    }

private:
    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IClientResponseHandlerPtr underlyingHandler,
            TClosure onFailed)
            : UnderlyingHandler(underlyingHandler)
            , OnFailed(onFailed)
        { }

        virtual void OnAcknowledgement() override
        {
            UnderlyingHandler->OnAcknowledgement();
        }

        virtual void OnResponse(TSharedRefArray message) override
        {
            UnderlyingHandler->OnResponse(message);
        }

        virtual void OnError(const TError& error) override
        {
            UnderlyingHandler->OnError(error);
            if (IsRetriableError(error)) {
                OnFailed.Run();
            }
        }

    private:
        IClientResponseHandlerPtr UnderlyingHandler;
        TClosure OnFailed;

    };


    void OnEndpointDiscovered(
        TPromise< TErrorOr<IChannelPtr> > channelPromise,
        TErrorOr<IChannelPtr> result)
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (Terminated) {
            guard.Release();
            if (result.IsOK()) {
	            auto channel = result.GetValue();
                channel->Terminate(TerminationError);
            }
            return;
        }

        if (ChannelPromise == channelPromise && !result.IsOK()) {
            ChannelPromise.Reset();
        }

        guard.Release();
        channelPromise.Set(result);
    }

    void OnGotChannel(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck,
        TErrorOr<IChannelPtr> result)
    {
        if (!result.IsOK()) {
            responseHandler->OnError(result);
        } else {
            auto channel = result.GetValue();
            auto responseHandlerWrapper = New<TResponseHandler>(
                responseHandler,
                BIND(&TRoamingChannel::OnChannelFailed, MakeStrong(this), channel));
            channel->Send(
                request,
                responseHandlerWrapper,
                timeout,
                requestAck);
        }
    }

    void OnChannelFailed(IChannelPtr failedChannel)
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (ChannelPromise) {
            auto currentChannel = ChannelPromise.TryGet();
            if (currentChannel && currentChannel->IsOK() && currentChannel->GetValue() == failedChannel) {
                ChannelPromise.Reset();
            }
        }
    }


    TNullable<TDuration> DefaultTimeout;
    TChannelProducer Producer;

    TSpinLock SpinLock;
    volatile bool Terminated;
    TError TerminationError;
    TPromise< TErrorOr<IChannelPtr> > ChannelPromise;

};

IChannelPtr CreateRoamingChannel(TChannelProducer producer)
{
    return New<TRoamingChannel>(producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

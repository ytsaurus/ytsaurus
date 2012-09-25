#include "stdafx.h"
#include "roaming_channel.h"
#include "client.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

class TRoamingChannel
    : public IChannel
{
public:
    TRoamingChannel(
        TNullable<TDuration> defaultTimeout,
        bool retryEnabled,
        TChannelProducer producer)
        : DefaultTimeout(defaultTimeout)
        , RetryEnabled(retryEnabled)
        , Producer(producer)
        , Terminated(false)
        , ChannelPromise(Null)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout;
    }

    virtual bool GetRetryEnabled() const
    {
        return RetryEnabled;
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override
    {
        YASSERT(request);
        YASSERT(responseHandler);

        TPromise< TValueOrError<IChannelPtr> > channelPromise(Null);
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Terminated) {
                guard.Release();
                responseHandler->OnError(TError(EErrorCode::TransportError, "Channel terminated"));
                return;
            }

            channelPromise = ChannelPromise;
            if (channelPromise.IsNull()) {
                channelPromise = ChannelPromise = NewPromise< TValueOrError<IChannelPtr> >();
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
            timeout));
    }

    virtual void Terminate(const TError& error) override
    {
        YCHECK(!error.IsOK());

        TNullable< TValueOrError<IChannelPtr> > channel;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Terminated) {
                return;
            }

            channel = ChannelPromise.IsNull() ? Null : ChannelPromise.TryGet();
            ChannelPromise.Reset();
            TerminationError = error;
            Terminated = true;
        }
    
        if (channel && channel->IsOK()) {
            channel->Value()->Terminate(error);
        }
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

        virtual void OnResponse(IMessagePtr message) override
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
        TPromise< TValueOrError<IChannelPtr> > channelPromise,
        TValueOrError<IChannelPtr> result)
    {
        TGuard<TSpinLock> guard(SpinLock);
        
        if (Terminated) {
            guard.Release();
            if (result.IsOK()) {
                result.Value()->Terminate(TerminationError);
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
        TValueOrError<IChannelPtr> result)
    {
        if (!result.IsOK()) {
            responseHandler->OnError(result);
        } else {
            auto channel = result.Value();
            auto responseHandlerWrapper = New<TResponseHandler>(
                responseHandler,
                BIND(&TRoamingChannel::OnChannelFailed, MakeStrong(this), channel));
            channel->Send(request, responseHandlerWrapper, timeout);
        }
    }

    void OnChannelFailed(IChannelPtr failedChannel)
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (!ChannelPromise.IsNull()) {
            auto currentChannel = ChannelPromise.TryGet();
            if (currentChannel && currentChannel->IsOK() && currentChannel->Value() == failedChannel) {
                ChannelPromise.Reset();
            }
        }
    }


    TNullable<TDuration> DefaultTimeout;
    bool RetryEnabled;
    TChannelProducer Producer;

    TSpinLock SpinLock;
    volatile bool Terminated;
    TError TerminationError;
    TPromise< TValueOrError<IChannelPtr> > ChannelPromise;

};

IChannelPtr CreateRoamingChannel(
    TNullable<TDuration> defaultTimeout,
    bool retryEnabled,
    TChannelProducer producer)
{
    return New<TRoamingChannel>(
        defaultTimeout,
        retryEnabled,
        producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

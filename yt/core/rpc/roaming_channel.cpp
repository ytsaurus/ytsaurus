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
        : Producer_(std::move(producer))
        , Terminated_(false)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout_;
    }

    void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout_ = timeout;
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

            if (Terminated_) {
                guard.Release();
                responseHandler->OnError(TError(EErrorCode::TransportError, "Channel terminated"));
                return;
            }

            channelPromise = ChannelPromise;
            if (!channelPromise) {
                channelPromise = ChannelPromise = NewPromise< TErrorOr<IChannelPtr> >();
                guard.Release();

                Producer_.Run(request).Subscribe(BIND(
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

            if (Terminated_) {
                return MakeFuture();
            }

            channel = ChannelPromise ? ChannelPromise.TryGet() : Null;
            ChannelPromise.Reset();
            TerminationError = error;
            Terminated_ = true;
        }

        if (channel && channel->IsOK()) {
            return channel->Value()->Terminate(error);
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

        if (Terminated_) {
            guard.Release();
            if (result.IsOK()) {
                auto channel = result.Value();
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
            auto channel = result.Value();
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
            if (currentChannel && currentChannel->IsOK() && currentChannel->Value() == failedChannel) {
                ChannelPromise.Reset();
            }
        }
    }


    TNullable<TDuration> DefaultTimeout_;
    TChannelProducer Producer_;

    TSpinLock SpinLock;
    volatile bool Terminated_;
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

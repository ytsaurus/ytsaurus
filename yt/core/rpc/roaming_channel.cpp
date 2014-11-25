#include "stdafx.h"
#include "roaming_channel.h"
#include "client.h"

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TRoamingChannel
    : public IChannel
{
public:
    explicit TRoamingChannel(
        IRoamingChannelProviderPtr provider,
        TCallback<bool(const TError&)> isChannelFailureError)
        : Provider_(std::move(provider))
        , IsChannelFailureError_(isChannelFailureError)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout_;
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout_ = timeout;
    }

    virtual TYsonString GetEndpointDescription() const override
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (ChannelPromise_ && ChannelPromise_.IsSet() && ChannelPromise_.Get().IsOK()) {
            return ChannelPromise_.Get().Value()->GetEndpointDescription();
        } else {
            return Provider_->GetEndpointDescription();
        }
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
                responseHandler->OnError(TError(NRpc::EErrorCode::TransportError, "Channel terminated"));
                return;
            }

            channelPromise = ChannelPromise_;
            if (!channelPromise) {
                channelPromise = ChannelPromise_ = NewPromise< TErrorOr<IChannelPtr> >();
                guard.Release();

                Provider_->DiscoverChannel(request).Subscribe(BIND(
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
                return VoidFuture;
            }

            channel = ChannelPromise_ ? ChannelPromise_.TryGet() : Null;
            ChannelPromise_.Reset();
            TerminationError_ = error;
            Terminated_ = true;
        }

        if (channel && channel->IsOK()) {
            return channel->Value()->Terminate(error);
        }

        return VoidFuture;
    }

private:
    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IClientResponseHandlerPtr underlyingHandler,
            TClosure onFailed,
            TCallback<bool(const TError&)> isChannelFailureError)
            : UnderlyingHandler_(underlyingHandler)
            , OnFailed_(onFailed)
            , IsChannelFailureError_(isChannelFailureError)
        { }

        virtual void OnAcknowledgement() override
        {
            UnderlyingHandler_->OnAcknowledgement();
        }

        virtual void OnResponse(TSharedRefArray message) override
        {
            UnderlyingHandler_->OnResponse(message);
        }

        virtual void OnError(const TError& error) override
        {
            UnderlyingHandler_->OnError(error);
            if (IsChannelFailureError_.Run(error)) {
                OnFailed_.Run();
            }
        }

    private:
        IClientResponseHandlerPtr UnderlyingHandler_;
        TClosure OnFailed_;
        TCallback<bool(const TError&)> IsChannelFailureError_;
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
                channel->Terminate(TerminationError_);
            }
            return;
        }

        if (ChannelPromise_ == channelPromise && !result.IsOK()) {
            ChannelPromise_.Reset();
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
                BIND(&TRoamingChannel::OnChannelFailed, MakeStrong(this), channel),
                IsChannelFailureError_);
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

        if (ChannelPromise_) {
            auto currentChannel = ChannelPromise_.TryGet();
            if (currentChannel && currentChannel->IsOK() && currentChannel->Value() == failedChannel) {
                ChannelPromise_.Reset();
            }
        }
    }


    TNullable<TDuration> DefaultTimeout_;
    IRoamingChannelProviderPtr Provider_;

    TSpinLock SpinLock;
    volatile bool Terminated_ = false;
    TError TerminationError_;
    TPromise<TErrorOr<IChannelPtr>> ChannelPromise_;

    TCallback<bool(const TError&)> IsChannelFailureError_;
};

IChannelPtr CreateRoamingChannel(
    IRoamingChannelProviderPtr producer,
    TCallback<bool(const TError&)> isChannelFailureError)
{
    return New<TRoamingChannel>(producer, isChannelFailureError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

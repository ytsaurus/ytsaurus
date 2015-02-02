#include "stdafx.h"
#include "roaming_channel.h"
#include "client.h"
#include "channel_detail.h"

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

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        YASSERT(request);
        YASSERT(responseHandler);

        TPromise<IChannelPtr> channelPromise;
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (Terminated_) {
                guard.Release();
                responseHandler->HandleError(TError(NRpc::EErrorCode::TransportError, "Channel terminated"));
                return nullptr;
            }

            channelPromise = ChannelPromise_;
            if (!channelPromise) {
                channelPromise = ChannelPromise_ = NewPromise<IChannelPtr>();
                guard.Release();

                Provider_->DiscoverChannel(request).Subscribe(BIND(
                    &TRoamingChannel::OnEndpointDiscovered,
                    MakeStrong(this),
                    channelPromise));
            }
        }

        auto requestControlThunk = New<TClientRequestControlThunk>();

        channelPromise.ToFuture().Subscribe(BIND(
            &TRoamingChannel::OnGotChannel,
            MakeStrong(this),
            request,
            responseHandler,
            timeout,
            requestAck,
            requestControlThunk));

        return requestControlThunk;
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        YCHECK(!error.IsOK());

        TNullable<TErrorOr<IChannelPtr>> channel;
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

        virtual void HandleAcknowledgement() override
        {
            UnderlyingHandler_->HandleAcknowledgement();
        }

        virtual void HandleResponse(TSharedRefArray message) override
        {
            UnderlyingHandler_->HandleResponse(message);
        }

        virtual void HandleError(const TError& error) override
        {
            UnderlyingHandler_->HandleError(error);
            if (IsChannelFailureError_.Run(error)) {
                OnFailed_.Run();
            }
        }

    private:
        const IClientResponseHandlerPtr UnderlyingHandler_;
        const TClosure OnFailed_;
        const TCallback<bool(const TError&)> IsChannelFailureError_;

    };


    void OnEndpointDiscovered(
        TPromise<IChannelPtr> channelPromise,
        const TErrorOr<IChannelPtr>& result)
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
        TClientRequestControlThunkPtr requestControlThunk,
        const TErrorOr<IChannelPtr>& result)
    {
        if (!result.IsOK()) {
            responseHandler->HandleError(result);
        } else {
            auto channel = result.Value();
            auto responseHandlerWrapper = New<TResponseHandler>(
                responseHandler,
                BIND(&TRoamingChannel::OnChannelFailed, MakeStrong(this), channel),
                IsChannelFailureError_);
            auto requestControl = channel->Send(
                request,
                responseHandlerWrapper,
                timeout,
                requestAck);
            requestControlThunk->SetUnderlying(std::move(requestControl));
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


    const IRoamingChannelProviderPtr Provider_;
    const TCallback<bool(const TError&)> IsChannelFailureError_;

    TNullable<TDuration> DefaultTimeout_;

    TSpinLock SpinLock;
    volatile bool Terminated_ = false;
    TError TerminationError_;
    TPromise<IChannelPtr> ChannelPromise_;

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

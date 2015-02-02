#include "stdafx.h"
#include "scoped_channel.h"
#include "channel_detail.h"
#include "channel_detail.h"
#include "client.h"

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TScopedChannel;
typedef TIntrusivePtr<TScopedChannel> TScopedChannelPtr;

class TScopedChannel
    : public TChannelWrapper
{
public:
    explicit TScopedChannel(IChannelPtr underlyingChannel)
        : TChannelWrapper(std::move(underlyingChannel))
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Terminated_) {
                guard.Release();
                responseHandler->HandleError(TerminationError_);
                return nullptr;
            }
            ++OutstandingRequestCount_;
        }
        auto scopedHandler = New<TResponseHandler>(std::move(responseHandler), this);
        return UnderlyingChannel_->Send(
            std::move(request),
            std::move(scopedHandler),
            timeout,
            requestAck);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        TGuard<TSpinLock> guard(SpinLock_);
    
        if (!Terminated_) {
            Terminated_ = true;
            TerminationError_ = error;
        }

        if (OutstandingRequestCount_ == 0) {
            return VoidFuture;
        }

        return OutstandingRequestsCompleted_;
    }

    void OnRequestCompleted()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        --OutstandingRequestCount_;
        if (Terminated_ && OutstandingRequestCount_ == 0) {
            guard.Release();
            OutstandingRequestsCompleted_.Set();
        }
    }

private:
    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IClientResponseHandlerPtr underlyingHandler,
            TScopedChannelPtr channel)
            : UnderlyingHandler_(std::move(underlyingHandler))
            , Owner_(std::move(channel))
        { }

        virtual void HandleAcknowledgement() override
        {
            UnderlyingHandler_->HandleAcknowledgement();
        }

        virtual void HandleResponse(TSharedRefArray message) override
        {
            UnderlyingHandler_->HandleResponse(std::move(message));
            Owner_->OnRequestCompleted();
        }

        virtual void HandleError(const TError& error) override
        {
            UnderlyingHandler_->HandleError(error);
            Owner_->OnRequestCompleted();
        }

    private:
        IClientResponseHandlerPtr UnderlyingHandler_;
        TScopedChannelPtr Owner_;

    };

    TSpinLock SpinLock_;
    bool Terminated_ = false;
    TError TerminationError_;
    int OutstandingRequestCount_ = 0;
    TPromise<void> OutstandingRequestsCompleted_ = NewPromise<void>();

};

IChannelPtr CreateScopedChannel(IChannelPtr underlyingChannel)
{
    YCHECK(underlyingChannel);

    return New<TScopedChannel>(underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#include "stdafx.h"
#include "scoped_channel.h"
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

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Terminated_) {
                guard.Release();
                responseHandler->OnError(TerminationError_);
                return;
            }
            ++OutstandingRequestCount_;
        }
        auto scopedHandler = New<TResponseHandler>(std::move(responseHandler), this);
        UnderlyingChannel_->Send(
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

        virtual void OnAcknowledgement() override
        {
            UnderlyingHandler_->OnAcknowledgement();
        }

        virtual void OnResponse(TSharedRefArray message) override
        {
            UnderlyingHandler_->OnResponse(std::move(message));
            Owner_->OnRequestCompleted();
        }

        virtual void OnError(const TError& error) override
        {
            UnderlyingHandler_->OnError(error);
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

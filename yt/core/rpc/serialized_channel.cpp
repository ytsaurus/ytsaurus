#include "stdafx.h"
#include "serialized_channel.h"
#include "channel_detail.h"
#include "client.h"

#include <queue>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TSerializedChannel;
typedef TIntrusivePtr<TSerializedChannel> TSerializedChannelPtr;

class TSerializedChannel
    : public TChannelWrapper
{
public:
    explicit TSerializedChannel(IChannelPtr underlyingChannel)
        : TChannelWrapper(std::move(underlyingChannel))
    { }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        auto entry = New<TEntry>(
            request,
            responseHandler,
            timeout,
            requestAck);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            Queue_.push(entry);
        }

        TrySendQueuedRequests();
    }

    virtual TFuture<void> Terminate(const TError& /*error*/) override
    {
        YUNREACHABLE();
    }

    void OnRequestCompleted()
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(RequestInProgress_);
            RequestInProgress_ = false;
        }

        TrySendQueuedRequests();
    }

private:
    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IClientResponseHandlerPtr underlyingHandler,
            TSerializedChannelPtr owner)
            : UnderlyingHandler_(std::move(underlyingHandler))
            , Owner_(std::move(owner))
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
        TSerializedChannelPtr Owner_;

    };

    struct TEntry
        : public TIntrinsicRefCounted
    {
        TEntry(
            IClientRequestPtr request,
            IClientResponseHandlerPtr handler,
            TNullable<TDuration> timeout,
            bool requestAck)
            : Request(std::move(request))
            , Handler(std::move(handler))
            , Timeout(timeout)
            , RequestAck(requestAck)
        { }

        IClientRequestPtr Request;
        IClientResponseHandlerPtr Handler;
        TNullable<TDuration> Timeout;
        bool RequestAck;
    };

    typedef TIntrusivePtr<TEntry> TEntryPtr;

    TSpinLock SpinLock_;
    std::queue<TEntryPtr> Queue_;
    bool RequestInProgress_ = false;


    void TrySendQueuedRequests()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        while (!RequestInProgress_ && !Queue_.empty()) {
            auto entry = Queue_.front();
            Queue_.pop();
            RequestInProgress_ = true;
            guard.Release();

            auto serializedHandler = New<TResponseHandler>(entry->Handler, this);
            UnderlyingChannel_->Send(
                entry->Request,
                serializedHandler,
                entry->Timeout,
                entry->RequestAck);
            entry->Request.Reset();
            entry->Handler.Reset();
        }
    }

};

IChannelPtr CreateSerializedChannel(IChannelPtr underlyingChannel)
{
    YCHECK(underlyingChannel);

    return New<TSerializedChannel>(std::move(underlyingChannel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

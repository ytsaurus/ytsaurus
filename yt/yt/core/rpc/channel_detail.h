#pragma once

#include "channel.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TChannelWrapper
    : public virtual IChannel
{
public:
    explicit TChannelWrapper(IChannelPtr underlyingChannel);

    const TString& GetEndpointDescription() const override;
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    TNetworkId GetNetworkId() const override;

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override;

    void Terminate(const TError& error) override;

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override;
    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

protected:
    const IChannelPtr UnderlyingChannel_;
};

DEFINE_REFCOUNTED_TYPE(TChannelWrapper)

////////////////////////////////////////////////////////////////////////////////

class TClientRequestControlThunk
    : public IClientRequestControl
{
public:
    void SetUnderlying(IClientRequestControlPtr underlying);

    void Cancel() override;

    TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override;
    TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override;

private:
    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);

    bool Canceled_ = false;

    struct TPendingStreamingPayload
    {
        TStreamingPayload Payload;
        TPromise<void> Promise;
    };
    std::vector<TPendingStreamingPayload> PendingStreamingPayloads_;

    struct TPendingStreamingFeedback
    {
        TStreamingFeedback Feedback{-1};
        TPromise<void> Promise;
    };
    TPendingStreamingFeedback PendingStreamingFeedback_;

    bool UnderlyingCanceled_ = false;

    IClientRequestControlPtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TClientRequestControlThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

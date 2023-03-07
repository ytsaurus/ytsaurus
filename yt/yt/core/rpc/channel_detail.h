#pragma once

#include "channel.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TChannelWrapper
    : public IChannel
{
public:
    explicit TChannelWrapper(IChannelPtr underlyingChannel);

    virtual const TString& GetEndpointDescription() const override;
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;
    virtual TNetworkId GetNetworkId() const override;

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override;

    virtual void Terminate(const TError& error) override;

    virtual void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override;
    virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

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

    virtual void Cancel() override;

    virtual TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override;
    virtual TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override;

private:
    TSpinLock SpinLock_;

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

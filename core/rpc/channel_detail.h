#pragma once

#include "channel.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TChannelWrapper
    : public IChannel
{
public:
    explicit TChannelWrapper(IChannelPtr underlyingChannel);

    virtual const TString& GetEndpointDescription() const override;
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override;

    virtual TFuture<void> Terminate(const TError& error) override;

protected:
    const IChannelPtr UnderlyingChannel_;

};

DEFINE_REFCOUNTED_TYPE(TChannelWrapper)

////////////////////////////////////////////////////////////////////////////////

class TClientRequestControlThunk
    : public IClientRequestControl
{
public:
    void SetUnderlying(IClientRequestControlPtr underlyingControl);

    virtual void Cancel() override;

private:
    TSpinLock SpinLock_;
    bool Canceled_ = false;
    bool UnderlyingCanceled_ = false;
    IClientRequestControlPtr Underlying_;


    void PropagateCancel(TGuard<TSpinLock>& guard);

};

DEFINE_REFCOUNTED_TYPE(TClientRequestControlThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

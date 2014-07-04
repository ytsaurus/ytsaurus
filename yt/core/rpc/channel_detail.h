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

    virtual TNullable<TDuration> GetDefaultTimeout() const override;
    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override;

    virtual NYTree::TYsonString GetEndpointDescription() const override;

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override;

    virtual TFuture<void> Terminate(const TError& error) override;

protected:
    IChannelPtr UnderlyingChannel_;
    TNullable<TDuration> DefaultTimeout_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

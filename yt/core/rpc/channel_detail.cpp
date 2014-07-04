#include "stdafx.h"
#include "channel_detail.h"

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

TChannelWrapper::TChannelWrapper(IChannelPtr underlyingChannel)
    : UnderlyingChannel_(std::move(underlyingChannel))
{
    YASSERT(UnderlyingChannel_);
}

TNullable<TDuration> TChannelWrapper::GetDefaultTimeout() const
{
    return DefaultTimeout_;
}

void TChannelWrapper::SetDefaultTimeout(const TNullable<TDuration>& timeout)
{
    DefaultTimeout_ = timeout;
}

NYTree::TYsonString TChannelWrapper::GetEndpointDescription() const
{
    return UnderlyingChannel_->GetEndpointDescription();
}

void TChannelWrapper::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout,
    bool requestAck)
{
    UnderlyingChannel_->Send(
        std::move(request),
        std::move(responseHandler),
        timeout ? timeout : DefaultTimeout_,
        requestAck);
}

TFuture<void> TChannelWrapper::Terminate(const TError& error)
{
    return UnderlyingChannel_->Terminate(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

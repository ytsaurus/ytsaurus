#pragma once

#include "common.h"
#include "channel.h"
#include "client.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Implements simple retry politic. If remote call fails on the RPC 
//! level, it is retried #RetryCount times with given backoff time.
//! If request finally fails, the EErrorCode::Unavailable is returned.
class TRetriableChannel
    : public IChannel
{
public:
    typedef TIntrusivePtr<TRetriableChannel> TPtr;

    TRetriableChannel(
        IChannel::TPtr channel, 
        TDuration backoffTime, 
        int retryCount);

    TFuture<EErrorCode>::TPtr Send(
        IClientRequest::TPtr request, 
        IClientResponseHandler::TPtr responseHandler, 
        TDuration timeout);

    void Terminate();

    IChannel::TPtr GetChannel();
    int GetRetryCount() const;
    TDuration GetBackoffTime() const;

private:
    IChannel::TPtr Channel;
    const int RetryCount;
    const TDuration BackoffTime;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
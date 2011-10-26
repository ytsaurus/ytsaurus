#pragma once

#include "common.h"
#include "channel.h"
#include "client.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TRetriableChannel
    : public IChannel
{
public:
    TRetriableChannel(
        IChannel::TPtr channel, 
        TDuration backoffTime, 
        int retryCount);

    TFuture<EErrorCode>::TPtr Send(
        TClientRequest::TPtr request, 
        IClientResponseHandler::TPtr responseHandler, 
        TDuration timeout);

    void Terminate();

private:
    IChannel::TPtr Channel;
    const int RetryCount;
    const TDuration BackoffTime;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
#pragma once

#include "common.h"
#include "channel.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TRetryConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TRetryConfig> TPtr;
    
    TDuration BackoffTime;
    int RetryCount;

    TRetryConfig()
    {
        Register("backoff_time", BackoffTime)
            .Default(TDuration::Seconds(5));
        Register("retry_count", RetryCount)
            .GreaterThanOrEqual(1)
            .Default(3);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a channel that implements a simple retry policy.
/*!
 *  If a remote call fails at RPC level (see #NRpc::EErrorCode::IsRpcError)
 *  it is retried a given number of times with a given back off time.
 *  
 *  If the request is still failing, then EErrorCode::Unavailable is returned.
 *  
 *  \param underlyingChannel An underlying channel.
 *  \param backoffTime A interval between successive attempts.
 *  \param retryCount Maximum number of retry attempts.
 *  \returns A retriable channel.
 */ 
IChannelPtr CreateRetriableChannel(
    TRetryConfig* config,
    IChannel* underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

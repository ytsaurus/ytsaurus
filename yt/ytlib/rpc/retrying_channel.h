#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TRetryConfig
    : public TYsonSerializable
{
    TDuration BackoffTime;
    int MaxAttempts;

    TRetryConfig()
    {
        Register("backoff_time", BackoffTime)
            .Default(TDuration::Seconds(3));
        Register("max_attempts", MaxAttempts)
            .GreaterThanOrEqual(1)
            .Default(10);
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
 *  \returns The retrying channel.
 */ 
IChannelPtr CreateRetryingChannel(
    TRetryConfigPtr config,
    IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

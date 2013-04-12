#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannelConfig
    : public TYsonSerializable
{
public:
    TDuration BackoffTime;
    int MaxAttempts;

    TRetryingChannelConfig()
    {
        RegisterParameter("backoff_time", BackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("max_attempts", MaxAttempts)
            .GreaterThanOrEqual(1)
            .Default(10);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a channel that implements a simple retry policy.
/*!
 *  If a request fails with a retriable error (see #NRpc::IsRetriableError),
 *  it is retried a given number of times with a given back off time.
 *
 *  If the request is still failing, then EErrorCode::Unavailable is returned.
 *
 *  If number of retry attemps is one then the underlying channel is returned.
 */
IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

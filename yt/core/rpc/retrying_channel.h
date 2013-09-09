#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannelConfig
    : public TYsonSerializable
{
public:
    //! Time to wait between consequent attempts.
    TDuration RetryBackoffTime;

    //! Maximum number of retry attempts to make.
    int RetryAttempts;

    //! Maximum time to spend while retrying.
    //! If |Null| then no limit is enforced.
    TNullable<TDuration> RetryTimeout;

    TRetryingChannelConfig()
    {
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("retry_attempts", RetryAttempts)
            .GreaterThanOrEqual(1)
            .Default(10);
        RegisterParameter("retry_timeout", RetryTimeout)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a channel that implements a simple retry policy.
/*!
 *  The channel determines if the request must be retried by calling
 *  #NRpc::IsRetriableError.
 *  
 *  The channel makes at most #TRetryingChannelConfig::RetryAttempts
 *  attempts totally spending at most #TRetryingChannelConfig::RetryTimeout time
 *  (if given).
 *  
 *  A delay of #TRetryingChannelConfig::RetryBackoffTime is inserted
 *  between any pair of consequent attempts.
 */
IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

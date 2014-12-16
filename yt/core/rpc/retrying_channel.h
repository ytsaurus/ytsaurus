#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

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
    IChannelPtr underlyingChannel,
    TCallback<bool(const TError&)> isRetriableError = BIND(&IsRetriableError));

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

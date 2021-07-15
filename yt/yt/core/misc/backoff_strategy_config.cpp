#include "backoff_strategy_config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSerializableExponentialBackoffOptions::TSerializableExponentialBackoffOptions()
{
    RegisterParameter("retry_count", RetryCount)
        .Default(DefaultRetryCount);
    RegisterParameter("min_backoff", MinBackoff)
        .Default(DefaultMinBackoff);
    RegisterParameter("max_backoff", MaxBackoff)
        .Default(DefaultMaxBackoff);
    RegisterParameter("backoff_multiplier", BackoffMultiplier)
        .Default(DefaultBackoffMultiplier);
    RegisterParameter("backoff_jitter", BackoffJitter)
        .Default(DefaultBackoffJitter);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableConstantBackoffOptions::TSerializableConstantBackoffOptions()
{
    RegisterParameter("retry_count", RetryCount)
        .Default(DefaultRetryCount);
    RegisterParameter("backoff", Backoff)
        .Default(DefaultBackoff);
    RegisterParameter("backoff_jitter", BackoffJitter)
        .Default(DefaultBackoffJitter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

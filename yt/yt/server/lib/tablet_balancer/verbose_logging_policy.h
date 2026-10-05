#pragma once

#include "public.h"

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVerboseLogThrottling,
    (Enabled)
    (Disabled)
);

////////////////////////////////////////////////////////////////////////////////

template <bool EnableVerboseLoggingValue>
class TVerboseLoggingPolicy
{
public:
    static constexpr bool EnableVerboseLogging = EnableVerboseLoggingValue;

    void ResetLogMessageCount() const;

    bool TryAcquireMessage(EVerboseLogThrottling throttling) const;

private:
    mutable std::atomic<int> LogMessageCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// These macros require TLoggingPolicy and LoggingPolicy_ in the enclosing class.
// Conditions and chained attributes are not evaluated in the non-verbose specialization.
#define YT_TLOG_DEBUG_VERBOSE(throttling, message) \
    if constexpr (!TLoggingPolicy::EnableVerboseLogging) { } else \
        YT_TLOG_DEBUG_IF(LoggingPolicy_.TryAcquireMessage(throttling), message)

#define YT_TLOG_DEBUG_VERBOSE_IF(condition, throttling, message) \
    if constexpr (!TLoggingPolicy::EnableVerboseLogging) { } else \
        YT_TLOG_DEBUG_IF((condition) && LoggingPolicy_.TryAcquireMessage(throttling), message)

#define YT_TLOG_WARNING_VERBOSE_IF(condition, throttling, message) \
    if constexpr (!TLoggingPolicy::EnableVerboseLogging) { } else \
        YT_TLOG_WARNING_IF((condition) && LoggingPolicy_.TryAcquireMessage(throttling), message)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define VERBOSE_LOGGING_POLICY_INL_H_
#include "verbose_logging_policy-inl.h"
#undef VERBOSE_LOGGING_POLICY_INL_H_

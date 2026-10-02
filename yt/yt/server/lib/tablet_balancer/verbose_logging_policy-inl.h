#ifndef VERBOSE_LOGGING_POLICY_INL_H_
#error "Direct inclusion of this file is not allowed, include verbose_logging_policy.h"
// For the sake of sane code completion.
#include "verbose_logging_policy.h"
#endif

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <bool EnableVerboseLoggingValue>
void TVerboseLoggingPolicy<EnableVerboseLoggingValue>::ResetLogMessageCount() const
{
    if constexpr (EnableVerboseLogging) {
        LogMessageCount_.store(0, std::memory_order::relaxed);
    }
}

template <bool EnableVerboseLoggingValue>
bool TVerboseLoggingPolicy<EnableVerboseLoggingValue>::TryAcquireMessage(EVerboseLogThrottling throttling) const
{
    static_assert(EnableVerboseLogging);

    if (throttling == EVerboseLogThrottling::Disabled) {
        return true;
    }

    if (LogMessageCount_.load(std::memory_order::relaxed) >= MaxVerboseLogMessagesPerIteration) {
        return false;
    }

    return LogMessageCount_.fetch_add(1, std::memory_order::relaxed) < MaxVerboseLogMessagesPerIteration;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

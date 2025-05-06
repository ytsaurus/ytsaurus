#include "helpers.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TDuration ComputeForwardingTimeout(
    TDuration originTimeout,
    std::optional<TInstant> startTime,
    TDuration forwardedRequestTimeoutReserve,
    bool* reserved)
{
    auto adjustedTimeout = originTimeout;
    if (startTime) {
        auto elapsedTime = NProfiling::GetInstant() - *startTime;
        if (elapsedTime < originTimeout) {
            adjustedTimeout -= elapsedTime;
        }
    }

    return ComputeForwardingTimeout(adjustedTimeout, forwardedRequestTimeoutReserve, reserved);
}

TDuration ComputeForwardingTimeout(
    TDuration suggestedTimeout,
    TDuration forwardedRequestTimeoutReserve,
    bool* reserved)
{
    if (suggestedTimeout > 2 * forwardedRequestTimeoutReserve) {
        if (reserved) {
            *reserved = true;
        }
        return suggestedTimeout - forwardedRequestTimeoutReserve;
    } else {
        if (reserved) {
            *reserved = false;
        }
        return suggestedTimeout;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

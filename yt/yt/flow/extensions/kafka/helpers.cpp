#include "helpers.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void SleepUnlessTerminated(const std::atomic<bool>& terminated, TDuration duration)
{
    // Short slices, so that termination is not delayed by a whole backoff.
    constexpr auto Slice = TDuration::MilliSeconds(100);
    auto deadline = TInstant::Now() + duration;
    while (!terminated.load() && TInstant::Now() < deadline) {
        Sleep(Min(Slice, deadline - TInstant::Now()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

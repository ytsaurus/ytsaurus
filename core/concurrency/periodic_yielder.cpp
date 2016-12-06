#include "periodic_yielder.h"

#include "scheduler.h"

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPeriodicYielder::TPeriodicYielder(TDuration period)
    : Period_(DurationToCpuDuration(period))
{ }

bool TPeriodicYielder::TryYield()
{
    if (GetCpuInstant() - LastYieldTime_ > Period_) {
        // YT-5601: replace with Yield after merge into prestable/18.
        WaitFor(VoidFuture);
        LastYieldTime_ = GetCpuInstant();
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

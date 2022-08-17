#include "ema_counter.h"

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEmaCounter::TEmaCounter(TWindowDurations windowDurations)
    : WindowDurations(std::move(windowDurations))
    , WindowRates(WindowDurations.size())
{ }

void TEmaCounter::Update(i64 newCount, TInstant newTimestamp)
{
    if (!Timestamp) {
        // Just set current value, we do not know enough information to deal with rates.
        Count = newCount;
        Timestamp = newTimestamp;
        return;
    }

    if (newTimestamp <= *Timestamp) {
        // Ignore obsolete update.
        return;
    }

    auto timeDelta = (newTimestamp - *Timestamp).SecondsFloat();
    i64 countDelta = std::max(Count, newCount) - Count;
    auto newRate = countDelta / timeDelta;

    Count = newCount;
    ImmediateRate = newRate;
    Timestamp = newTimestamp;

    for (int windowIndex = 0; windowIndex < std::ssize(WindowDurations); ++windowIndex) {
        auto exp = std::exp(-timeDelta / (WindowDurations[windowIndex].SecondsFloat() / 2.0));
        auto& rate = WindowRates[windowIndex];
        rate = newRate * (1 - exp) + rate * exp;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

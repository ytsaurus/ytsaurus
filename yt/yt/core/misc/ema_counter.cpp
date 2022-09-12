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

TEmaCounter operator+(const TEmaCounter& lhs, const TEmaCounter& rhs)
{
    TEmaCounter result = lhs;
    result += rhs;
    return result;
}

TEmaCounter& operator+=(TEmaCounter& lhs, const TEmaCounter& rhs)
{
    YT_VERIFY(lhs.WindowDurations == rhs.WindowDurations);
    lhs.Timestamp = std::max(lhs.Timestamp, rhs.Timestamp);
    lhs.Count += rhs.Count;
    lhs.ImmediateRate += rhs.ImmediateRate;
    for (int windowIndex = 0; windowIndex < std::ssize(lhs.WindowDurations); ++windowIndex) {
        lhs.WindowRates[windowIndex] += rhs.WindowRates[windowIndex];
    }
    return lhs;
}

TEmaCounter& operator*=(TEmaCounter& lhs, double coefficient)
{
    lhs.Count *= coefficient;
    lhs.ImmediateRate *= coefficient;
    for (auto& rate : lhs.WindowRates) {
        rate *= coefficient;
    }
    return lhs;
}

TEmaCounter operator*(const TEmaCounter& lhs, double coefficient)
{
    TEmaCounter result = lhs;
    result *= coefficient;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

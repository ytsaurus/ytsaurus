#include "spinlock.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

std::atomic<TSpinlockHiccupHandler> SpinlockHiccupHandler;
// 1M ticks is always worth attention.
std::atomic<i64> SpinlockHiccupThreshold = 1'000'000;

} // namespace

void SetSpinlockHiccupHandler(TSpinlockHiccupHandler handler)
{
    NDetail::SpinlockHiccupHandler.store(handler);
}

void SetSpinlockHiccupThresholdTicks(i64 threshold)
{
    NDetail::SpinlockHiccupThreshold.store(threshold);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

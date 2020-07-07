#include "timing_reader.h"

namespace NYT::NTableClient {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TTimerGuard<TWallTimer> TTimingReaderBase::AcquireReadGuard()
{
    return TTimerGuard<TWallTimer>(&ReadTimer_);
}

TTimingStatistics TTimingReaderBase::GetTimingStatistics() const
{
    TTimingStatistics result;
    result.WaitTime = GetWaitTime();
    result.ReadTime = ReadTimer_.GetElapsedTime();
    result.IdleTime = TotalTimer_.GetElapsedTime() - result.WaitTime - result.ReadTime;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

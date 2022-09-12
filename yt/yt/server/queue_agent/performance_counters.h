#pragma once

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

extern const TEmaCounter::TWindowDurations WindowDurations;

//! A couple of EMA counters either for reading or writing.
struct TPerformanceCounters
{
    TEmaCounter RowCount = TEmaCounter(WindowDurations);
    TEmaCounter DataWeight = TEmaCounter(WindowDurations);
};

TPerformanceCounters& operator +=(TPerformanceCounters& lhs, const TPerformanceCounters& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

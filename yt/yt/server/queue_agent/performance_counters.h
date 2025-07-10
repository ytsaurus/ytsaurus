#pragma once

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! A couple of EMA counters either for reading or writing.
struct TPerformanceCounters
{
    static constexpr int WindowCount = 3;
    static const TEmaCounterWindowDurations<WindowCount> WindowDurations;

    TEmaCounter<i64, WindowCount> RowCount{WindowDurations};
    TEmaCounter<i64, WindowCount> DataWeight{WindowDurations};
};

TPerformanceCounters& operator +=(TPerformanceCounters& lhs, const TPerformanceCounters& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

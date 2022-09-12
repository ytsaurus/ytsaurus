#include "performance_counters.h"

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

const TEmaCounter::TWindowDurations WindowDurations = {
    TDuration::Minutes(1),
    TDuration::Hours(1),
    TDuration::Days(1),
};

TPerformanceCounters& operator +=(TPerformanceCounters& lhs, const TPerformanceCounters& rhs)
{
    lhs.RowCount += rhs.RowCount;
    lhs.DataWeight += rhs.DataWeight;
    return lhs;
}

TPerformanceCounters& operator *(TPerformanceCounters& lhs, double coefficient)
{
    lhs.RowCount *= coefficient;
    lhs.DataWeight *= coefficient;
    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

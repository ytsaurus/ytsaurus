#include "timing_statistics.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTimingStatistics& operator+=(TTimingStatistics& lhs, const TTimingStatistics& rhs)
{
    lhs.IdleTime += rhs.IdleTime;
    lhs.ReadTime += rhs.ReadTime;
    lhs.WaitTime += rhs.WaitTime;

    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTimingStatistics& statistics, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Wait: %v, Read: %v, Idle: %v}",
        statistics.WaitTime,
        statistics.ReadTime,
        statistics.IdleTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

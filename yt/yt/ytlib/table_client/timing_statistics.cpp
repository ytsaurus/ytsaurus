#include "timing_statistics.h"

namespace NYT::NTableClient {

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

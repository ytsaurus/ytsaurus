#include "timing_statistics.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TTimingStatistics& statistics)
{
    return Format(
        "{Wait: %v, Read: %v, Idle: %v}",
        statistics.WaitTime,
        statistics.ReadTime,
        statistics.IdleTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

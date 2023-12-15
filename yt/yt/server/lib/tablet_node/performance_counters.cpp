#include "performance_counters.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

const TEmaCounter::TWindowDurations TTabletPerformanceCounters::TabletPerformanceWindowDurations = {
    TDuration::Minutes(10),
    TDuration::Hours(1),
};

void Serialize(const TTabletPerformanceCounters& counters, NYson::IYsonConsumer* consumer)
{
    #define XX(name, Name) \
        .Item(#name "_count").Value(counters.Name.Count) \
        .Item(#name "_rate").Value(counters.Name.ImmediateRate) \
        .Item(#name "_10m_rate").Value(counters.Name.WindowRates[0]) \
        .Item(#name "_1h_rate").Value(counters.Name.WindowRates[1])
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
        .EndMap();
    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

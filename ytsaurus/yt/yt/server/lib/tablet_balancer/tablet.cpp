#include "tablet.h"

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    TTabletId tabletId,
    TTable* table)
    : Id(tabletId)
    , Table(std::move(table))
{ }

////////////////////////////////////////////////////////////////////////////////

TYsonString BuildTabletPerformanceCountersYson(
    const TTablet::TPerformanceCountersProtoList& emaCounters,
    const std::vector<TString>& performanceCountersKeys)
{
    YT_VERIFY(emaCounters.size() == std::ssize(performanceCountersKeys));
    return BuildYsonStringFluently()
        .DoMap([&] (TFluentMap fluent) {
            for (int i = 0; i < std::ssize(performanceCountersKeys); ++i) {
                const auto& counter = emaCounters[i];
                fluent
                    .Item(performanceCountersKeys[i] + "_count").Value(counter.count())
                    .Item(performanceCountersKeys[i] + "_rate").Value(counter.rate())
                    .Item(performanceCountersKeys[i] + "_10m_rate").Value(counter.rate_10m())
                    .Item(performanceCountersKeys[i] + "_1h_rate").Value(counter.rate_1h());
            }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

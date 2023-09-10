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

NYson::TYsonString TTablet::GetPerformanceCountersYson(
    const std::vector<TString>& performanceCountersKeys) const
{
    if (auto performanceCounters = std::get_if<TYsonString>(&PerformanceCounters)) {
        return *performanceCounters;
    }

    auto performanceCountersProto = std::get_if<TPerformanceCountersProtoList>(&PerformanceCounters);
    YT_VERIFY(performanceCountersProto);

    YT_VERIFY(performanceCountersProto->size() == std::ssize(performanceCountersKeys));
    return BuildYsonStringFluently()
        .DoMap([&] (TFluentMap fluent) {
            for (int i = 0; i < std::ssize(performanceCountersKeys); ++i) {
                const auto& counter = performanceCountersProto->at(i);
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

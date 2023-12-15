#include "tablet.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/ema_counter.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

using namespace NTableClient;
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
    const std::vector<TString>& performanceCountersKeys,
    const TTableSchemaPtr& performanceCountersTableSchema) const
{
    if (auto performanceCounters = std::get_if<TYsonString>(&PerformanceCounters)) {
        return *performanceCounters;
    }

    if (auto performanceCountersRow = std::get_if<NTableClient::TUnversionedOwningRow>(&PerformanceCounters)) {
        return BuildYsonStringFluently()
            .DoMap([&] (TFluentMap fluent) {
                for (const auto& performenceCounterKey : performanceCountersKeys) {
                    auto index = performanceCountersTableSchema->GetColumnIndexOrThrow(performenceCounterKey);
                    auto values = ConvertTo<IListNodePtr>(performanceCountersRow->Get()[index]);
                    fluent
                        .Item(performenceCounterKey + "_count").Value(values->GetChildValueOrThrow<i64>(0))
                        .Item(performenceCounterKey + "_rate").Value(values->GetChildValueOrThrow<double>(1))
                        .Item(performenceCounterKey + "_10m_rate").Value(values->GetChildValueOrThrow<double>(2))
                        .Item(performenceCounterKey + "_1h_rate").Value(values->GetChildValueOrThrow<double>(3));
                }
        });
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

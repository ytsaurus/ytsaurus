#include "tablet.h"

#include "private.h"
#include "table.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    TTabletId tabletId,
    TTable* table)
    : Id(tabletId)
    , Table(std::move(table))
{ }

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString TTablet::GetPerformanceCountersYson(
    const std::vector<std::string>& performanceCountersKeys,
    const TTableSchemaPtr& performanceCountersTableSchema) const
{
    if (auto performanceCounters = std::get_if<TYsonString>(&PerformanceCounters)) {
        return *performanceCounters;
    }

    if (auto performanceCountersRow = std::get_if<NTableClient::TUnversionedOwningRow>(&PerformanceCounters)) {
        YT_LOG_DEBUG_IF(Index == 0 &&
                performanceCountersTableSchema->GetValueColumnCount() != std::ssize(performanceCountersKeys),
            "Statistics reporter schema and current tablet balancer version has different performance counter keys "
            "(StatisticsReporterValueColumnCount: %v, PerformanceCountersKeyCount: %v, TabletId: %v)",
            performanceCountersTableSchema->GetValueColumnCount(),
            std::ssize(performanceCountersKeys),
            Id);

        return BuildYsonStringFluently()
            .DoMap([&] (TFluentMap fluent) {
                for (const auto& performanceCounterKey : performanceCountersKeys) {
                    if (!performanceCountersTableSchema->FindColumn(performanceCounterKey)) {
                        YT_LOG_DEBUG_IF(Index == 0 &&
                                performanceCountersTableSchema->GetValueColumnCount() == std::ssize(performanceCountersKeys),
                            "Statistics reporter schema does not contain performance counter column "
                            "(PerformanceCounterKey: %v, TabletId: %v)",
                            performanceCounterKey,
                            Id);
                        continue;
                    }

                    auto index = performanceCountersTableSchema->GetColumnIndexOrThrow(performanceCounterKey);
                    auto values = ConvertTo<INodePtr>(performanceCountersRow->Get()[index]);
                    THROW_ERROR_EXCEPTION_IF(values->GetType() != ENodeType::List && values->GetType() != ENodeType::Entity,
                        "Node has unexpected value type: expected one of (%Qv, %Qv), actual %Qv",
                        ENodeType::List,
                        ENodeType::Entity,
                        values->GetType());

                    bool isListNode = values->GetType() == ENodeType::List;
                    fluent
                        .Item(performanceCounterKey + "_count").Value(isListNode ? values->AsList()->GetChildValueOrThrow<i64>(0) : 0)
                        .Item(performanceCounterKey + "_rate").Value(isListNode ? values->AsList()->GetChildValueOrThrow<double>(1) : 0)
                        .Item(performanceCounterKey + "_10m_rate").Value(isListNode ? values->AsList()->GetChildValueOrThrow<double>(2) : 0)
                        .Item(performanceCounterKey + "_1h_rate").Value(isListNode ? values->AsList()->GetChildValueOrThrow<double>(3) : 0);
                }
        });
    }

    auto performanceCountersProto = std::get_if<TPerformanceCountersProtoList>(&PerformanceCounters);
    YT_VERIFY(performanceCountersProto);

    if (performanceCountersProto->size() != std::ssize(performanceCountersKeys)) {
        YT_LOG_WARNING("Logging current state before coredump (TabletId: %v, TableId: %v, TablePath: %v, "
            "MountTime: %v, TabletState: %v, PerformanceCountersProtoSize: %v, PerformanceCountersKeysSize: %v, "
            "PerformanceCountersProto: %v, PerformanceCountersKeys: %v)",
            Id,
            Table->Id,
            Table->Path,
            MountTime,
            State,
            performanceCountersProto->size(),
            std::ssize(performanceCountersKeys),
            performanceCountersProto,
            performanceCountersKeys);
    }

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

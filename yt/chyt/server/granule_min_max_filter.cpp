#include "granule_min_max_filter.h"

#include "conversion.h"
#include "query_context.h"

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/table_client/granule_filter.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYPath;
using namespace NStatisticPath;

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilter
    : public IGranuleFilter
{
public:
    TGranuleMinMaxFilter(
        DB::KeyCondition keyCondition,
        TTableSchemaPtr queryRealColumnsSchema,
        TCompositeSettingsPtr settings,
        TCallback<void(const TStatisticPath&, i64)> statisticsSampleCallback,
        std::shared_ptr<DB::ActionsDAG> filterActions)
        : KeyCondition_(std::move(keyCondition))
        , QueryRealColumnsSchema_(std::move(queryRealColumnsSchema))
        , ColumnDataTypes_(ToDataTypes(*QueryRealColumnsSchema_, settings))
        , StatisticsSampleCallback_(std::move(statisticsSampleCallback))
        , FilterActions_(std::move(filterActions))
    { }

    bool CanSkip(
        const TColumnarStatistics& statistics,
        const TNameTablePtr& granuleNameTable) const override
    {
        if (!statistics.HasValueStatistics()) {
            StatisticsSampleCallback_("/granule_min_max_filter/can_skip"_SP, false);
            return false;
        }

        auto typeAny = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));

        std::vector<DB::Range> columnRanges;
        columnRanges.reserve(QueryRealColumnsSchema_->GetColumnCount());

        for (const auto& columnSchema : QueryRealColumnsSchema_->Columns()) {
            auto columnId = granuleNameTable->FindId(columnSchema.Name());

            if (!columnId || *columnId >= statistics.GetColumnCount() || statistics.ColumnNonNullValueCounts[*columnId] == 0) {
                // All column values are null.
                columnRanges.emplace_back(DB::NEGATIVE_INFINITY);
            } else {
                bool hasNull = statistics.ColumnNonNullValueCounts[*columnId] != statistics.ChunkRowCount;
                auto columnType = columnSchema.LogicalType();

                auto range = hasNull
                    ? DB::Range::createWholeUniverse()
                    : DB::Range::createWholeUniverseWithoutNull();

                // 'Any' columns are converted to yson strings, so min/max statistics are meaningless for them.
                if (*columnType != *typeAny) {
                    if (statistics.ColumnMinValues[*columnId].Type() != EValueType::Min && !hasNull) {
                        range.left = ToField(statistics.ColumnMinValues[*columnId], columnType);
                        range.left_included = true;
                    }
                    if (statistics.ColumnMaxValues[*columnId].Type() != EValueType::Max) {
                        range.right = ToField(statistics.ColumnMaxValues[*columnId], columnType);
                        range.right_included = true;
                    }
                }

                columnRanges.push_back(std::move(range));
            }
        }

        StatisticsSampleCallback_("/granule_min_max_filter/can_skip"_SP, false);

        bool canSkip = !KeyCondition_.checkInHyperrectangle(columnRanges, ColumnDataTypes_).can_be_true;
        StatisticsSampleCallback_("/granule_min_max_filter/can_skip"_SP, canSkip);
        return canSkip;
    }

private:
    const DB::KeyCondition KeyCondition_;
    const TTableSchemaPtr QueryRealColumnsSchema_;
    const DB::DataTypes ColumnDataTypes_;
    const TCallback<void(const TStatisticPath&, i64)> StatisticsSampleCallback_;
    std::shared_ptr<DB::ActionsDAG> FilterActions_;
};

////////////////////////////////////////////////////////////////////////////////

IGranuleFilterPtr CreateGranuleMinMaxFilter(
    const DB::SelectQueryInfo& queryInfo,
    TCompositeSettingsPtr compositeSettings,
    const TTableSchemaPtr& schema,
    const DB::ContextPtr& context,
    const std::vector<std::string>& realColumnNames)
{
    auto filteredSchema = schema->Filter(realColumnNames);
    std::vector<std::string> columnNames;
    columnNames.reserve(filteredSchema->GetColumnCount());
    for (const auto& columnSchema : filteredSchema->Columns()) {
        columnNames.push_back(columnSchema.Name());
    }

    auto primaryKeyExpression = std::make_shared<DB::ExpressionActions>(DB::ActionsDAG(
        ToNamesAndTypesList(*filteredSchema, compositeSettings)));

    std::shared_ptr<DB::ActionsDAG> whereFilters = (queryInfo.filter_actions_dag != nullptr) ?
        std::make_shared<DB::ActionsDAG>(queryInfo.filter_actions_dag->clone())
        : nullptr;
    std::shared_ptr<DB::ActionsDAG> prewhereFilters = (queryInfo.prewhere_info != nullptr) ?
        std::make_shared<DB::ActionsDAG>(queryInfo.prewhere_info->prewhere_actions.clone())
        : nullptr;
    std::shared_ptr<DB::ActionsDAG> mergedFilters;
    if (whereFilters && prewhereFilters) {
        mergedFilters = std::make_shared<DB::ActionsDAG>(DB::ActionsDAG::merge(std::move(*whereFilters.get()), std::move(*prewhereFilters.get())));
    } else {
        mergedFilters = whereFilters ? whereFilters : prewhereFilters;
    }

    DB::KeyCondition keyCondition(mergedFilters.get(), context, std::move(columnNames), primaryKeyExpression);

    auto statisticsSampleCallback = BIND([weakContext = MakeWeak(GetQueryContext(context))] (const TStatisticPath& path, i64 sample) {
        if (auto queryContext = weakContext.Lock()) {
            queryContext->AddStatisticsSample(path, sample);
        }
    });

    return New<TGranuleMinMaxFilter>(
        std::move(keyCondition),
        std::move(filteredSchema),
        std::move(compositeSettings),
        std::move(statisticsSampleCallback),
        std::move(mergedFilters));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

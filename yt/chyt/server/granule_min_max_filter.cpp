#include "granule_min_max_filter.h"

#include "conversion.h"
#include "std_helpers.h"

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/table_client/granule_filter.h>

#include <Storages/MergeTree/KeyCondition.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilter
    : public IGranuleFilter
{
public:
    TGranuleMinMaxFilter(
        DB::KeyCondition keyCondition,
        TTableSchemaPtr queryRealColumnsSchema,
        TCompositeSettingsPtr settings)
        : KeyCondition_(std::move(keyCondition))
        , QueryRealColumnsSchema_(std::move(queryRealColumnsSchema))
        , ColumnDataTypes_(ToDataTypes(*QueryRealColumnsSchema_, settings))
    { }

    bool CanSkip(
        const TColumnarStatistics& statistics,
        const TNameTablePtr& granuleNameTable) const override
    {
        if (!statistics.HasValueStatistics()) {
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

        return !KeyCondition_.checkInHyperrectangle(columnRanges, ColumnDataTypes_).can_be_true;
    }

private:
    const DB::KeyCondition KeyCondition_;
    const TTableSchemaPtr QueryRealColumnsSchema_;
    const DB::DataTypes ColumnDataTypes_;
};

////////////////////////////////////////////////////////////////////////////////

IGranuleFilterPtr CreateGranuleMinMaxFilter(
    const DB::SelectQueryInfo& queryInfo,
    TCompositeSettingsPtr compositeSettings,
    const TTableSchemaPtr& schema,
    const DB::ContextPtr& context,
    const std::vector<TString>& realColumnNames)
{
    auto filteredSchema = schema->Filter(realColumnNames);

    auto primaryKeyExpression = std::make_shared<DB::ExpressionActions>(std::make_shared<DB::ActionsDAG>(
        ToNamesAndTypesList(*filteredSchema, compositeSettings)));

    DB::KeyCondition keyCondition(queryInfo, context, ToNames(realColumnNames), primaryKeyExpression);

    return New<TGranuleMinMaxFilter>(std::move(keyCondition), std::move(filteredSchema), std::move(compositeSettings));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

#include "granule_min_max_filter.h"

#include "conversion.h"
#include "std_helpers.h"

#include <yt/yt/client/table_client/columnar_statistics.h>
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

    bool CanSkip(const TColumnarStatistics& statistics, TNameTablePtr granuleNameTable) const override
    {
        if (!statistics.HasValueStatistics()) {
            return false;
        }
        std::vector<DB::Range> columnarValueRanges;
        columnarValueRanges.reserve(QueryRealColumnsSchema_->GetColumnCount());
        for (const auto& columnSchema : QueryRealColumnsSchema_->Columns()) {
            // Here by "position" we mean the column position in the table schema.
            auto maybeColumnPosition = granuleNameTable->FindId(columnSchema.Name());
            TUnversionedValue minValue;
            TUnversionedValue maxValue;
            if (!maybeColumnPosition.has_value()) {
                // There is no such a column in the chunk, but we're iterating over real columns,
                // so it is a column consisting of Null's
                minValue = MakeUnversionedNullValue();
                maxValue = MakeUnversionedNullValue();
            } else {
                minValue = statistics.ColumnMinValues[*maybeColumnPosition];
                maxValue = statistics.ColumnMaxValues[*maybeColumnPosition];
            }

            DB::Field minField;
            DB::Field maxField;
            bool includeMinBound = true;
            bool includeMaxBound = true;

            if (minValue.Type == EValueType::Min) {
                minField = DB::NEGATIVE_INFINITY;
                includeMinBound = false;
            } else {
                minField = ToField(minValue, columnSchema.LogicalType());
            }
            if (maxValue.Type == EValueType::Max) {
                maxField = DB::POSITIVE_INFINITY;
                includeMaxBound = false;
            } else {
                maxField = ToField(maxValue, columnSchema.LogicalType());
            }

            if (!maybeColumnPosition.has_value() || statistics.ColumnNonNullValueCounts[*maybeColumnPosition] < statistics.ChunkRowCount) {
                // There is Null in the column. Even if there's no such column in the chunk
                // we fall here for consistency, because it's still column with at least one Null.
                minField = DB::NEGATIVE_INFINITY;
                includeMinBound = true;
            }
            columnarValueRanges.push_back(DB::Range(minField, includeMinBound, maxField, includeMaxBound));
        }
        return !KeyCondition_.checkInHyperrectangle(columnarValueRanges, ColumnDataTypes_).can_be_true;
    }

private:
    DB::KeyCondition KeyCondition_;
    TTableSchemaPtr QueryRealColumnsSchema_;
    DB::DataTypes ColumnDataTypes_;
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

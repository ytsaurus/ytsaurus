#include "granule_min_max_filter.h"

#include "granule_filter.h"

#include <yt/yt/library/query/base/constraints.h>
#include <yt/yt/library/query/base/query.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilter
    : public IGranuleFilter
{
public:
    TGranuleMinMaxFilter(
        TConstExpressionPtr predicate,
        TKeyColumns keyColumns,
        TTableSchemaPtr schema)
        : Predicate_(std::move(predicate))
        , KeyColumns_(std::move(keyColumns))
        , Schema_(std::move(schema))
        , QueryConstraintsHolder_(KeyColumns_.size())
    {
        QueryConstraint_ = QueryConstraintsHolder_.ExtractFromExpression(
            Predicate_,
            KeyColumns_,
            New<TRowBuffer>());
    }

    bool CanSkip(
        const TColumnarStatistics& statistics,
        const TNameTablePtr& granuleNameTable) const override
    {
        if (!statistics.HasValueStatistics()) {
            return false;
        }

        TConstraintsHolder constraints = QueryConstraintsHolder_;
        auto rootConstraint = QueryConstraint_;

        for (const auto& column: KeyColumns_) {
            auto columnSchema = Schema_->GetColumn(column);
            auto columnId = granuleNameTable->FindId(columnSchema.StableName().Underlying());
            auto keyPartIndex = ColumnNameToKeyPartIndex(KeyColumns_, column);

            if (!columnId || *columnId >= statistics.GetColumnCount()) {
                continue;
            }

            if (statistics.ColumnNonNullValueCounts[*columnId] == 0) {
                auto nullConstraint = constraints.Constant(
                    MakeUnversionedNullValue(),
                    keyPartIndex);
                rootConstraint = constraints.Intersect(rootConstraint, nullConstraint);

            } else {
                auto constraint = constraints.Interval(
                    TValueBound{statistics.ColumnMinValues[*columnId], false},
                    TValueBound{statistics.ColumnMaxValues[*columnId], true},
                    keyPartIndex);

                if (statistics.ColumnNonNullValueCounts[*columnId] != statistics.ChunkRowCount) {
                    constraint = constraints.Unite(
                        constraint,
                        constraints.Constant(MakeUnversionedNullValue(), keyPartIndex));
                }

                rootConstraint = constraints.Intersect(rootConstraint, constraint);
            }
        }

        return rootConstraint.IsEmpty();
    }

private:
    const TConstExpressionPtr Predicate_;
    const TKeyColumns KeyColumns_;
    const TTableSchemaPtr Schema_;

    TConstraintsHolder QueryConstraintsHolder_;
    TConstraintRef QueryConstraint_;
};

////////////////////////////////////////////////////////////////////////////////

IGranuleFilterPtr CreateGranuleMinMaxFilter(const TConstQueryPtr& query)
{
    auto readSchema = query->GetReadSchema();

    // For these columns we will build disjunctive form
    // that represents constraints from predicate and min/max statistics.
    // Here we will take into account all columns that have statistics.
    TKeyColumns keyColumns;
    keyColumns.reserve(readSchema->Columns().size());
    for (const auto& column : readSchema->Columns()) {
        keyColumns.emplace_back(column.Name());
    }

    return New<TGranuleMinMaxFilter>(
        query->WhereClause,
        std::move(keyColumns),
        std::move(readSchema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

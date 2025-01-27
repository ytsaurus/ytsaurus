#include "granule_min_max_filter.h"

#include "granule_filter.h"

#include <yt/yt/library/query/base/constraints.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/query_visitors.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NTableClient {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilter
    : public IGranuleFilter
{
public:
    TGranuleMinMaxFilter(
        TTableSchemaPtr schema,
        std::vector<int> relevantKeyPartIndices,
        TConstraintRef queryConstraint,
        TConstraintsHolder queryConstraintsHolder,
        TRowBufferPtr rowBuffer,
        NLogging::TLogger logger)
        : Schema_(std::move(schema))
        , RelevantKeyPartIndices_(std::move(relevantKeyPartIndices))
        , QueryConstraint_(std::move(queryConstraint))
        , QueryConstraintsHolder_(std::move(queryConstraintsHolder))
        , RowBuffer_(std::move(rowBuffer))
        , Logger(std::move(logger))
    { }

    ~TGranuleMinMaxFilter()
    {
        YT_LOG_DEBUG("Destroying granule filter (SeenGranules: %v, SkippedGranules: %v, TotalTimeSpentMilliSeconds: %vms)",
            SeenGranules_,
            SkippedGranules_,
            TotalTimeSpentMilliSeconds_);
    }

    bool CanSkip(
        const TColumnarStatistics& statistics,
        const TNameTablePtr& granuleNameTable) const override
    {
        ++SeenGranules_;

        NProfiling::TWallTimer wallTime;
        auto finally = Finally([&] {
            TotalTimeSpentMilliSeconds_ += wallTime.GetElapsedTime().MilliSeconds();
        });

        if (!statistics.HasValueStatistics()) {
            return false;
        }

        TConstraintsHolder constraints = QueryConstraintsHolder_;
        auto statisticsConstraints = TConstraintRef::Universal();

        for (int keyPartIndex : RelevantKeyPartIndices_) {
            auto& columnSchema = Schema_->Columns()[keyPartIndex];

            auto columnId = granuleNameTable->FindId(columnSchema.StableName().Underlying());

            if (!columnId || *columnId >= statistics.GetColumnCount()) {
                continue;
            }

            if (statistics.ColumnNonNullValueCounts[*columnId] == 0) {
                auto nullConstraint = constraints.Constant(MakeUnversionedNullValue(), keyPartIndex);
                statisticsConstraints = constraints.Intersect(statisticsConstraints, nullConstraint);
            } else {
                auto constraint = constraints.Interval(
                    TValueBound(statistics.ColumnMinValues[*columnId], false),
                    TValueBound(statistics.ColumnMaxValues[*columnId], true),
                    keyPartIndex);

                if (statistics.ColumnNonNullValueCounts[*columnId] != statistics.ChunkRowCount) {
                    auto nullConstraint = constraints.Constant(MakeUnversionedNullValue(), keyPartIndex);
                    constraint = constraints.Unite(constraint, nullConstraint);
                }

                statisticsConstraints = constraints.Intersect(statisticsConstraints, constraint);
            }
        }

        bool canSkip = constraints.Intersect(statisticsConstraints, QueryConstraint_).IsEmpty();

        if (canSkip) {
            ++SkippedGranules_;
        }

        return canSkip;
    }

private:
    const TTableSchemaPtr Schema_;
    const std::vector<int> RelevantKeyPartIndices_;
    const TConstraintRef QueryConstraint_;
    const TConstraintsHolder QueryConstraintsHolder_;
    const TRowBufferPtr RowBuffer_;

    const NLogging::TLogger Logger;

    mutable std::atomic<ui64> SeenGranules_ = 0;
    mutable std::atomic<ui64> SkippedGranules_ = 0;
    mutable std::atomic<ui64> TotalTimeSpentMilliSeconds_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TGranuleMinMaxFilterQueryConstraintsTag
{ };

IGranuleFilterPtr CreateGranuleMinMaxFilter(const TConstQueryPtr& query, NLogging::TLogger logger)
{
    auto schema = query->GetReadSchema();
    auto& schemaColumns = schema->Columns();

    auto relevantKeyPartIndices = std::vector<int>();
    {
        auto queryReferences = TColumnSet();
        TReferenceHarvester(&queryReferences).Visit(query->WhereClause);

        for (int index = 0; index < std::ssize(schemaColumns); ++index) {
            if (auto it = queryReferences.find(schemaColumns[index].Name()); it != queryReferences.end()) {
                relevantKeyPartIndices.push_back(index);
                queryReferences.erase(it);
            }
        }

        YT_VERIFY(queryReferences.empty());
    }

    auto rowBuffer = New<TRowBuffer>(TGranuleMinMaxFilterQueryConstraintsTag());

    auto queryConstraintsHolder = TConstraintsHolder(
        schemaColumns.size(),
        GetRefCountedTypeCookie<TGranuleMinMaxFilterQueryConstraintsTag>(),
        GetDefaultMemoryChunkProvider());

    auto queryConstraint = TConstraintRef();
    {
        auto keyColumns = TKeyColumns();
        keyColumns.reserve(std::ssize(schemaColumns));
        for (const auto& column : schemaColumns) {
            keyColumns.emplace_back(column.Name());
        }

        queryConstraint = queryConstraintsHolder.ExtractFromExpression(
            query->WhereClause,
            keyColumns,
            rowBuffer);
    }

    return New<TGranuleMinMaxFilter>(
        std::move(schema),
        std::move(relevantKeyPartIndices),
        std::move(queryConstraint),
        std::move(queryConstraintsHolder),
        std::move(rowBuffer),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

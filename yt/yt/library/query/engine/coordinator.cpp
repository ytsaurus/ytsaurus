
#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>
#include <yt/yt/library/query/engine_api/new_range_inferrer.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/writer.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>

#include <yt/yt/core/logging/log.h>

#include <numeric>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

std::pair<TConstFrontQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TRefiner>& refiners)
{
    auto Logger = MakeQueryLogger(query);

    auto subqueryPattern = New<TQuery>();

    subqueryPattern->Schema.Original = query->Schema.Original;
    subqueryPattern->Schema.Mapping = query->Schema.Mapping;
    subqueryPattern->JoinClauses = query->JoinClauses;
    subqueryPattern->OrderClause = query->OrderClause;
    subqueryPattern->HavingClause = query->HavingClause;
    subqueryPattern->GroupClause = query->GroupClause;
    subqueryPattern->Offset = 0;
    subqueryPattern->Limit = query->Offset + query->Limit;
    subqueryPattern->UseDisjointGroupBy = query->UseDisjointGroupBy;
    subqueryPattern->InferRanges = query->InferRanges;
    subqueryPattern->IsFinal = false;

    auto topQuery = New<TFrontQuery>();

    topQuery->GroupClause = query->GroupClause;
    topQuery->HavingClause = query->HavingClause;
    topQuery->OrderClause = query->OrderClause;
    topQuery->Offset = query->Offset;
    topQuery->Limit = query->Limit;
    topQuery->IsFinal = query->IsFinal;
    topQuery->ProjectClause = query->ProjectClause;
    topQuery->Schema = subqueryPattern->GetTableSchema();

    std::vector<TConstQueryPtr> subqueries;

    for (const auto& refiner : refiners) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(*subqueryPattern);
        subquery->Id = TGuid::Create();

        if (query->WhereClause) {
            auto refinedWhere = refiner(query->WhereClause, subquery->GetKeyColumns());
            subquery->WhereClause = IsTrue(refinedWhere) ? nullptr : refinedWhere;
        }

        subqueries.push_back(subquery);
    }

    return std::make_pair(topQuery, subqueries);
}

TRowRanges GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchemaPtr& tableSchema,
    const TKeyColumns& keyColumns,
    TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    TGuid queryId)
{
    auto Logger = MakeQueryLogger(queryId);

    YT_LOG_DEBUG("Inferring ranges from predicate");

    TRangeInferrer rangeInferrer;

    if (options.NewRangeInference) {
        rangeInferrer = CreateNewRangeInferrer(
            predicate,
            tableSchema,
            keyColumns,
            evaluatorCache,
            GetBuiltinConstraintExtractors(),
            options);
    } else {
        rangeInferrer = CreateRangeInferrer(
            predicate,
            tableSchema,
            keyColumns,
            evaluatorCache,
            rangeExtractors,
            options);
    }

    auto keyRangeFormatter = [] (const TRowRange& range) {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    YT_LOG_DEBUG("Splitting %v sources according to ranges", ranges.Size());

    TRowRanges result;
    for (const auto& originalRange : ranges) {
        auto inferred = rangeInferrer(originalRange, rowBuffer);
        result.insert(result.end(), inferred.begin(), inferred.end());

        for (const auto& range : inferred) {
            YT_LOG_DEBUG_IF(options.VerboseLogging, "Narrowing source %v key range from %v to %v",
                tableId,
                keyRangeFormatter(originalRange),
                keyRangeFormatter(range));
        }
    }

    return result;
}

TRowRanges GetPrunedRanges(
    const TConstQueryPtr& query,
    TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options)
{
    return GetPrunedRanges(
        query->WhereClause,
        query->Schema.Original,
        query->GetKeyColumns(),
        tableId,
        std::move(ranges),
        rowBuffer,
        evaluatorCache,
        rangeExtractors,
        options,
        query->Id);
}

TQueryStatistics CoordinateAndExecute(
    const TConstQueryPtr& query,
    const IUnversionedRowsetWriterPtr& writer,
    const std::vector<TRefiner>& refiners,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstFrontQueryPtr&, const ISchemafulUnversionedReaderPtr&, const IUnversionedRowsetWriterPtr&)> evaluateTop)
{
    auto Logger = MakeQueryLogger(query);

    YT_LOG_DEBUG("Begin coordinating query");

    TConstFrontQueryPtr topQuery;
    std::vector<TConstQueryPtr> subqueries;
    std::tie(topQuery, subqueries) = CoordinateQuery(query, refiners);

    YT_LOG_DEBUG("Finished coordinating query");

    std::vector<ISchemafulUnversionedReaderPtr> splitReaders;

    // Use TFutureHolder to prevent leaking subqueries.
    std::vector<TFutureHolder<TQueryStatistics>> subqueryHolders;

    auto subqueryReaderCreator = [&, index = 0] () mutable -> ISchemafulUnversionedReaderPtr {
        if (index >= std::ssize(subqueries)) {
            return nullptr;
        }

        const auto& subquery = subqueries[index];

        ISchemafulUnversionedReaderPtr reader;
        TFuture<TQueryStatistics> statistics;
        std::tie(reader, statistics) = evaluateSubquery(subquery, index);

        subqueryHolders.push_back(statistics);

        ++index;

        return reader;
    };

    // TODO: Use separate condition for prefetch after protocol update
    auto topReader = query->IsOrdered()
        ? (query->Limit == std::numeric_limits<i64>::max() - 1
            ? CreateFullPrefetchingOrderedSchemafulReader(std::move(subqueryReaderCreator))
            : CreateOrderedSchemafulReader(std::move(subqueryReaderCreator)))
        : CreateUnorderedSchemafulReader(std::move(subqueryReaderCreator), subqueries.size());

    auto queryStatistics = evaluateTop(topQuery, std::move(topReader), std::move(writer));

    for (int index = 0; index < std::ssize(subqueryHolders); ++index) {
        auto subqueryStatisticsOrError = WaitFor(subqueryHolders[index].Get());
        if (subqueryStatisticsOrError.IsOK()) {
            const auto& subqueryStatistics = subqueryStatisticsOrError.ValueOrThrow();
            YT_LOG_DEBUG("Subquery finished (SubqueryId: %v, Statistics: %v)",
                subqueries[index]->Id,
                subqueryStatistics);
            queryStatistics.AddInnerStatistics(subqueryStatistics);
        } else {
            YT_LOG_DEBUG(subqueryStatisticsOrError, "Subquery failed (SubqueryId: %v)",
                subqueries[index]->Id);
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#include "coordinator.h"
#include "private.h"
#include "helpers.h"
#include "query.h"
#include "query_helpers.h"
#include "range_inferrer.h"

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/unordered_schemaful_reader.h>
#include <yt/ytlib/table_client/writer.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/logging/log.h>

#include <numeric>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::pair<TConstFrontQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TRefiner>& refiners)
{
    auto Logger = MakeQueryLogger(query);

    auto subqueryPattern = New<TQuery>();

    subqueryPattern->OriginalSchema = query->OriginalSchema;
    subqueryPattern->SchemaMapping = query->SchemaMapping;
    subqueryPattern->JoinClauses = query->JoinClauses;
    subqueryPattern->OrderClause = query->OrderClause;
    subqueryPattern->HavingClause = query->HavingClause;
    subqueryPattern->GroupClause = query->GroupClause;
    subqueryPattern->Limit = query->Limit;
    subqueryPattern->UseDisjointGroupBy = query->UseDisjointGroupBy;
    subqueryPattern->InferRanges = query->InferRanges;
    subqueryPattern->IsFinal = false;

    auto topQuery = New<TFrontQuery>();

    if (auto groupClause = query->GroupClause.Get()) {
        auto topGroupClause = New<TGroupClause>();
        auto& finalGroupItems = topGroupClause->GroupItems;
        for (const auto& groupItem : groupClause->GroupItems) {
            auto referenceExpr = New<TReferenceExpression>(
                groupItem.Expression->Type,
                groupItem.Name);
            finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
        }

        auto& finalAggregateItems = topGroupClause->AggregateItems;
        for (const auto& aggregateItem : groupClause->AggregateItems) {
            auto referenceExpr = New<TReferenceExpression>(
                aggregateItem.Expression->Type,
                aggregateItem.Name);
            finalAggregateItems.emplace_back(
                std::move(referenceExpr),
                aggregateItem.AggregateFunction,
                aggregateItem.Name,
                aggregateItem.StateType,
                aggregateItem.ResultType);
        }

        topGroupClause->TotalsMode = groupClause->TotalsMode;

        topQuery->GroupClause = topGroupClause;
    }

    topQuery->HavingClause = query->HavingClause;
    topQuery->OrderClause = query->OrderClause;
    topQuery->Limit = query->Limit;
    topQuery->IsFinal = query->IsFinal;
    topQuery->ProjectClause = query->ProjectClause;

    // Use groupClause->KeyPrefix

    topQuery->Schema = subqueryPattern->GetTableSchema();

    std::vector<TConstQueryPtr> subqueries;

    for (const auto& refiner : refiners) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(*subqueryPattern);
        subquery->Id = TGuid::Create();

        if (query->WhereClause) {
            subquery->WhereClause = refiner(
                query->WhereClause,
                subquery->GetKeyColumns());
        }

        subqueries.push_back(subquery);
    }

    return std::make_pair(topQuery, subqueries);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRowRanges GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TObjectId& tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    const NLogging::TLogger& Logger)
{
    LOG_DEBUG("Inferring ranges from predicate");

    auto rangeInferrer = CreateRangeInferrer(
        predicate,
        tableSchema,
        keyColumns,
        evaluatorCache,
        rangeExtractors,
        options);

    auto keyRangeFormatter = [] (const TRowRange& range) -> TString {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    LOG_DEBUG("Splitting %v sources according to ranges", ranges.Size());

    TRowRanges result;
    for (const auto& originalRange : ranges) {
        auto inferred = rangeInferrer(originalRange, rowBuffer);
        result.insert(result.end(), inferred.begin(), inferred.end());

        for (const auto& range : inferred) {
            LOG_DEBUG_IF(options.VerboseLogging, "Narrowing source %v key range from %v to %v",
                tableId,
                keyRangeFormatter(originalRange),
                keyRangeFormatter(range));
        }
    }

    return result;
}

TRowRanges GetPrunedRanges(
    const TConstQueryPtr& query,
    const TObjectId& tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options)
{
    auto Logger = MakeQueryLogger(query);
    return GetPrunedRanges(
        query->WhereClause,
        query->OriginalSchema,
        query->GetKeyColumns(),
        tableId,
        std::move(ranges),
        rowBuffer,
        evaluatorCache,
        rangeExtractors,
        options,
        Logger);
}

TQueryStatistics CoordinateAndExecute(
    const TConstQueryPtr& query,
    const ISchemafulWriterPtr& writer,
    const std::vector<TRefiner>& refiners,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstFrontQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop)
{
    auto Logger = MakeQueryLogger(query);

    LOG_DEBUG("Begin coordinating query");

    TConstFrontQueryPtr topQuery;
    std::vector<TConstQueryPtr> subqueries;
    std::tie(topQuery, subqueries) = CoordinateQuery(query, refiners);

    LOG_DEBUG("Finished coordinating query");

    std::vector<ISchemafulReaderPtr> splitReaders;

    // Use TFutureHolder to prevent leaking subqueries.
    std::vector<TFutureHolder<TQueryStatistics>> subqueryHolders;

    auto subqueryReaderCreator = [&, index = 0] () mutable -> ISchemafulReaderPtr {
        if (index >= subqueries.size()) {
            return nullptr;
        }

        const auto& subquery = subqueries[index];

        ISchemafulReaderPtr reader;
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

    for (int index = 0; index < subqueryHolders.size(); ++index) {
        auto subqueryStatisticsOrError = WaitFor(subqueryHolders[index].Get());
        if (subqueryStatisticsOrError.IsOK()) {
            const auto& subqueryStatistics = subqueryStatisticsOrError.ValueOrThrow();
            LOG_DEBUG("Subquery finished (SubqueryId: %v, Statistics: %v)",
                subqueries[index]->Id,
                subqueryStatistics);
            queryStatistics.AddInnerStatistics(subqueryStatistics);
        } else {
            LOG_DEBUG(subqueryStatisticsOrError, "Subquery failed (SubqueryId: %v)",
                subqueries[index]->Id);
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


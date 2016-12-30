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

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    TConstQueryPtr query,
    const std::vector<TRefiner>& refiners)
{
    auto Logger = MakeQueryLogger(query);

    auto subqueryInputRowLimit = query->InputRowLimit;
    auto subqueryOutputRowLimit = query->OutputRowLimit;

    auto subqueryPattern = New<TQuery>(
        subqueryInputRowLimit,
        subqueryOutputRowLimit);

    subqueryPattern->OriginalSchema = query->OriginalSchema;
    subqueryPattern->SchemaMapping = query->SchemaMapping;
    subqueryPattern->JoinClauses = query->JoinClauses;
    subqueryPattern->UseDisjointGroupBy = query->UseDisjointGroupBy;

    auto topQuery = New<TQuery>(
        query->InputRowLimit,
        query->OutputRowLimit);

    topQuery->OrderClause = query->OrderClause;
    topQuery->Limit = query->Limit;

    auto groupClause = query->GroupClause;

    if (groupClause && refiners.size() > 1 &&
        !(query->UseDisjointGroupBy && groupClause->TotalsMode == ETotalsMode::None))
    {
        auto subqueryGroupClause = New<TGroupClause>();
        subqueryGroupClause->GroupItems = groupClause->GroupItems;
        subqueryGroupClause->AggregateItems = groupClause->AggregateItems;
        subqueryGroupClause->IsMerge = groupClause->IsMerge;
        subqueryGroupClause->IsFinal = false;
        subqueryGroupClause->TotalsMode = ETotalsMode::None;
        subqueryPattern->GroupClause = subqueryGroupClause;

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

        topGroupClause->IsMerge = true;
        topGroupClause->IsFinal = groupClause->IsFinal;
        topGroupClause->TotalsMode = groupClause->TotalsMode;
        topQuery->GroupClause = topGroupClause;
        topQuery->HavingClause = query->HavingClause;
        topQuery->ProjectClause = query->ProjectClause;
    } else {
        subqueryPattern->GroupClause = groupClause;
        subqueryPattern->HavingClause = query->HavingClause;
        subqueryPattern->Limit = query->Limit;

        if (query->OrderClause) {
            subqueryPattern->OrderClause = query->OrderClause;
            topQuery->ProjectClause = query->ProjectClause;
        } else {
            subqueryPattern->ProjectClause = query->ProjectClause;
        }
    }

    topQuery->OriginalSchema = subqueryPattern->GetTableSchema();
    const auto& originalSchemaColumns = topQuery->OriginalSchema.Columns();
    for (size_t index = 0; index < originalSchemaColumns.size(); ++index) {
        const auto& column = originalSchemaColumns[index];
        topQuery->SchemaMapping.push_back(TColumnDescriptor{column.Name, index});
    }

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
    TConstExpressionPtr predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TObjectId& tableId,
    TSharedRange<TRowRange> ranges,
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

    auto keyRangeFormatter = [] (const TRowRange& range) -> Stroka {
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
    TConstQueryPtr query,
    const TObjectId& tableId,
    TSharedRange<TRowRange> ranges,
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
    TConstQueryPtr query,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& refiners,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop)
{
    auto Logger = MakeQueryLogger(query);

    LOG_DEBUG("Begin coordinating query");

    TConstQueryPtr topQuery;
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

    int topReaderConcurrency = query->IsOrdered() ? 1 : subqueries.size();
    auto topReader = CreateUnorderedSchemafulReader(std::move(subqueryReaderCreator), topReaderConcurrency);
    auto queryStatistics = evaluateTop(topQuery, std::move(topReader), std::move(writer));

    for (int index = 0; index < subqueryHolders.size(); ++index) {
        auto subQueryStatisticsOrError = WaitFor(subqueryHolders[index].Get());
        if (subQueryStatisticsOrError.IsOK()) {
            const auto& subQueryStatistics = subQueryStatisticsOrError.ValueOrThrow();
            LOG_DEBUG("Subquery finished (SubqueryId: %v, Statistics: %v)",
                subqueries[index]->Id,
                subQueryStatistics);
            queryStatistics += subQueryStatistics;
        } else {
            LOG_DEBUG(subQueryStatisticsOrError, "Subquery failed (SubqueryId: %v)",
                subqueries[index]->Id);
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


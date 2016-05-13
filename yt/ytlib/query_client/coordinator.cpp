#include "coordinator.h"
#include "private.h"
#include "helpers.h"
#include "plan_fragment.h"
#include "plan_helpers.h"
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

////////////////////////////////////////////////////////////////////////////////

TTableSchema GetIntermediateSchema(TConstGroupClausePtr groupClause)
{
    std::vector<TColumnSchema> columns;
    for (auto item : groupClause->GroupItems) {
        columns.push_back(TColumnSchema(
            item.Name,
            item.Expression->Type));
    }

    for (auto item : groupClause->AggregateItems) {
        columns.push_back(TColumnSchema(
            item.Name,
            item.StateType));
    }

    return TTableSchema(std::move(columns));
}

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    TConstQueryPtr query,
    const std::vector<TRefiner>& refiners)
{
    auto Logger = BuildLogger(query);

    auto subqueryInputRowLimit = refiners.empty()
        ? 0
        : 2 * std::min(query->InputRowLimit, std::numeric_limits<i64>::max() / 2) / refiners.size();

    auto subqueryOutputRowLimit = query->OutputRowLimit;

    auto subqueryPattern = New<TQuery>(
        subqueryInputRowLimit,
        subqueryOutputRowLimit);

    subqueryPattern->TableSchema = query->TableSchema;
    subqueryPattern->KeyColumnsCount = query->KeyColumnsCount;
    subqueryPattern->RenamedTableSchema = query->RenamedTableSchema;
    subqueryPattern->JoinClauses = query->JoinClauses;

    auto topQuery = New<TQuery>(
        query->InputRowLimit,
        query->OutputRowLimit);

    topQuery->HavingClause = query->HavingClause;
    topQuery->OrderClause = query->OrderClause;
    topQuery->Limit = query->Limit;

    if (query->GroupClause) {
        if (refiners.size() > 1) {
            auto subqueryGroupClause = New<TGroupClause>();
            subqueryGroupClause->GroupedTableSchema = GetIntermediateSchema(query->GroupClause);
            subqueryGroupClause->GroupItems = query->GroupClause->GroupItems;
            subqueryGroupClause->AggregateItems = query->GroupClause->AggregateItems;
            subqueryGroupClause->IsMerge = query->GroupClause->IsMerge;
            subqueryGroupClause->IsFinal = false;
            subqueryGroupClause->TotalsMode = ETotalsMode::None;
            subqueryPattern->GroupClause = subqueryGroupClause;

            auto topGroupClause = New<TGroupClause>();

            topGroupClause->GroupedTableSchema = query->GroupClause->GroupedTableSchema;

            auto& finalGroupItems = topGroupClause->GroupItems;
            for (const auto& groupItem : query->GroupClause->GroupItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    groupItem.Expression->Type,
                    groupItem.Name);
                finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
            }

            auto& finalAggregateItems = topGroupClause->AggregateItems;
            for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
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
            topGroupClause->IsFinal = query->GroupClause->IsFinal;
            topGroupClause->TotalsMode = query->GroupClause->TotalsMode;
            topQuery->GroupClause = topGroupClause;
        } else {
            subqueryPattern->GroupClause = query->GroupClause;
        }

        topQuery->ProjectClause = query->ProjectClause;
    } else {
        subqueryPattern->Limit = query->Limit;

        if (query->OrderClause) {
            subqueryPattern->OrderClause = query->OrderClause;
            topQuery->ProjectClause = query->ProjectClause;
        } else {
            subqueryPattern->ProjectClause = query->ProjectClause;
        }
    }

    topQuery->TableSchema = subqueryPattern->GetTableSchema();
    topQuery->RenamedTableSchema = topQuery->TableSchema;

    std::vector<TConstQueryPtr> subqueries;

    for (const auto& refiner : refiners) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(*subqueryPattern);
        subquery->Id = TGuid::Create();

        if (query->WhereClause) {
            subquery->WhereClause = refiner(
                query->WhereClause,
                subquery->TableSchema,
                TableSchemaToKeyColumns(
                    subquery->RenamedTableSchema,
                    subquery->KeyColumnsCount));
        }

        subqueries.push_back(subquery);
    }

    return std::make_pair(topQuery, subqueries);
}

TRowRanges GetPrunedRanges(
    TConstExpressionPtr predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    NObjectClient::TObjectId tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    ui64 rangeExpansionLimit,
    bool verboseLogging,
    const NLogging::TLogger& Logger)
{
    LOG_DEBUG("Infering ranges from predicate");

    auto rangeInferrer = CreateRangeInferrer(
        predicate,
        tableSchema,
        keyColumns,
        evaluatorCache,
        rangeExtractors,
        rangeExpansionLimit,
        verboseLogging);

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
            LOG_DEBUG_IF(verboseLogging, "Narrowing source %v key range from %v to %v",
                tableId,
                keyRangeFormatter(originalRange),
                keyRangeFormatter(range));
        }
    }

    return result;
}

TRowRanges GetPrunedRanges(
    TConstQueryPtr query,
    NObjectClient::TObjectId tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    ui64 rangeExpansionLimit,
    bool verboseLogging)
{
    auto Logger = BuildLogger(query);
    return GetPrunedRanges(
        query->WhereClause,
        query->TableSchema,
        TableSchemaToKeyColumns(query->RenamedTableSchema, query->KeyColumnsCount),
        tableId,
        std::move(ranges),
        rowBuffer,
        evaluatorCache,
        rangeExtractors,
        rangeExpansionLimit,
        verboseLogging,
        Logger);
}

TRowRange GetRange(const TSharedRange<TDataRange>& sources)
{
    YCHECK(!sources.Empty());
    return std::accumulate(sources.Begin() + 1, sources.End(), sources.Begin()->Range, [] (
        TRowRange keyRange, const TDataRange & source) -> TRowRange
    {
        return Unite(keyRange, source.Range);
    });
}

TQueryStatistics CoordinateAndExecute(
    TConstQueryPtr query,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& refiners,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop)
{
    auto Logger = BuildLogger(query);

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


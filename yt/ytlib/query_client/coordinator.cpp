#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"
#include "plan_helpers.h"
#include "plan_fragment.h"
#include "range_inferrer.h"
#include "functions.h"

#include <core/logging/log.h>

#include <ytlib/table_client/schemaful_reader.h>
#include <ytlib/table_client/writer.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/unordered_schemaful_reader.h>

#include <ytlib/tablet_client/public.h>

#include <numeric>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTableSchema GetIntermediateSchema(
    TConstGroupClausePtr groupClause,
    IFunctionRegistryPtr functionRegistry)
{
    auto schema = TTableSchema();

    for (auto item : groupClause->GroupItems) {
        schema.Columns().emplace_back(
            item.Name,
            item.Expression->Type);
    }

    for (auto item : groupClause->AggregateItems) {
        auto intermediateType = functionRegistry
            ->GetAggregateFunction(item.AggregateFunction)
            ->GetStateType(item.Expression->Type);
        schema.Columns().emplace_back(
            item.Name,
            intermediateType);
    }

    return schema;
}

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    TConstQueryPtr query,
    const std::vector<TRefiner>& refiners,
    IFunctionRegistryPtr functionRegistry)
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
            subqueryGroupClause->GroupedTableSchema = GetIntermediateSchema(
                query->GroupClause,
                functionRegistry);
            subqueryGroupClause->GroupItems = query->GroupClause->GroupItems;
            subqueryGroupClause->AggregateItems = query->GroupClause->AggregateItems;
            subqueryGroupClause->IsMerge = query->GroupClause->IsMerge;
            subqueryGroupClause->IsFinal = false;
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
            topQuery->GroupClause = topGroupClause;
        } else {
            auto groupClause = New<TGroupClause>();
            groupClause->GroupedTableSchema = query->GroupClause->GroupedTableSchema;
            groupClause->GroupItems = query->GroupClause->GroupItems;
            groupClause->AggregateItems = query->GroupClause->AggregateItems;
            groupClause->IsMerge = query->GroupClause->IsMerge;
            groupClause->IsFinal = query->GroupClause->IsFinal;
            subqueryPattern->GroupClause = groupClause;
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

TGroupedRanges GetPrunedRanges(
    TConstExpressionPtr predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TDataSources& sources,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
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
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging);

    LOG_DEBUG("Splitting %v sources according to ranges", sources.size());

    TGroupedRanges prunedSources;
    for (const auto& source : sources) {
        prunedSources.emplace_back();
        const auto& originalRange = source.Range;
        auto ranges = rangeInferrer(originalRange, rowBuffer);
        auto& group = prunedSources.back();
        group.insert(group.end(), ranges.begin(), ranges.end());

        for (const auto& range : ranges) {
            LOG_DEBUG_IF(verboseLogging, "Narrowing source %v key range from %v to %v",
                source.Id,
                MakeFormatted(TRowRangeFormatter(), originalRange),
                MakeFormatted(TRowRangeFormatter(), range));
        }
    }

    return prunedSources;
}

TGroupedRanges GetPrunedRanges(
    TConstExpressionPtr predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TDataSources& sources,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging)
{
    return GetPrunedRanges(
        predicate,
        tableSchema,
        keyColumns,
        sources,
        rowBuffer,
        evaluatorCache,
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging,
        Logger);
}

TGroupedRanges GetPrunedRanges(
    TConstQueryPtr query,
    const TDataSources& sources,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging)
{
    auto Logger = BuildLogger(query);
    return GetPrunedRanges(
        query->WhereClause,
        query->TableSchema,
        TableSchemaToKeyColumns(query->RenamedTableSchema, query->KeyColumnsCount),
        sources,
        rowBuffer,
        evaluatorCache,
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging,
        Logger);
}

TRowRange GetRange(const TDataSources& sources)
{
    YCHECK(!sources.empty());
    return std::accumulate(sources.begin() + 1, sources.end(), sources.front().Range, [] (TRowRange keyRange, const TDataSource& source) -> TRowRange {
        return Unite(keyRange, source.Range);
    });
}

TRowRanges GetRanges(const std::vector<TDataSources>& groupedSplits)
{
    TRowRanges ranges(groupedSplits.size());
    for (int index = 0; index < groupedSplits.size(); ++index) {
        ranges[index] = GetRange(groupedSplits[index]);
    }
    return ranges;
}

TQueryStatistics CoordinateAndExecute(
    TPlanFragmentPtr fragment,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& refiners,
    bool isOrdered,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop,
    IFunctionRegistryPtr functionRegistry)
{
    auto query = fragment->Query;
    auto Logger = BuildLogger(query);

    LOG_DEBUG("Begin coordinating query");

    TConstQueryPtr topQuery;
    std::vector<TConstQueryPtr> subqueries;
    std::tie(topQuery, subqueries) = CoordinateQuery(query, refiners, functionRegistry);

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
        TFuture <TQueryStatistics> statistics;
        std::tie(reader, statistics) = evaluateSubquery(subquery, index);

        subqueryHolders.push_back(MakeHolder(statistics, false));

        ++index;

        return reader;
    };

    int topReaderConcurrency = isOrdered ? 1 : subqueries.size();
    auto topReader = CreateUnorderedSchemafulReader(std::move(subqueryReaderCreator), topReaderConcurrency);
    auto queryStatistics = evaluateTop(topQuery, std::move(topReader), std::move(writer));

    for (int index = 0; index < subqueryHolders.size(); ++index) {
        auto subQueryStatisticsOrError = WaitFor(subqueryHolders[index].Get());
        if (subQueryStatisticsOrError.IsOK()) {
            const auto& subQueryStatistics = subQueryStatisticsOrError.ValueOrThrow();
            LOG_DEBUG("Subquery has finished (SubQueryId: %v, Statistics: {%v})",
                subqueries[index]->Id,
                subQueryStatistics);
            queryStatistics += subQueryStatistics;
        } else {
            LOG_DEBUG(subQueryStatisticsOrError, "Subquery has failed (SubQueryId: %v)",
                subqueries[index]->Id);
        }
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"
#include "plan_helpers.h"
#include "plan_fragment.h"
#include "range_inferrer.h"

#include <core/logging/log.h>

#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unordered_schemaful_reader.h>
#include <ytlib/new_table_client/ordered_schemaful_reader.h>

#include <ytlib/tablet_client/public.h>

#include <numeric>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

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
    subqueryPattern->KeyColumns = query->KeyColumns;
    subqueryPattern->JoinClause = query->JoinClause;

    auto topQuery = New<TQuery>(
        query->InputRowLimit,
        query->OutputRowLimit);

    topQuery->OrderClause = query->OrderClause;
    topQuery->Limit = query->Limit;

    if (query->GroupClause) {
        subqueryPattern->GroupClause = query->GroupClause;
        if (refiners.size() > 1) {
            auto groupClause = New<TGroupClause>();
            groupClause->GroupedTableSchema = query->GroupClause->GroupedTableSchema;

            auto& finalGroupItems = groupClause->GroupItems;
            for (const auto& groupItem : query->GroupClause->GroupItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    NullSourceLocation,
                    groupItem.Expression->Type,
                    groupItem.Name);
                finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
            }

            auto& finalAggregateItems = groupClause->AggregateItems;
            for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    NullSourceLocation,
                    aggregateItem.Expression->Type,
                    aggregateItem.Name);
                finalAggregateItems.emplace_back(
                    std::move(referenceExpr),
                    aggregateItem.AggregateFunction,
                    aggregateItem.Name);
            }

            topQuery->GroupClause = groupClause;
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

    std::vector<TConstQueryPtr> subqueries;

    for (const auto& refiner : refiners) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(*subqueryPattern);
        subquery->Id = TGuid::Create();

        if (query->WhereClause) {
            subquery->WhereClause = refiner(query->WhereClause, subquery->TableSchema, subquery->KeyColumns);
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
    bool verboseLogging)
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
    TConstQueryPtr query,
    const TDataSources& sources,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging)
{
    return GetPrunedRanges(
        query->WhereClause,
        query->TableSchema,
        query->KeyColumns,
        sources,
        rowBuffer,
        evaluatorCache,
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging);
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
    std::function<TQueryStatistics(TConstQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop)
{
    auto query = fragment->Query;
    auto Logger = BuildLogger(query);

    LOG_DEBUG("Begin coordinating query");

    TConstQueryPtr topQuery;
    std::vector<TConstQueryPtr> subqueries;
    std::tie(topQuery, subqueries) = CoordinateQuery(query, refiners);

    LOG_DEBUG("Finished coordinating query");

    std::vector<ISchemafulReaderPtr> splitReaders;

    ISchemafulReaderPtr topReader;
    // Use TFutureHolder to prevent leaking subqueries.
    std::vector<TFutureHolder<TQueryStatistics>> subqueryHolders;

    if (isOrdered) {
        int index = 0;

        topReader = CreateOrderedSchemafulReader([&, index] () mutable -> ISchemafulReaderPtr {
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
        });
    } else {
        for (int index = 0; index < subqueries.size(); ++index) {
            auto subquery = subqueries[index];

            ISchemafulReaderPtr reader;
            TFuture<TQueryStatistics> statistics;
            std::tie(reader, statistics) = evaluateSubquery(subquery, index);

            splitReaders.push_back(reader);
            subqueryHolders.push_back(statistics);
        }

        topReader = CreateUnorderedSchemafulReader(splitReaders);
    }

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


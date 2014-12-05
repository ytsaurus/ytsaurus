#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"

#include "plan_helpers.h"

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/protobuf_helpers.h>

#include <core/tracing/trace_context.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NVersionedTableClient;

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TKeyRange>& ranges)
{
    auto Logger = BuildLogger(query);

    std::vector<TConstQueryPtr> subqueries;

    auto subqueryInputRowLimit = ranges.empty()
        ? 0
        : query->GetInputRowLimit() / ranges.size();

    for (const auto& keyRange : ranges) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(
            subqueryInputRowLimit,
            query->GetOutputRowLimit());

        subquery->TableSchema = query->TableSchema;
        subquery->KeyColumns = query->KeyColumns;

        // Set predicate
        int rangeSize = std::min(keyRange.first.GetCount(), keyRange.second.GetCount());

        size_t commonPrefixSize = 0;
        while (commonPrefixSize < rangeSize) {
            commonPrefixSize++;
            if (keyRange.first[commonPrefixSize - 1] != keyRange.second[commonPrefixSize - 1]) {
                break;
            }
        }

        if (query->Predicate) {
            subquery->Predicate = RefinePredicate(keyRange, commonPrefixSize, query->Predicate, subquery->KeyColumns);
        }

        // Set group clause
        subquery->GroupClause = query->GroupClause;

        // Set project clause
        if (!query->GroupClause) {
            subquery->ProjectClause = query->ProjectClause;
        }

        subqueries.push_back(subquery);
    }

    auto topQuery = New<TQuery>(
        query->GetInputRowLimit(),
        query->GetOutputRowLimit());

    if (query->GroupClause) {
        topQuery->TableSchema = query->GroupClause->GetTableSchema();

        topQuery->GroupClause.Emplace();

        auto& finalGroupItems = topQuery->GroupClause->GroupItems;
        for (const auto& groupItem : query->GroupClause->GroupItems) {
            auto referenceExpr = New<TReferenceExpression>(
                NullSourceLocation,
                groupItem.Expression->Type,
                groupItem.Name);
            finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
        }

        auto& finalAggregateItems = topQuery->GroupClause->AggregateItems;
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

        topQuery->ProjectClause = query->ProjectClause;
    } else {
        topQuery->TableSchema = query->GetTableSchema();
    }

    return std::make_pair(topQuery, subqueries);
}

TDataSplits GetPrunedSplits(
    const TConstQueryPtr& query,
    const TDataSplits& splits)
{
    auto Logger = BuildLogger(query);

    TRowBuffer rowBuffer;
    auto predicateConstraints = query->Predicate
        ? ExtractMultipleConstraints(query->Predicate, query->KeyColumns, &rowBuffer)
        : TKeyTrieNode::Universal();

    auto keyRangeFormatter = [] (const TKeyRange& range) -> Stroka {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    LOG_DEBUG("Splitting %v splits according to ranges", splits.size());

    TDataSplits prunedSplits;
    for (const auto& split : splits) {
        auto originalRange = GetBothBoundsFromDataSplit(split);

        std::vector<TKeyRange> ranges = 
            GetRangesFromTrieWithinRange(originalRange, predicateConstraints);

        for (const auto& range : ranges) {
            auto splitCopy = split;

            LOG_DEBUG("Narrowing split %v key range from %v to %v",
                    GetObjectIdFromDataSplit(splitCopy),
                    keyRangeFormatter(originalRange),
                    keyRangeFormatter(range));
            SetBothBounds(&splitCopy, range);

            prunedSplits.push_back(std::move(splitCopy));
        }
    }

    return prunedSplits;
}

TKeyRange GetRange(const TDataSplits& splits)
{
    if (!splits.empty()) {
        TKeyRange keyRange = GetBothBoundsFromDataSplit(splits[0]);
        auto keyColumns = GetKeyColumnsFromDataSplit(splits[0]);

        for (size_t i = 1; i < splits.size(); ++i) {
            keyRange = Unite(keyRange, GetBothBoundsFromDataSplit(splits[i]));
        }

        return keyRange;
    } else {
        return TKeyRange();
    }
}

std::vector<TKeyRange> GetRanges(const TGroupedDataSplits& groupedSplits)
{
    std::vector<TKeyRange> ranges(groupedSplits.size());
    for (size_t splitIndex = 0; splitIndex < groupedSplits.size(); ++splitIndex) {
        ranges[splitIndex] = GetRange(groupedSplits[splitIndex]);
    }
    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


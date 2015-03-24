#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "key_trie.h"

#include <core/logging/log.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TKeyRange>& ranges,
    bool refinePredicates);

TDataSources GetPrunedSources(
    const TConstExpressionPtr& predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TDataSources& sources,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    bool verboseLogging);

TDataSources GetPrunedSources(
    const TConstQueryPtr& query,
    const TDataSources& sources,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    bool verboseLogging);

TKeyRange GetRange(const TDataSources& sources);

std::vector<TKeyRange> GetRanges(const std::vector<TDataSources>& groupedSplits);

typedef std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> TEvaluateResult;

TQueryStatistics CoordinateAndExecute(
    const TPlanFragmentPtr& fragment,
    ISchemafulWriterPtr writer,
    bool isOrdered,
    const std::vector<TKeyRange>& ranges,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstQueryPtr&, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop,
    bool refinePredicates = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "function_registry.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TConstExpressionPtr(
    const TConstExpressionPtr& expr,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns)> TRefiner;

typedef std::vector<std::vector<TKeyRange>> TGroupedRanges;

TGroupedRanges GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TDataSources& sources,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    i64 rangeExpansionLimit,
    bool verboseLogging);

TGroupedRanges GetPrunedRanges(
    const TConstQueryPtr& query,
    const TDataSources& sources,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    i64 rangeExpansionLimit,
    bool verboseLogging);

TKeyRange GetRange(const TDataSources& sources);

std::vector<TKeyRange> GetRanges(const std::vector<TDataSources>& groupedSplits);

typedef std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> TEvaluateResult;

TQueryStatistics CoordinateAndExecute(
    const TPlanFragmentPtr& fragment,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& ranges,
    bool isOrdered,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstQueryPtr&, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


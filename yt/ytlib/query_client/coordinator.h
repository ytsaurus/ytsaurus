#pragma once

#include "public.h"
#include "callbacks.h"
#include "function_registry.h"
#include "plan_fragment.h"
#include "query_statistics.h"

namespace NYT {
namespace NQueryClient {

extern const NLogging::TLogger QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TConstExpressionPtr(
    TConstExpressionPtr expr,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns)> TRefiner;

TRowRanges GetPrunedRanges(
    TConstExpressionPtr predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    NObjectClient::TObjectId tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging,
    const NLogging::TLogger& Logger = QueryClientLogger);

TRowRanges GetPrunedRanges(
    TConstQueryPtr query,
    NObjectClient::TObjectId tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging);

TRowRange GetRange(const std::vector<TDataRange>& sources);

typedef std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> TEvaluateResult;

TQueryStatistics CoordinateAndExecute(
    TConstQueryPtr query,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& ranges,
    bool isOrdered,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop,
    IFunctionRegistryPtr functionRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


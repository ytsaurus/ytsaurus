#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"
#include "query_statistics.h"

#include <yt/core/logging/log.h>

namespace NYT {
namespace NQueryClient {

extern const NLogging::TLogger QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TConstExpressionPtr(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns)> TRefiner;

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
    const NLogging::TLogger& Logger = QueryClientLogger);

TRowRanges GetPrunedRanges(
    TConstQueryPtr query,
    NObjectClient::TObjectId tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    ui64 rangeExpansionLimit,
    bool verboseLogging);

typedef std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> TEvaluateResult;

TQueryStatistics CoordinateAndExecute(
    TConstQueryPtr query,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& ranges,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


#pragma once

#include "public.h"
#include "callbacks.h"
#include "query.h"
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
    const NObjectClient::TObjectId& tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    const NLogging::TLogger& Logger = QueryClientLogger);

TRowRanges GetPrunedRanges(
    TConstQueryPtr query,
    const NObjectClient::TObjectId& tableId,
    TSharedRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

typedef std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> TEvaluateResult;

TQueryStatistics CoordinateAndExecute(
    TConstQueryPtr query,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& ranges,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstFrontQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


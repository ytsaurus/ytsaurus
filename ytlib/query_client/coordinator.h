#pragma once

#include "public.h"
#include "callbacks.h"
#include "query.h"

#include <yt/client/query_client/query_statistics.h>

#include <yt/core/logging/log.h>

namespace NYT::NQueryClient {

extern const NLogging::TLogger QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

using TRefiner = std::function<TConstExpressionPtr(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns)>;

TRowRanges GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    const NLogging::TLogger& Logger = QueryClientLogger);

TRowRanges GetPrunedRanges(
    const TConstQueryPtr& query,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

using TEvaluateResult = std::pair<
    ISchemafulReaderPtr,
    TFuture<TQueryStatistics>>;

TQueryStatistics CoordinateAndExecute(
    const TConstQueryPtr& query,
    const ISchemafulWriterPtr& writer,
    const std::vector<TRefiner>& ranges,
    std::function<TEvaluateResult(TConstQueryPtr, int)> evaluateSubquery,
    std::function<TQueryStatistics(TConstFrontQueryPtr, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient


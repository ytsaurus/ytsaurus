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
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns)>;

std::pair<TConstFrontQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TRefiner>& refiners);

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
    const IUnversionedRowsetWriterPtr& writer,
    const std::vector<TRefiner>& ranges,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstFrontQueryPtr&, const ISchemafulReaderPtr&, const IUnversionedRowsetWriterPtr&)> evaluateTop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient


#pragma once

#include "public.h"

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::pair<TConstFrontQueryPtr, TConstQueryPtr> GetDistributedQueryPattern(const TConstQueryPtr& query);

TSharedRange<TRowRange> GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchemaPtr& tableSchema,
    const TKeyColumns& keyColumns,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    TGuid queryId = {});

TSharedRange<TRowRange> GetPrunedRanges(
    const TConstQueryPtr& query,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

using TEvaluateResult = std::pair<
    ISchemafulUnversionedReaderPtr,
    TFuture<TQueryStatistics>>;

TQueryStatistics CoordinateAndExecute(
    bool ordered,
    bool prefetch,
    int splitCount,
    std::function<TEvaluateResult()> evaluateSubQuery,
    std::function<TQueryStatistics(const ISchemafulUnversionedReaderPtr&)> evaluateTopQuery);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

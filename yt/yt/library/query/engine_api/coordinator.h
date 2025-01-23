#pragma once

#include "public.h"

#include <yt/yt/library/query/base/query_common.h>

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
    const IMemoryChunkProviderPtr& memoryChunkProvider,
    bool forceLightRangeInference,
    TGuid queryId = {});

TSharedRange<TRowRange> GetPrunedRanges(
    const TConstQueryPtr& query,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    const IMemoryChunkProviderPtr& memoryChunkProvider);

struct TEvaluateResult
{
    ISchemafulUnversionedReaderPtr Reader;
    TFuture<TQueryStatistics> Statistics;
    TFuture<TFeatureFlags> ResponseFeatureFlags;
};

using TSubQueryEvaluator = std::function<TEvaluateResult()>;

using TTopQueryEvaluator = std::function<TQueryStatistics(
    const ISchemafulUnversionedReaderPtr& /*reader*/,
    TFuture<TFeatureFlags> /*responseFeatureFlags*/)>;

TQueryStatistics CoordinateAndExecute(
    bool ordered,
    bool prefetch,
    int splitCount,
    TSubQueryEvaluator evaluateSubQuery,
    TTopQueryEvaluator evaluateTopQuery);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

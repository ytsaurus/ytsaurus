#pragma once

#include "public.h"

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSubplanFutureHolders)

struct TSubplanFutureHolders final
    : public std::vector<TFutureHolder<TQueryStatistics>> // Use TFutureHolder to prevent leaking subqueries.
{ };

DEFINE_REFCOUNTED_TYPE(TSubplanFutureHolders)

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

std::pair<TDataSource, TConstQueryPtr> InferRanges(
    const IColumnEvaluatorCachePtr& columnEvaluatorCache,
    TConstQueryPtr query,
    const TDataSource& dataSource,
    const TQueryOptions& options,
    TRowBufferPtr rowBuffer,
    const IMemoryChunkProviderPtr& memoryChunkProvider,
    const NLogging::TLogger& Logger);

struct TEvaluateResult
{
    ISchemafulUnversionedReaderPtr Reader;
    TFuture<TQueryStatistics> Statistics;
    TFuture<TFeatureFlags> ResponseFeatureFlags;
};

using TSubQueryEvaluator = std::function<TEvaluateResult()>;

using TMiddleQueryEvaluator = std::function<TEvaluateResult(
    ISchemafulUnversionedReaderPtr /*reader*/,
    TFuture<TFeatureFlags> /*responseFeatureFlags*/)>;

using TTopQueryEvaluator = std::function<TQueryStatistics(
    const ISchemafulUnversionedReaderPtr& /*reader*/,
    TFuture<TFeatureFlags> /*responseFeatureFlags*/)>;

TQueryStatistics CoordinateAndExecute(
    EScanOrder scanOrder,
    bool prefetch,
    int splitCount,
    i64 offset,
    i64 limit,
    bool useAdaptiveOrderedSchemafulReader,
    TSubQueryEvaluator evaluateSubQuery,
    TTopQueryEvaluator evaluateTopQuery,
    TSubplanFutureHoldersPtr subplanHolders = nullptr);

TQueryStatistics CoordinateAndExecuteWithShuffle(
    int splitCount,
    int groupKeyPrefix,
    TSubQueryEvaluator evaluateSubQuery,
    TMiddleQueryEvaluator evaluateMiddleQuery,
    TTopQueryEvaluator evaluateTopQuery,
    const IMemoryChunkProviderPtr& memoryChunkProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

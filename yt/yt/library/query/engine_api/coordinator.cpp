#include "coordinator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::pair<TConstFrontQueryPtr, TConstQueryPtr> GetDistributedQueryPattern(
    const TConstQueryPtr& /*query*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

Y_WEAK TSharedRange<TRowRange> GetPrunedRanges(
    const TConstExpressionPtr& /*predicate*/,
    const TTableSchemaPtr& /*tableSchema*/,
    const TKeyColumns& /*keyColumns*/,
    NObjectClient::TObjectId /*tableId*/,
    const TSharedRange<TRowRange>& /*ranges*/,
    const TRowBufferPtr& /*rowBuffer*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstRangeExtractorMapPtr& /*rangeExtractors*/,
    const TQueryOptions& /*options*/,
    const IMemoryChunkProviderPtr& /*memoryChunkProvider*/,
    bool /*forceLightRangeInference*/,
    TGuid /*queryId*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

Y_WEAK TSharedRange<TRowRange> GetPrunedRanges(
    const TConstQueryPtr& /*query*/,
    NObjectClient::TObjectId /*tableId*/,
    const TSharedRange<TRowRange>& /*ranges*/,
    const TRowBufferPtr& /*rowBuffer*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstRangeExtractorMapPtr& /*rangeExtractors*/,
    const TQueryOptions& /*options*/,
    const IMemoryChunkProviderPtr& /*memoryChunkProvider*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

Y_WEAK TQueryStatistics CoordinateAndExecute(
    bool /*ordered*/,
    bool /*prefetch*/,
    int /*splitCount*/,
    TSubQueryEvaluator /*evaluateSubQuery*/,
    TTopQueryEvaluator /*evaluateTopQuery*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

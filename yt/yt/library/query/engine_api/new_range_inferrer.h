#pragma once

#include "public.h"

#include <yt/yt/library/query/base/constraints.h>
#include <yt/yt/library/query/base/query.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TConstraintExtractor = std::function<TConstraintRef(
    TConstraintsHolder* constraints,
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)>;

struct TConstraintExtractorMap
    : public TRefCounted
    , public std::unordered_map<std::string, TConstraintExtractor>
{ };

DEFINE_REFCOUNTED_TYPE(TConstraintExtractorMap)

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> CreateNewRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstConstraintExtractorMapPtr& constraintExtractors,
    const TQueryOptions& options,
    const IMemoryChunkProviderPtr& memoryChunkProvider,
    bool forceLightRangeInference);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

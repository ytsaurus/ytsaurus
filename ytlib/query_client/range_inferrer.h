#pragma once

#include "public.h"
#include "callbacks.h"
#include "column_evaluator.h"
#include "query.h"

#include <yt/client/query_client/query_statistics.h>

#include <functional>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TRangeInferrer = std::function<std::vector<TMutableRowRange>(
    const TRowRange& keyRange,
    const TRowBufferPtr& rowBuffer)>;

TRangeInferrer CreateRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


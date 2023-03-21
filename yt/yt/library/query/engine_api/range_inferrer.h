#pragma once

#include "public.h"
#include "column_evaluator.h"

#include <functional>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TRangeInferrer = std::function<std::vector<TMutableRowRange>(
    const TRowRange& keyRange,
    const TRowBufferPtr& rowBuffer)>;

TRangeInferrer CreateRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient


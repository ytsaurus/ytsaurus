#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "column_evaluator.h"
#include "function_registry.h"

#include <functional>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TRangeInferrer = std::function<std::vector<TRowRange>(
    const TRowRange& keyRange,
    const TRowBufferPtr& rowBuffer)>;

TRangeInferrer CreateRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr& functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


#pragma once

#include <yt/yt/library/query/base/builtin_function_types.h>

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFunctionRegistryBuilder> CreateFunctionRegistryBuilder(
    const TTypeInferrerMapPtr& typeInferrers,
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

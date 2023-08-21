#pragma once

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/functions_builder.h>
#include <yt/yt/library/query/base/builtin_function_registry.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFunctionRegistryBuilder> CreateProfilerFunctionRegistryBuilder(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

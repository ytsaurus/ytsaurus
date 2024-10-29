#pragma once

#include "public.h"

#include <yt/yt/library/query/base/builtin_function_registry.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/functions_builder.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFunctionProfilerMap)

struct TFunctionProfilerMap
    : public TRefCounted
    , public std::unordered_map<std::string, IFunctionCodegenPtr>
{
    const IFunctionCodegenPtr& GetFunction(const std::string& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TFunctionProfilerMap)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFunctionProfilerMap)

struct TAggregateProfilerMap
    : public TRefCounted
    , public std::unordered_map<std::string, IAggregateCodegenPtr>
{
    const IAggregateCodegenPtr& GetAggregate(const std::string& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TAggregateProfilerMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

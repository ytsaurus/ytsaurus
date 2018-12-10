#pragma once

#include "public.h"

#include "functions_common.h"

#include <yt/core/misc/ref.h>

#ifdef YT_IN_ARCADIA
#define UDF_BC(name) TSharedRef::FromString(::NResource::Find(TString("/llvm_bc/") + #name))
#else
#define UDF_BC(name) TSharedRef(name ## _bc, name ## _bc_len, nullptr)
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TFunctionRegistryBuilder
{
    TFunctionRegistryBuilder(
        const TTypeInferrerMapPtr& typeInferrers,
        const TFunctionProfilerMapPtr& functionProfilers,
        const TAggregateProfilerMapPtr& aggregateProfilers)
        : TypeInferrers_(typeInferrers)
        , FunctionProfilers_(functionProfilers)
        , AggregateProfilers_(aggregateProfilers)
    { }

    TTypeInferrerMapPtr TypeInferrers_;
    TFunctionProfilerMapPtr FunctionProfilers_;
    TAggregateProfilerMapPtr AggregateProfilers_;

    void RegisterFunction(
        const TString& functionName,
        const TString& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention,
        bool useFunctionContext = false);

    void RegisterFunction(
        const TString& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

    void RegisterFunction(
        const TString& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile);

    void RegisterAggregate(
        const TString& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

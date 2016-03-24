#pragma once

#include "public.h"

#include "functions_common.h"

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NQueryClient {

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
        const Stroka& functionName,
        const Stroka& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile,
        ICallingConventionPtr callingConvention);

    void RegisterFunction(
        const Stroka& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

    void RegisterFunction(
        const Stroka& functionName,
        const Stroka& symbolName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

    void RegisterFunction(
        const Stroka& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile);

    void RegisterFunction(
        const Stroka& functionName,
        const Stroka& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TSharedRef implementationFile);

    void RegisterAggregate(
        const Stroka& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TSharedRef implementationFile,
        ECallingConvention callingConvention);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#pragma once

#include "public.h"

#include <yt/yt/library/query/base/functions_common.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class IFunctionRegistryBuilder
{
public:
    virtual ~IFunctionRegistryBuilder() = default;

    virtual void RegisterFunction(
        const TString& functionName,
        const TString& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool useFunctionContext = false) = 0;

    virtual void RegisterFunction(
        const TString& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention) = 0;

    virtual void RegisterFunction(
        const TString& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile) = 0;

    virtual void RegisterAggregate(
        const TString& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool isFirst = false) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

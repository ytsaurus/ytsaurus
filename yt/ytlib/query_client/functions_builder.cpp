#include "functions_builder.h"
#include "functions.h"
#include "functions_cg.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TFunctionRegistryBuilder::RegisterFunction(
    const TString& functionName,
    const TString& symbolName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile,
    ICallingConventionPtr callingConvention)
{
    if (TypeInferrers_) {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            typeArgumentConstraints,
            argumentTypes,
            repeatedArgType,
            resultType));
    }
    if (FunctionProfilers_) {
        FunctionProfilers_->emplace(functionName, New<TExternalFunctionCodegen>(
            functionName, symbolName, implementationFile, callingConvention, TSharedRef()));
    }
}

void TFunctionRegistryBuilder::RegisterFunction(
    const TString& functionName,
    std::vector<TType> argumentTypes,
    TType resultType,
    TSharedRef implementationFile,
    ECallingConvention callingConvention)
{
    RegisterFunction(
        functionName,
        functionName,
        std::unordered_map<TTypeArgument, TUnionType>(),
        argumentTypes,
        EValueType::Null,
        resultType,
        implementationFile,
        GetCallingConvention(callingConvention));
}

void TFunctionRegistryBuilder::RegisterFunction(
    const TString& functionName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    std::vector<TType> argumentTypes,
    TType repeatedArgType,
    TType resultType,
    TSharedRef implementationFile)
{
    RegisterFunction(
        functionName,
        functionName,
        typeArgumentConstraints,
        argumentTypes,
        repeatedArgType,
        resultType,
        implementationFile,
        GetCallingConvention(ECallingConvention::UnversionedValue, argumentTypes.size(), repeatedArgType, false));
}

void TFunctionRegistryBuilder::RegisterAggregate(
    const TString& aggregateName,
    std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
    TType argumentType,
    TType resultType,
    TType stateType,
    TSharedRef implementationFile,
    ECallingConvention callingConvention)
{
    if (TypeInferrers_) {
        TypeInferrers_->emplace(aggregateName, New<TAggregateTypeInferrer>(
            typeArgumentConstraints,
            argumentType,
            resultType,
            stateType));
    }

    if (AggregateProfilers_) {
        AggregateProfilers_->emplace(aggregateName, New<TExternalAggregateCodegen>(
            aggregateName, implementationFile, callingConvention, TSharedRef()));
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

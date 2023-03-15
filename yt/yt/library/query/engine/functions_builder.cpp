#include "functions_builder.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionRegistryBuilder
    : public IFunctionRegistryBuilder
{
public:
    TFunctionRegistryBuilder(
        const TTypeInferrerMapPtr& typeInferrers,
        const TFunctionProfilerMapPtr& functionProfilers,
        const TAggregateProfilerMapPtr& aggregateProfilers)
        : TypeInferrersBuilder_(CreateTypeInferrerFunctionRegistryBuilder(typeInferrers))
        , FunctionProfilersBuilder_(
            CreateProfilerFunctionRegistryBuilder(
                functionProfilers,
                aggregateProfilers))
    { }

    void RegisterFunction(
        const TString& functionName,
        const TString& symbolName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool useFunctionContext) override
    {
        TypeInferrersBuilder_->RegisterFunction(
            functionName, symbolName, typeArgumentConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile, callingConvention, useFunctionContext);

        FunctionProfilersBuilder_->RegisterFunction(
            functionName, symbolName, typeArgumentConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile, callingConvention, useFunctionContext);
    }

    void RegisterFunction(
        const TString& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention) override
    {
        TypeInferrersBuilder_->RegisterFunction(
            functionName, argumentTypes, resultType,
            implementationFile, callingConvention);

        FunctionProfilersBuilder_->RegisterFunction(
            functionName, argumentTypes, resultType,
            implementationFile, callingConvention);
    }

    void RegisterFunction(
        const TString& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile) override
    {
        TypeInferrersBuilder_->RegisterFunction(
            functionName, typeArgumentConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile);

        FunctionProfilersBuilder_->RegisterFunction(
            functionName, typeArgumentConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile);
    }

    void RegisterAggregate(
        const TString& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool isFirst) override
    {
        TypeInferrersBuilder_->RegisterAggregate(
            aggregateName, typeArgumentConstraints,
            argumentType, resultType, stateType,
            implementationFile, callingConvention, isFirst);

        FunctionProfilersBuilder_->RegisterAggregate(
            aggregateName, typeArgumentConstraints,
            argumentType, resultType, stateType,
            implementationFile, callingConvention, isFirst);
    }

private:
    std::unique_ptr<IFunctionRegistryBuilder> TypeInferrersBuilder_;
    std::unique_ptr<IFunctionRegistryBuilder> FunctionProfilersBuilder_;
};

std::unique_ptr<IFunctionRegistryBuilder> CreateFunctionRegistryBuilder(
    const TTypeInferrerMapPtr& typeInferrers,
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers)
{
    return std::make_unique<TFunctionRegistryBuilder>(
        typeInferrers, functionProfilers, aggregateProfilers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

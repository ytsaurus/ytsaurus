#include "functions_builder.h"
#include "builtin_function_profiler.h"

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
        const std::string& functionName,
        const std::string& symbolName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool useFunctionContext) override
    {
        TypeInferrersBuilder_->RegisterFunction(
            functionName, symbolName, typeParameterConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile, callingConvention, useFunctionContext);

        FunctionProfilersBuilder_->RegisterFunction(
            functionName, symbolName, typeParameterConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile, callingConvention, useFunctionContext);
    }

    void RegisterFunction(
        const std::string& functionName,
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
        const std::string& functionName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf implementationFile) override
    {
        TypeInferrersBuilder_->RegisterFunction(
            functionName, typeParameterConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile);

        FunctionProfilersBuilder_->RegisterFunction(
            functionName, typeParameterConstraints,
            argumentTypes, repeatedArgType, resultType,
            implementationFile);
    }

    void RegisterAggregate(
        const std::string& aggregateName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType resultType,
        TType stateType,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool isFirst) override
    {
        TypeInferrersBuilder_->RegisterAggregate(
            aggregateName, typeParameterConstraints,
            argumentTypes, resultType, stateType,
            implementationFile, callingConvention, isFirst);

        FunctionProfilersBuilder_->RegisterAggregate(
            aggregateName, typeParameterConstraints,
            std::move(argumentTypes), resultType, stateType,
            implementationFile, callingConvention, isFirst);
    }

private:
    const std::unique_ptr<IFunctionRegistryBuilder> TypeInferrersBuilder_;
    const std::unique_ptr<IFunctionRegistryBuilder> FunctionProfilersBuilder_;
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

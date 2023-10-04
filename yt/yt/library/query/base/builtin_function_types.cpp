#include "builtin_function_types.h"

#include "functions_builder.h"
#include "functions.h"

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTypeInferrerFunctionRegistryBuilder
    : public IFunctionRegistryBuilder
{
public:
    explicit TTypeInferrerFunctionRegistryBuilder(const TTypeInferrerMapPtr& typeInferrers)
        : TypeInferrers_(typeInferrers)
    { }

    void RegisterFunction(
        const TString& functionName,
        const TString& /*symbolName*/,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/,
        bool /*useFunctionContext*/) override
    {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            std::move(typeArgumentConstraints),
            std::move(argumentTypes),
            repeatedArgType,
            resultType));
    }

    void RegisterFunction(
        const TString& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/) override
    {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            std::unordered_map<TTypeArgument, TUnionType>{},
            std::move(argumentTypes),
            EValueType::Null,
            resultType));
    }

    void RegisterFunction(
        const TString& functionName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf /*implementationFile*/) override
    {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            std::move(typeArgumentConstraints),
            std::move(argumentTypes),
            repeatedArgType,
            resultType));
    }

    void RegisterAggregate(
        const TString& aggregateName,
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/,
        bool /*isFirst*/) override
    {
        TypeInferrers_->emplace(aggregateName, New<TAggregateTypeInferrer>(
            typeArgumentConstraints,
            argumentType,
            resultType,
            stateType));
    }

private:
    const TTypeInferrerMapPtr TypeInferrers_;
};

std::unique_ptr<IFunctionRegistryBuilder> CreateTypeInferrerFunctionRegistryBuilder(
    const TTypeInferrerMapPtr& typeInferrers)
{
    return std::make_unique<TTypeInferrerFunctionRegistryBuilder>(typeInferrers);
}

////////////////////////////////////////////////////////////////////////////////

TConstTypeInferrerMapPtr CreateBuiltinTypeInferrers()
{
    auto result = New<TTypeInferrerMap>();

    result->emplace("if", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{ EValueType::Boolean, 0, 0 },
        0));

    result->emplace("is_prefix", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{ EValueType::String, EValueType::String },
        EValueType::Boolean));

    result->emplace("is_null", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{0},
        EValueType::Null,
        EValueType::Boolean));

    result->emplace("is_nan", New<TFunctionTypeInferrer>(
        std::vector<TType>{EValueType::Double},
        EValueType::Boolean));

    auto typeArg = 0;
    auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    castConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Any};

    result->emplace("int64", New<TFunctionTypeInferrer>(
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Int64));

    result->emplace("uint64", New<TFunctionTypeInferrer>(
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Uint64));

    result->emplace("double", New<TFunctionTypeInferrer>(
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Double));

    {
        auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
        castConstraints[typeArg] = std::vector<EValueType>{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Any,
        };

        result->emplace("boolean", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{typeArg},
            EValueType::Null,
            EValueType::Boolean));
    }

    {
        auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
        castConstraints[typeArg] = std::vector<EValueType>{
            EValueType::String,
            EValueType::Any,
        };

        result->emplace("string", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{typeArg},
            EValueType::Null,
            EValueType::String));
    }

    result->emplace("if_null", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{0, 0},
        0));

    auto sumConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    sumConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    result->emplace("sum", New<TAggregateTypeInferrer>(
        sumConstraints,
        typeArg,
        typeArg,
        typeArg));

    auto constraints = std::unordered_map<TTypeArgument, TUnionType>();
    constraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String};
    for (const auto& name : {"min", "max"}) {
        result->emplace(name, New<TAggregateTypeInferrer>(
            constraints,
            typeArg,
            typeArg,
            typeArg));
    }

    TTypeInferrerFunctionRegistryBuilder builder{result.Get()};
    RegisterBuiltinFunctions(&builder);

    return result;
}

const TConstTypeInferrerMapPtr GetBuiltinTypeInferrers()
{
    static const auto builtinTypeInferrers = CreateBuiltinTypeInferrers();
    return builtinTypeInferrers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

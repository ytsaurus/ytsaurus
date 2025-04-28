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
        const std::string& functionName,
        const std::string& /*symbolName*/,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/,
        bool /*useFunctionContext*/) override
    {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            std::move(typeParameterConstraints),
            std::move(argumentTypes),
            repeatedArgType,
            resultType));
    }

    void RegisterFunction(
        const std::string& functionName,
        std::vector<TType> argumentTypes,
        TType resultType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/) override
    {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            std::unordered_map<TTypeParameter, TUnionType>{},
            std::move(argumentTypes),
            EValueType::Null,
            resultType));
    }

    void RegisterFunction(
        const std::string& functionName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType resultType,
        TStringBuf /*implementationFile*/) override
    {
        TypeInferrers_->emplace(functionName, New<TFunctionTypeInferrer>(
            std::move(typeParameterConstraints),
            std::move(argumentTypes),
            repeatedArgType,
            resultType));
    }

    void RegisterAggregate(
        const std::string& aggregateName,
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType resultType,
        TType stateType,
        TStringBuf /*implementationFile*/,
        ECallingConvention /*callingConvention*/,
        bool /*isFirst*/) override
    {
        TypeInferrers_->emplace(aggregateName, New<TAggregateFunctionTypeInferrer>(
            typeParameterConstraints,
            std::move(argumentTypes),
            stateType,
            resultType));
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

    const TTypeParameter primitive = 0;

    result->emplace("if", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::Boolean, primitive, primitive},
        primitive));

    result->emplace("is_prefix", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean));

    result->emplace("is_null", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{primitive},
        EValueType::Null,
        EValueType::Boolean));

    result->emplace("is_nan", New<TFunctionTypeInferrer>(
        std::vector<TType>{EValueType::Double},
        EValueType::Boolean));

    result->emplace("is_finite", New<TFunctionTypeInferrer>(
        std::vector<TType>{EValueType::Double},
        EValueType::Boolean));

    const TTypeParameter castable = 0;

    {
        auto castConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        castConstraints[castable] = std::vector<EValueType>{
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Any,
        };

        result->emplace("int64", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{castable},
            EValueType::Null,
            EValueType::Int64));

        result->emplace("uint64", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{castable},
            EValueType::Null,
            EValueType::Uint64));

        result->emplace("double", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{castable},
            EValueType::Null,
            EValueType::Double));
    }

    {
        auto castConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        castConstraints[castable] = std::vector<EValueType>{
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Any,
        };

        result->emplace("boolean", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{castable},
            EValueType::Null,
            EValueType::Boolean));
    }

    {
        auto castConstraints = std::unordered_map<TTypeParameter, TUnionType>();
        castConstraints[castable] = std::vector<EValueType>{
            EValueType::String,
            EValueType::Any,
        };

        result->emplace("string", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{castable},
            EValueType::Null,
            EValueType::String));
    }

    result->emplace("if_null", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{primitive, primitive},
        primitive));

    const TTypeParameter nullable = 0;

    std::unordered_map<TTypeParameter, TUnionType> coalesceConstraints;
    coalesceConstraints[nullable] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Composite,
        EValueType::Any,
    };
    result->emplace("coalesce", New<TFunctionTypeInferrer>(
        coalesceConstraints,
        std::vector<TType>{},
        nullable,
        nullable));

    const TTypeParameter summable = 0;
    auto sumConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    sumConstraints[summable] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
    };

    result->emplace("sum", New<TAggregateFunctionTypeInferrer>(
        sumConstraints,
        summable,
        summable,
        summable));

    const TTypeParameter comparable = 0;
    auto minMaxConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    minMaxConstraints[comparable] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
    };
    for (const auto& name : {"min", "max"}) {
        result->emplace(name, New<TAggregateFunctionTypeInferrer>(
            minMaxConstraints,
            comparable,
            comparable,
            comparable));
    }

    auto argMinMaxConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    argMinMaxConstraints[0] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
    };
    for (const auto& name : {"argmin", "argmax"}) {
        result->emplace(name, New<TAggregateFunctionTypeInferrer>(
            argMinMaxConstraints,
            std::vector<TType>{1, 0},
            EValueType::String,
            1));
    }

    result->emplace("avg", New<TAggregateFunctionTypeInferrer>(
        sumConstraints,
        std::vector<TType>{summable},
        EValueType::String,
        EValueType::Double));

    result->emplace("array_agg", New<TArrayAggTypeInferrer>(
        std::unordered_map<TTypeParameter, TUnionType>{},
        std::vector<TType>{
            TUnionType{
                EValueType::String,
                EValueType::Uint64,
                EValueType::Int64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::Any,
                EValueType::Composite,
            },
            EValueType::Boolean,
        },
        EValueType::String,
        EValueType::Any));

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

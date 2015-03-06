#include "function_registry.h"
#include "cg_fragment_compiler.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TFunctionRegistry::TFunctionRegistry() {
    //TODO: add range builders
    auto getCodegenBuilder = [] (Stroka functionName) {
      return [] (
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        Stroka name) {
            return MakeCodegenFunctionExpr("is_prefix", codegenArgs, type, name);
        };
    };


    GetFunctionRegistry()->RegisterFunction(
        "if",
        std::vector<TType>({ EValueType::Boolean, 0, 0 }),
        0,
        getCodegenBuilder("if"));

    GetFunctionRegistry()->RegisterFunction(
        "is_prefix",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean,
        getCodegenBuilder("is_prefix"));

    GetFunctionRegistry()->RegisterFunction(
        "is_substr",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean,
        getCodegenBuilder("is_substr"));

    GetFunctionRegistry()->RegisterFunction(
        "lower",
        std::vector<TType>({ EValueType::String }),
        EValueType::String,
        getCodegenBuilder("lower"));

    auto hashTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String });

    GetFunctionRegistry()->RegisterVariadicFunction(
        "simple_hash",
        std::vector<TType>({ hashTypes }),
        hashTypes,
        EValueType::String,
        getCodegenBuilder("simple_hash"));

    GetFunctionRegistry()->RegisterFunction(
        "is_null",
        std::vector<TType>({ 0 }),
        EValueType::Boolean,
        getCodegenBuilder("is_null"));

    auto castTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double });

    GetFunctionRegistry()->RegisterFunction(
        "int64",
        std::vector<TType>({ castTypes }),
        EValueType::Int64,
        getCodegenBuilder("int64"));

    GetFunctionRegistry()->RegisterFunction(
        "uint64",
        std::vector<TType>({ castTypes }),
        EValueType::Uint64,
        getCodegenBuilder("uint64"));

    GetFunctionRegistry()->RegisterFunction(
        "double",
        std::vector<TType>({ castTypes }),
        EValueType::Double,
        getCodegenBuilder("double"));
}

void TFunctionRegistry::RegisterFunctionImpl(const Stroka& functionName, const TFunctionMetadata& functionMetadata)
{
    YCHECK(registeredFunctions.count(functionName) == 0);
    std::pair<Stroka, TFunctionMetadata> kv(functionName, functionMetadata);
    registeredFunctions.insert(kv);
}

//TODO: graceful handling of unregistered function lookups
TFunctionRegistry::TFunctionMetadata& TFunctionRegistry::LookupMetadata(const Stroka& functionName)
{
    return registeredFunctions.at(functionName);
}

void TFunctionRegistry::RegisterFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType resultType,
    TCodegenBuilder bodyBuilder,
    TRangeBuilder rangeBuilder)
{
    TFunctionMetadata functionMetadata(
        argumentTypes,
        EValueType::Null,
        resultType,
        bodyBuilder,
        rangeBuilder);
    RegisterFunctionImpl(functionName, functionMetadata);
}

void TFunctionRegistry::RegisterVariadicFunction(
    const Stroka& functionName,
    std::vector<TType> argumentTypes,
    TType repeatedArgumentType,
    TType resultType,
    TCodegenBuilder bodyBuilder,
    TRangeBuilder rangeBuilder)
{
    TFunctionMetadata functionMetadata(
        argumentTypes,
        repeatedArgumentType,
        resultType,
        bodyBuilder,
        rangeBuilder);
    RegisterFunctionImpl(functionName, functionMetadata);
}


bool TFunctionRegistry::IsRegistered(const Stroka& functionName)
{
    return registeredFunctions.count(functionName) != 0;
}

std::vector<TType> TFunctionRegistry::GetArgumentTypes(
    const Stroka& functionName)
{
    return LookupMetadata(functionName).ArgumentTypes;
}

TType TFunctionRegistry::GetRepeatedArgumentType(
    const Stroka& functionName)
{
    return LookupMetadata(functionName).RepeatedArgumentType;
}

TType TFunctionRegistry::GetResultType(
    const Stroka& functionName)
{
    return LookupMetadata(functionName).ResultType;
}

TCodegenBuilder TFunctionRegistry::GetCodegenBuilder(
    const Stroka& functionName,
    std::vector<EValueType> argumentTypes)
{
    return LookupMetadata(functionName).BodyBuilder;
}

TRangeBuilder TFunctionRegistry::ExtractRangeConstraints(
    const Stroka& functionName,
    std::vector<EValueType> argumentTypes)
{
    return LookupMetadata(functionName).RangeBuilder;
}


////////////////////////////////////////////////////////////////////////////////
} // namespace NQueryClient
} // namespace NYT

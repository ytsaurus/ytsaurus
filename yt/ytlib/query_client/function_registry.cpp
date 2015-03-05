#include "function_registry.h"

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

    registry->RegisterFunction(
        "if",
        std::vector<TType>({ EValueType::Boolean, 0, 0 }),
        0,
        getCodegenBuilder("if"));

    registry->RegisterFunction(
        "is_prefix",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean,
        getCodegenBuilder("is_prefix"));

    registry->RegisterFunction(
        "is_substr",
        std::vector<TType>({ EValueType::String, EValueType::String }),
        EValueType::Boolean,
        getCodegenBuilder("is_substr"));

    registry->RegisterFunction(
        "lower",
        std::vector<TType>({ EValueType::String }),
        EValueType::String,
        getCodegenBuilder("lower"));

    auto hashTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String });

    registry->RegisterVariadicFunction(
        "simple_hash",
        std::vector<TType>({ hashTypes }),
        hashTypes,
        EValueType::String,
        getCodegenBuilder("simple_hash"));

    registry->RegisterFunction(
        "is_null",
        std::vector<TType>({ 0 }),
        EValueType::Boolean,
        getCodegenBuilder("is_null"));

    auto castTypes = std::set<EValueType>({
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double });

    registry->RegisterFunction(
        "int64",
        std::vector<TType>({ castTypes }),
        EValueType::Int64,
        getCodegenBuilder("int64"));

    registry->RegisterFunction(
        "uint64",
        std::vector<TType>({ castTypes }),
        EValueType::Uint64,
        getCodegenBuilder("uint64"));

    registry->RegisterFunction(
        "double",
        std::vector<TType>({ castTypes }),
        EValueType::Double,
        getCodegenBuilder("double"));
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NQueryClient
} // namespace NYT

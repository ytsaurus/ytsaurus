#include "function_registry.h"
#include "builtin_functions.h"
#include "user_defined_functions.h"
#include "udf/avg.h"
#include "udf/double_cast.h"
#include "udf/farm_hash.h"
#include "udf/hyperloglog.h"
#include "udf/int64.h"
#include "udf/is_null.h"
#include "udf/is_substr.h"
#include "udf/lower.h"
#include "udf/concat.h"
#include "udf/max.h"
#include "udf/min.h"
#include "udf/regex.h"
#include "udf/sleep.h"
#include "udf/sum.h"
#include "udf/uint64.h"
#include "udf/dates.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctions(TIntrusivePtr<TFunctionRegistry>& registry)
{
    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        TSharedRef(
            is_substr_bc,
            is_substr_bc_len,
            nullptr),
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        TSharedRef(
            lower_bc,
            lower_bc_len,
            nullptr),
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "concat",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::String,
        TSharedRef(
            concat_bc,
            concat_bc_len,
            nullptr),
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        TSharedRef(
            sleep_bc,
            sleep_bc_len,
            nullptr),
        ECallingConvention::Simple));

    TUnionType hashTypes = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String};

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "farm_hash",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{},
        hashTypes,
        EValueType::Uint64,
        TSharedRef(
            farm_hash_bc,
            farm_hash_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_null",
        std::vector<TType>{0},
        EValueType::Boolean,
        TSharedRef(
            is_null_bc,
            is_null_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));

    auto typeArg = 0;
    auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    castConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "int64",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Int64,
        TSharedRef(
            int64_bc,
            int64_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "uint64",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Uint64,
        TSharedRef(
            uint64_bc,
            uint64_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "double",
        "double_cast",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Double,
        TSharedRef(
            double_cast_bc,
            double_cast_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "regex_full_match",
        "regex_full_match",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "regex_partial_match",
        "regex_partial_match",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "regex_replace_first",
        "regex_replace_first",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "regex_replace_all",
        "regex_replace_all",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "regex_extract",
        "regex_extract",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "regex_escape",
        "regex_escape",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true)));


    registry->RegisterFunction(New<TIfFunction>());
    registry->RegisterFunction(New<TIsPrefixFunction>());

    auto constraints = std::unordered_map<TTypeArgument, TUnionType>();
    constraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::String};
    auto sumConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    sumConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "sum",
        sumConstraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            sum_bc,
            sum_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "min",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            min_bc,
            min_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "max",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            max_bc,
            max_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "avg",
        std::unordered_map<TTypeArgument, TUnionType>(),
        EValueType::Int64,
        EValueType::Double,
        EValueType::String,
        TSharedRef(
            avg_bc,
            avg_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "cardinality",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<EValueType>{
            EValueType::String,
            EValueType::Uint64,
            EValueType::Int64,
            EValueType::Double,
            EValueType::Boolean},
        EValueType::Uint64,
        EValueType::String,
        TSharedRef(
            hyperloglog_bc,
            hyperloglog_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));

    registry->RegisterFunction(New<TUserDefinedFunction>(
            "format_timestamp",
            std::vector<TType>{EValueType::Int64, EValueType::String},
            EValueType::String,
            TSharedRef(
                dates_bc,
                dates_bc_len,
                nullptr),
            ECallingConvention::Simple));

    std::vector<Stroka> timestampFloorFunctions = {
        "timestamp_floor_hour",
        "timestamp_floor_day",
        "timestamp_floor_week",
        "timestamp_floor_month",
        "timestamp_floor_year"};

    for (const auto& name : timestampFloorFunctions) {
        registry->RegisterFunction(New<TUserDefinedFunction>(
            name,
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            TSharedRef(
                dates_bc,
                dates_bc_len,
                nullptr),
            ECallingConvention::Simple));
    }

}

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry()
{
    auto registry = New<TFunctionRegistry>();
    RegisterBuiltinFunctions(registry);
    return registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#include "builtin_function_registry.h"

#include "functions.h"

#include <library/cpp/resource/resource.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctions(IFunctionRegistryBuilder* builder)
{
    builder->RegisterFunction(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        "is_substr",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        "lower",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "length",
        std::vector<TType>{EValueType::String},
        EValueType::Int64,
        "length",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "yson_length",
        std::vector<TType>{TUnionType{EValueType::Any, EValueType::Composite}},
        EValueType::Int64,
        "yson_length",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "concat",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::String,
        "concat",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        "sleep",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "farm_hash",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::String,
        },
        EValueType::Uint64,
        "farm_hash");

    builder->RegisterFunction(
        "bigb_hash",
        std::vector<TType>{EValueType::String},
        EValueType::Uint64,
        "bigb_hash",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "make_map",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Double,
            EValueType::String,
            EValueType::Any,
            EValueType::Composite,
        },
        EValueType::Any,
        "make_map");

    builder->RegisterFunction(
        "make_list",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{},
        TUnionType{
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Boolean,
            EValueType::Double,
            EValueType::String,
            EValueType::Any,
            EValueType::Composite,
        },
        EValueType::Any,
        "make_list");

    builder->RegisterFunction(
        "make_entity",
        std::vector<TType>{},
        EValueType::Any,
        "make_entity",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "numeric_to_string",
        std::vector<TType>{
            TUnionType{
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
            }},
        EValueType::String,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "parse_int64",
        std::vector<TType>{EValueType::String},
        EValueType::Int64,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "parse_uint64",
        std::vector<TType>{EValueType::String},
        EValueType::Uint64,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "parse_double",
        std::vector<TType>{EValueType::String},
        EValueType::Double,
        "str_conv",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "is_finite",
        std::vector<TType>{EValueType::Double},
        EValueType::Boolean,
        "is_finite",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "regex_full_match",
        "regex_full_match",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_partial_match",
        "regex_partial_match",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_replace_first",
        "regex_replace_first",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_replace_all",
        "regex_replace_all",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_extract",
        "regex_extract",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    builder->RegisterFunction(
        "regex_escape",
        "regex_escape",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::String},
        EValueType::Null,
        EValueType::String,
        "regex",
        ECallingConvention::UnversionedValue,
        true);

    const TTypeParameter typeParameter = 0;
    auto anyConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraints[typeParameter] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
        EValueType::Any
    };

    builder->RegisterAggregate(
        "first",
        anyConstraints,
        {typeParameter},
        typeParameter,
        typeParameter,
        "first",
        ECallingConvention::UnversionedValue,
        true);

    auto xdeltaConstraints = std::unordered_map<TTypeParameter, TUnionType>();
    xdeltaConstraints[typeParameter] = {
        EValueType::Null,
        EValueType::String,
    };
    builder->RegisterAggregate(
        "xdelta",
        xdeltaConstraints,
        {typeParameter},
        typeParameter,
        typeParameter,
        "xdelta",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "cardinality",
        std::unordered_map<TTypeParameter, TUnionType>(),
        {TUnionType{
            EValueType::String,
            EValueType::Uint64,
            EValueType::Int64,
            EValueType::Double,
            EValueType::Boolean,
        }},
        EValueType::Uint64,
        EValueType::String,
        "hyperloglog",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "array_agg",
        std::unordered_map<TTypeParameter, TUnionType>(),
        {
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
        EValueType::Any,
        EValueType::String,
        "array_agg",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "dict_sum",
        std::unordered_map<TTypeParameter, TUnionType>{},
        {TUnionType{EValueType::Any, EValueType::Composite}},
        EValueType::Any,
        EValueType::Any,
        "dict_sum",
        ECallingConvention::UnversionedValue);

    for (const auto& name : std::vector<std::string>{
        "format_timestamp",
        "format_timestamp_localtime",
    }) {
        builder->RegisterFunction(
            name,
            std::vector<TType>{EValueType::Int64, EValueType::String},
            EValueType::String,
            "dates",
            ECallingConvention::Simple);
    }

    for (const auto& name : std::vector<std::string>{
        "timestamp_floor_hour",
        "timestamp_floor_day",
        "timestamp_floor_week",
        "timestamp_floor_month",
        "timestamp_floor_year",
        "timestamp_floor_hour_localtime",
        "timestamp_floor_day_localtime",
        "timestamp_floor_week_localtime",
        "timestamp_floor_month_localtime",
        "timestamp_floor_year_localtime",
    }) {
        builder->RegisterFunction(
            name,
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            "dates",
            ECallingConvention::Simple);
    }

    builder->RegisterFunction(
        "format_guid",
        std::vector<TType>{EValueType::Uint64, EValueType::Uint64},
        EValueType::String,
        "format_guid",
        ECallingConvention::Simple);

    std::vector<std::pair<TString, EValueType>> ypathGetFunctions = {
        {"try_get_int64", EValueType::Int64},
        {"get_int64", EValueType::Int64},
        {"try_get_uint64", EValueType::Uint64},
        {"get_uint64", EValueType::Uint64},
        {"try_get_double", EValueType::Double},
        {"get_double", EValueType::Double},
        {"try_get_boolean", EValueType::Boolean},
        {"get_boolean", EValueType::Boolean},
        {"try_get_string", EValueType::String},
        {"get_string", EValueType::String},
        {"try_get_any", EValueType::Any},
        {"get_any", EValueType::Any},
    };

    for (const auto& fns : ypathGetFunctions) {
        auto&& name = fns.first;
        auto&& type = fns.second;
        builder->RegisterFunction(
            name,
            std::vector<TType>{TUnionType{EValueType::Any, EValueType::Composite}, EValueType::String},
            type,
            "ypath_get",
            ECallingConvention::UnversionedValue);
    }

    builder->RegisterFunction(
        "to_any",
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
        },
        EValueType::Any,
        "to_any",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "yson_string_to_any",
        std::vector<TType>{TUnionType{EValueType::String}},
        EValueType::Any,
        "yson_string_to_any",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "list_contains",
        std::vector<TType>{
            TUnionType{
                EValueType::Any,
                EValueType::Composite,
            },
            TUnionType{
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
            },
        },
        EValueType::Boolean,
        "list_contains",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "list_has_intersection",
        std::vector<TType>{
            TUnionType{
                EValueType::Any,
                EValueType::Composite,
            },
            TUnionType{
                EValueType::Any,
                EValueType::Composite,
            },
        },
        EValueType::Boolean,
        "list_has_intersection",
        ECallingConvention::UnversionedValue);

    builder->RegisterFunction(
        "any_to_yson_string",
        std::vector<TType>{TUnionType{EValueType::Any, EValueType::Composite}},
        EValueType::String,
        "any_to_yson_string",
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "_yt_has_permissions",
        "has_permissions",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::Any, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        "has_permissions",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "_yt_stored_replica_set",
        std::unordered_map<TTypeParameter, TUnionType>(),
        {EValueType::Any},
        EValueType::Any,
        EValueType::Any,
        "stored_replica_set",
        ECallingConvention::UnversionedValue);

    builder->RegisterAggregate(
        "_yt_last_seen_replica_set",
        std::unordered_map<TTypeParameter, TUnionType>(),
        {EValueType::Any},
        EValueType::Any,
        EValueType::Any,
        "last_seen_replica_set",
        ECallingConvention::UnversionedValue);

    const TTypeParameter typeParameterGreatest = 0;
    auto anyConstraintsGreatest = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraintsGreatest[typeParameterGreatest] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
    };
    builder->RegisterFunction(
        "greatest",
        anyConstraintsGreatest,
        {typeParameterGreatest},
        typeParameterGreatest,
        typeParameterGreatest,
        "greatest");

    const TTypeParameter typeParameterAbs = 0;
    auto anyConstraintsAbs = std::unordered_map<TTypeParameter, TUnionType>();
    anyConstraintsAbs[typeParameterAbs] = {
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
    };

    builder->RegisterFunction(
        "abs",
        anyConstraintsAbs,
        {typeParameterAbs},
        EValueType::Null,
        typeParameterAbs,
        "abs");

    struct TMathFunction {
        TString Name;
        std::vector<TType> ArgumentTypes;
        EValueType ResultType;
    };

    std::vector<TMathFunction> mathFunctions = {
        {"acos", {EValueType::Double}, EValueType::Double},
        {"asin", {EValueType::Double}, EValueType::Double},
        {"ceil", {EValueType::Double}, EValueType::Int64},
        {"cbrt", {EValueType::Double}, EValueType::Double},
        {"cos", {EValueType::Double}, EValueType::Double},
        {"cot", {EValueType::Double}, EValueType::Double},
        {"degrees", {EValueType::Double}, EValueType::Double},
        {"even", {EValueType::Double}, EValueType::Int64},
        {"exp", {EValueType::Double}, EValueType::Double},
        {"floor", {EValueType::Double}, EValueType::Int64},
        {"gamma", {EValueType::Double}, EValueType::Double},
        {"is_inf", {EValueType::Double}, EValueType::Boolean},
        {"lgamma", {EValueType::Double}, EValueType::Double},
        {"ln", {EValueType::Double}, EValueType::Double},
        {"log", {EValueType::Double}, EValueType::Double},
        {"log10", {EValueType::Double}, EValueType::Double},
        {"log2", {EValueType::Double}, EValueType::Double},
        {"radians", {EValueType::Double}, EValueType::Double},
        {"sign", {EValueType::Double}, EValueType::Int64},
        {"signbit", {EValueType::Double}, EValueType::Boolean},
        {"sin", {EValueType::Double}, EValueType::Double},
        {"sqrt", {EValueType::Double}, EValueType::Double},
        {"tan", {EValueType::Double}, EValueType::Double},
        {"trunc", {EValueType::Double}, EValueType::Int64},
        {"bit_count", {EValueType::Uint64}, EValueType::Int64},
        {"atan2", {EValueType::Double, EValueType::Double}, EValueType::Double},
        {"factorial", {EValueType::Uint64}, EValueType::Uint64},
        {"gcd", {EValueType::Uint64, EValueType::Uint64}, EValueType::Uint64},
        {"lcm", {EValueType::Uint64, EValueType::Uint64}, EValueType::Uint64},
        {"pow", {EValueType::Double, EValueType::Double}, EValueType::Double},
        {"round", {EValueType::Double}, EValueType::Int64},
        {"xor", {EValueType::Uint64, EValueType::Uint64}, EValueType::Uint64},
    };

    for (const auto& [name, argumentType, resultType] : mathFunctions) {
        builder->RegisterFunction(
            name,
            "math_" + name,
            {},
            argumentType,
            EValueType::Null,
            resultType,
            "math",
            ECallingConvention::Simple,
            false);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

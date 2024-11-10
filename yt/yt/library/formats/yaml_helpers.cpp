#include "yaml_helpers.h"

#include <yt/yt/core/ytree/fluent.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////

static THashMap<TStringBuf, EYamlScalarType> YTTypeMap = {
    {"!", EYamlScalarType::String},
    {YAML_INT_TAG, EYamlScalarType::Int},
    {YAML_FLOAT_TAG, EYamlScalarType::Float},
    {YAML_BOOL_TAG, EYamlScalarType::Bool},
    {YAML_NULL_TAG, EYamlScalarType::Null},
    {YAML_STR_TAG, EYamlScalarType::String},
    {YTUintTag, EYamlScalarType::Uint},
};

EYamlScalarType DeduceScalarTypeFromTag(const TStringBuf& tag)
{
    auto it = YTTypeMap.find(tag);
    if (it != YTTypeMap.end()) {
        return it->second;
    }
    return EYamlScalarType::String;
}

EYamlScalarType DeduceScalarTypeFromValue(const TStringBuf& value)
{
    // We conform to YAML 1.2 Core Schema:
    // https://yaml.org/spec/1.2.2/#103-core-schema
    static const re2::RE2 NullRE = "null|Null|NULL|~|";
    static const re2::RE2 BoolRE = "true|True|TRUE|false|False|FALSE";
    static const re2::RE2 IntRE = "[+-]?[0-9]+";
    // In YAML 1.2 there are also octal and hexadecimal integers, but they are always positive.
    // Therefore, we treat them separately and represent as a uint scalar type.
    static const re2::RE2 UintRE = "0o[0-7]+|0x[0-9a-fA-F]+";
    static const re2::RE2 FloatRE =
        "[-+]?(\\.[0-9]+|[0-9]+(\\.[0-9]*)?)([eE][-+]?[0-9]+)?|"
        "[-+]?(\\.inf|\\.Inf|\\.INF)|"
        "\\.nan|\\.NaN|\\.NAN";
    if (re2::RE2::FullMatch(value, NullRE)) {
        return EYamlScalarType::Null;
    } else if (re2::RE2::FullMatch(value, BoolRE)) {
        return EYamlScalarType::Bool;
    } else if (re2::RE2::FullMatch(value, IntRE)) {
        return EYamlScalarType::Int;
    } else if (re2::RE2::FullMatch(value, UintRE)) {
        return EYamlScalarType::Uint;
    } else if (re2::RE2::FullMatch(value, FloatRE)) {
        return EYamlScalarType::Float;
    }
    return EYamlScalarType::String;
}

bool ParseAndValidateYamlBool(const TStringBuf& value)
{
    if (value == "true" || value == "True" || value == "TRUE") {
        return true;
    } else if (value == "false" || value == "False" || value == "FALSE") {
        return false;
    } else {
        THROW_ERROR_EXCEPTION("Value %Qv is not a boolean", value);
    }
}

std::pair<ENodeType, TNonStringScalar> ParseAndValidateYamlInteger(const TStringBuf& value, EYamlScalarType yamlType)
{
    // First, detect the base and prepare a string to calling TryIntFromString function by
    // optionally removing the 0x/0o prefix,
    int base;
    TStringBuf adjustedValue;
    if (value.StartsWith("0x")) {
        base = 16;
        adjustedValue = value.substr(2);
    } else if (value.StartsWith("0o")) {
        base = 8;
        adjustedValue = value.substr(2);
    } else {
        base = 10;
        adjustedValue = value;
    }
    i64 i64Value;
    ui64 ui64Value;

    auto tryFromString = [&] (auto& result) -> bool {
        if (base == 10) {
            return TryIntFromString<10>(adjustedValue, result);
        } else if (base == 16) {
            return TryIntFromString<16>(adjustedValue, result);
        } else {
            return TryIntFromString<8>(adjustedValue, result);
        }
    };

    // For untagged or int-tagged values (EYamlScalarType::Int) we first try to fit the value into int64, then into uint64.
    // For uint-tagged values (EYamlScalarType::Uint) we try to fit the value only into uint64.
    if (yamlType == EYamlScalarType::Int && tryFromString(i64Value)) {
        return {ENodeType::Int64, {.Int64 = i64Value}};
    } else if (tryFromString(ui64Value)) {
        return {ENodeType::Uint64, {.Uint64 = ui64Value}};
    } else {
        TString requiredDomain = (yamlType == EYamlScalarType::Int) ? "either int64 or uint64" : "uint64";
        THROW_ERROR_EXCEPTION("Value %Qv is not an integer or does not fit into %v", value, requiredDomain);
    }
}

double ParseAndValidateYamlDouble(const TStringBuf& value)
{
    double doubleValue;
    if (value == ".inf" || value == ".Inf" || value == ".INF" ||
        value == "+.inf" || value == "+.Inf" || value == "+.INF")
    {
        doubleValue = std::numeric_limits<double>::infinity();
    } else if (value == "-.inf" || value == "-.Inf" || value == "-.INF") {
        doubleValue = -std::numeric_limits<double>::infinity();
    } else if (value == ".nan" || value == ".NaN" || value == ".NAN") {
        doubleValue = std::numeric_limits<double>::quiet_NaN();
    } else if (!TryFromString<double>(value, doubleValue)) {
        THROW_ERROR_EXCEPTION("Value %Qv is not a floating point integer or does not fit into double", value);
    }
    return doubleValue;
}

std::pair<ENodeType, TNonStringScalar> ParseScalarValue(const TStringBuf& value, EYamlScalarType yamlType)
{
    switch (yamlType) {
        case EYamlScalarType::String:
            return {ENodeType::String, {}};
        case EYamlScalarType::Null:
            return {ENodeType::Entity, {}};
        case EYamlScalarType::Bool: {
            bool boolValue = ParseAndValidateYamlBool(value);
            return {ENodeType::Boolean, {.Boolean = boolValue}};
        }
        case EYamlScalarType::Int:
        case EYamlScalarType::Uint: {
            return ParseAndValidateYamlInteger(value, yamlType);
        }
        case EYamlScalarType::Float: {
            auto doubleValue = ParseAndValidateYamlDouble(value);
            return {ENodeType::Double, {.Double = doubleValue}};
        }
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

void Serialize(const yaml_mark_t& mark, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("position").Value(NYT::Format("%v:%v", mark.line, mark.column))
            .Item("index").Value(static_cast<i64>(mark.index))
        .EndMap();
}


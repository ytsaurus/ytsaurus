#pragma once
#ifndef CONVERT_INL_H_
#error "Direct inclusion of this file is not allowed, include convert.h"
// For the sake of sane code completion.
#include "convert.h"
#endif

#include "default_building_consumer.h"
#include "serialize.h"
#include "tree_builder.h"
#include "helpers.h"

#include <yt/core/ypath/token.h>

#include <yt/core/yson/tokenizer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/stream.h>
#include <yt/core/yson/producer.h>

#include <yt/core/misc/cast.h>

#include <type_traits>
#include <limits>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonProducer ConvertToProducer(T&& value)
{
    auto type = GetYsonType(value);
    auto callback = BIND(
        [] (const T& value, NYson::IYsonConsumer* consumer) {
            Serialize(value, consumer);
        },
        std::forward<T>(value));
    return NYson::TYsonProducer(std::move(callback), type);
}

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value)
{
    return ConvertToYsonString(value, NYson::EYsonFormat::Binary);
}

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format)
{
    auto type = GetYsonType(value);
    TString result;
    TStringOutput stringOutput(result);
    WriteYson(&stringOutput, value, type, format);
    return NYson::TYsonString(result, type);
}

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format, int indent)
{
    auto type = GetYsonType(value);
    TString result;
    TStringOutput stringOutput(result);
    WriteYson(&stringOutput, value, type, format, indent);
    return NYson::TYsonString(result, type);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactory* factory)
{
    auto type = GetYsonType(value);

    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();

    switch (type) {
        case NYson::EYsonType::ListFragment:
            builder->OnBeginList();
            break;
        case NYson::EYsonType::MapFragment:
            builder->OnBeginMap();
            break;
        default:
            break;
    }

    Serialize(value, builder.get());

    switch (type) {
        case NYson::EYsonType::ListFragment:
            builder->OnEndList();
            break;
        case NYson::EYsonType::MapFragment:
            builder->OnEndMap();
            break;
        default:
            break;
    }

    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::unique_ptr<IAttributeDictionary> ConvertToAttributes(const T& value)
{
    auto attributes = CreateEphemeralAttributes();
    TAttributeConsumer consumer(attributes.get());
    Serialize(value, &consumer);
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(INodePtr node)
{
    TTo result;
    Deserialize(result, node);
    return result;
}

template <class TTo, class TFrom>
TTo ConvertTo(const TFrom& value)
{
    auto type = GetYsonType(value);
    std::unique_ptr<NYson::IBuildingYsonConsumer<TTo>> buildingConsumer;
    CreateBuildingYsonConsumer(&buildingConsumer, type);
    Serialize(value, buildingConsumer.get());
    return buildingConsumer->Finish();
}

const NYson::TToken& SkipAttributes(NYson::TTokenizer* tokenizer);

#define IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(type) \
    template <> \
    inline type ConvertTo(const NYson::TYsonString& str) \
    { \
        NYson::TTokenizer tokenizer(str.GetData()); \
        const auto& token = SkipAttributes(&tokenizer); \
        switch (token.GetType()) { \
            case NYson::ETokenType::Int64: \
                return CheckedIntegralCast<type>(token.GetInt64Value()); \
            case NYson::ETokenType::Uint64: \
                return CheckedIntegralCast<type>(token.GetUint64Value()); \
            default: \
                THROW_ERROR_EXCEPTION("Cannot parse \"" #type "\" from %Qlv value", \
                    token.GetType()) \
                    << TErrorAttribute("data", str.GetData()); \
        } \
    }

IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i64)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i32)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i16)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i8)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui64)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui32)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui16)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui8)

#undef IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO

template <>
inline double ConvertTo(const NYson::TYsonString& str)
{
    NYson::TTokenizer tokenizer(str.GetData());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return token.GetInt64Value();
        case NYson::ETokenType::Double:
            return token.GetDoubleValue();
        case NYson::ETokenType::Boolean:
            return token.GetBooleanValue();
        default:
            THROW_ERROR_EXCEPTION("Cannot parse \"number\" from %Qlv value",
                token.GetType())
                << TErrorAttribute("data", str.GetData());
    }
}

template <>
inline TString ConvertTo(const NYson::TYsonString& str)
{
    NYson::TTokenizer tokenizer(str.GetData());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::String:
            return TString(token.GetStringValue());
        default:
            THROW_ERROR_EXCEPTION("Cannot parse \"string\" from %Qlv value",
                token.GetType())
                << TErrorAttribute("data", str.GetData());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

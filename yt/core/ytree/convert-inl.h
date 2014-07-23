#ifndef CONVERT_INL_H_
#error "Direct inclusion of this file is not allowed, include convert.h"
#endif
#undef CONVERT_INL_H_

#include "serialize.h"
#include "tree_builder.h"
#include "yson_stream.h"
#include "yson_producer.h"
#include "attribute_helpers.h"

#include <core/ypath/token.h>
#include <core/yson/tokenizer.h>
#include <core/yson/parser.h>

#include <util/generic/typehelpers.h>
#include <util/generic/static_assert.h>

#include <core/misc/preprocessor.h>
#include <core/misc/small_vector.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Consume(const T& value, NYson::IYsonConsumer* consumer)
{
    Serialize(value, consumer);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonProducer ConvertToProducer(T&& value)
{
    auto type = GetYsonType(value);
    auto callback = BIND(
        [] (const T& value, NYson::IYsonConsumer* consumer) {
            Consume(value, consumer);
        },
        std::forward<T>(value));
    return TYsonProducer(std::move(callback), type);
}

template <class T>
TYsonString ConvertToYsonString(const T& value)
{
    return ConvertToYsonString(value, NYson::EYsonFormat::Binary);
}

template <class T>
TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format)
{
    auto type = GetYsonType(value);
    Stroka result;
    TStringOutput stringOutput(result);
    WriteYson(&stringOutput, value, type, format);
    return TYsonString(result, type);
}

template <class T>
TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format, int indent)
{
    auto type = GetYsonType(value);
    Stroka result;
    TStringOutput stringOutput(result);
    WriteYson(&stringOutput, value, type, format, indent);
    return TYsonString(result, type);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactoryPtr factory)
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

    Consume(value, builder.get());

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
    Consume(value, &consumer);
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
    return ConvertTo<TTo>(ConvertToNode(value));
}

const NYson::TToken& SkipAttributes(NYson::TTokenizer* tokenizer);

template <>
inline i64 ConvertTo(const TYsonString& str)
{
    NYson::TTokenizer tokenizer(str.Data());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return token.GetInt64Value();
        default:
            THROW_ERROR_EXCEPTION("Cannot parse integer from %s",
                ~str.Data().Quote());
    }
}

template <>
inline double ConvertTo(const TYsonString& str)
{
    NYson::TTokenizer tokenizer(str.Data());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return token.GetInt64Value();
        case NYson::ETokenType::Double:
            return token.GetDoubleValue();
        default:
            THROW_ERROR_EXCEPTION("Cannot parse number from %s",
                ~str.Data().Quote());
    }
}

template <>
inline Stroka ConvertTo(const TYsonString& str)
{
    NYson::TTokenizer tokenizer(str.Data());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::String:
            return Stroka(token.GetStringValue());
        default:
            THROW_ERROR_EXCEPTION("Cannot parse string from %s",
                ~str.Data().Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#ifndef CONVERT_INL_H_
#error "Direct inclusion of this file is not allowed, include convert.h"
#endif
#undef CONVERT_INL_H_

#include "serialize.h"
#include "yson_parser.h"
#include "tree_builder.h"
#include "yson_stream.h"
#include "yson_producer.h"

#include <util/generic/typehelpers.h>
#include <util/generic/static_assert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Consume(const T& value, IYsonConsumer* consumer)
{
    // Check that T differs from Stroka to prevent
    // accident usage of Stroka instead TYsonString.
    static_assert(!TSameType<T, Stroka>::Result,
        "Are you sure that you want to convert from Stroka, not from TYsonString?. "
        "In this case use RawString wrapper on Stroka.");
    
    Serialize(value, consumer);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonProducer ConvertToProducer(T&& value)
{
    EYsonType type = GetYsonType(type);
    TYsonCallback callback = BIND(
        [] (const T& value, IYsonConsumer* consumer) {
            Consume(value, consumer);
        },
        ForwardRV<T>(value));
    return TYsonProducer(callback, type);
}

template <class T>
TYsonString ConvertToYsonString(
    const T& value,
    EYsonFormat format)
{
    EYsonType type = GetYsonType(value);
    Stroka result;
    TStringOutput stringOutput(result);
    WriteYson(&stringOutput, value, type, format);
    return TYsonString(result, type);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactory* factory)
{
    EYsonType type = GetYsonType(value);
  
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();

    switch (type) {
        case EYsonType::ListFragment:
            builder->OnBeginList();
            break;
        case EYsonType::MapFragment:
            builder->OnBeginMap();
            break;
        default:
            break;
    }

    Consume(value, ~builder);

    switch (type) {
        case EYsonType::ListFragment:
            builder->OnEndList();
            break;
        case EYsonType::MapFragment:
            builder->OnEndMap();
            break;
        default:
            break;
    }

    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(INodePtr node)
{
    TTo result;
    Deserialize(result, node);
    return result;
}

template <class TTo, class T>
TTo ConvertTo(const T& value)
{
    return ConvertTo<TTo>(ConvertToNode(value));
}

////////////////////////////////////////////////////////////////////////////////

inline Stroka YsonizeString(const Stroka& string, EYsonFormat format)
{
    return ConvertToYsonString(RawString(string), format).Data();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

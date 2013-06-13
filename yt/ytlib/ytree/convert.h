#pragma once

#include "public.h"
#include "yson_string.h"
#include "ephemeral_node_factory.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Consume(const T& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonProducer ConvertToProducer(T&& value);

template <class T>
TYsonString ConvertToYsonString(const T& value);

template <class T>
TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format);

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactoryPtr factory = GetEphemeralNodeFactory());

template <class T>
std::unique_ptr<IAttributeDictionary> ConvertToAttributes(const T& value);

// Provide shared instantinations for different TUs for commongly-used types.
//extern template TYsonString ConvertToYsonString<int>(const int&);
//extern template TYsonString ConvertToYsonString<TRawString>(const TRawString&);

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(INodePtr node);

template <class TTo, class TFrom>
TTo ConvertTo(const TFrom& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define CONVERT_INL_H_
#include "convert-inl.h"
#undef CONVERT_INL_H_

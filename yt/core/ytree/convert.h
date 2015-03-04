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

TYsonString ConvertToYsonString(const char* value);

template <class T>
TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format);

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactoryPtr factory = GetEphemeralNodeFactory());

template <class T>
std::unique_ptr<IAttributeDictionary> ConvertToAttributes(const T& value);

// Provide shared instantiations for different TUs for commonly-used types.
// XXX(babenko): VS does not support this
//extern template TYsonString ConvertToYsonString<int>(const int&);
//extern template TYsonString ConvertToYsonString<unsigned long>(const unsigned long&);
//extern template TYsonString ConvertToYsonString<Stroka>(const Stroka&);

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

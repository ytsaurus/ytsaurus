#pragma once

#include "public.h"
#include "yson_string.h"
#include "ephemeral.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Consume(const T& value, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonProducer ConvertToProducer(T&& value);

template <class T>
TYsonString ConvertToYsonString(const T& value);

template <class T>
TYsonString ConvertToYsonString(const T& value, EYsonFormat format);

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactoryPtr factory = GetEphemeralNodeFactory());

template <class T>
TAutoPtr<IAttributeDictionary> ConvertToAttributes(const T& value);

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(INodePtr node);

template <class TTo, class T>
TTo ConvertTo(const T& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define CONVERT_INL_H_
#include "convert-inl.h"
#undef CONVERT_INL_H_

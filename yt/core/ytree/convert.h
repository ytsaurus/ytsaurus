#pragma once

#include "public.h"
#include "ephemeral_node_factory.h"

#include <core/yson/string.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonProducer ConvertToProducer(T&& value);

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value);

NYson::TYsonString ConvertToYsonString(const char* value);

NYson::TYsonString ConvertToYsonString(const TStringBuf& value);

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value, NYson::EYsonFormat format);

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactoryPtr factory = GetEphemeralNodeFactory());

template <class T>
std::unique_ptr<IAttributeDictionary> ConvertToAttributes(const T& value);

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

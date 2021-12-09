#pragma once

#include "public.h"
#include "ephemeral_node_factory.h"

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/string.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): consider moving to yson/

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value);

NYson::TYsonString ConvertToYsonString(const char* value);

NYson::TYsonString ConvertToYsonString(TStringBuf value);

template <class T>
NYson::TYsonString ConvertToYsonString(const T& value, EYsonFormat format);

// Provide shared instantiations for commonly used types.
extern template NYson::TYsonString ConvertToYsonString<bool>(const bool&);
extern template NYson::TYsonString ConvertToYsonString<int>(const int&);
extern template NYson::TYsonString ConvertToYsonString<long>(const long&);
extern template NYson::TYsonString ConvertToYsonString<unsigned int>(const unsigned int&);
extern template NYson::TYsonString ConvertToYsonString<unsigned long>(const unsigned long&);
extern template NYson::TYsonString ConvertToYsonString<TString>(const TString&);
extern template NYson::TYsonString ConvertToYsonString<TInstant>(const TInstant&);
extern template NYson::TYsonString ConvertToYsonString<TDuration>(const TDuration&);
extern template NYson::TYsonString ConvertToYsonString<TGuid>(const TGuid&);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonProducer ConvertToProducer(T&& value);

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactory* factory = GetEphemeralNodeFactory());

template <class T>
IAttributeDictionaryPtr ConvertToAttributes(const T& value);

template <class TTo>
TTo ConvertTo(INodePtr node);

template <class TTo, class TFrom>
TTo ConvertTo(const TFrom& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define CONVERT_INL_H_
#include "convert-inl.h"
#undef CONVERT_INL_H_

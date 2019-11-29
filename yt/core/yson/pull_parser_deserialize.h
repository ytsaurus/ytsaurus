#pragma once

#include "public.h"

#include "pull_parser.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// integers
void Deserialize(signed char& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned char& value, TYsonPullParserCursor* cursor);
void Deserialize(short& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned short& value, TYsonPullParserCursor* cursor);
void Deserialize(int& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned& value, TYsonPullParserCursor* cursor);
void Deserialize(long& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned long& value, TYsonPullParserCursor* cursor);
void Deserialize(long long& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned long long& value, TYsonPullParserCursor* cursor);

// double
void Deserialize(double& value, TYsonPullParserCursor* cursor);

// TString
void Deserialize(TString& value, TYsonPullParserCursor* cursor);

// bool
void Deserialize(bool& value, TYsonPullParserCursor* cursor);

// char
void Deserialize(char& value, TYsonPullParserCursor* cursor);

// TDuration
void Deserialize(TDuration& value, TYsonPullParserCursor* cursor);

// TInstant
void Deserialize(TInstant& value, TYsonPullParserCursor* cursor);

// TGuid.
void Deserialize(TGuid& value, TYsonPullParserCursor* cursor);

// std::vector.
template <class T, class A>
void Deserialize(std::vector<T, A>& value, TYsonPullParserCursor* cursor);

// std::optional.
template <class T>
void Deserialize(std::optional<T>& value, TYsonPullParserCursor* cursor);

// Enum.
template <class T>
void Deserialize(T& value, TYsonPullParserCursor* cursor, std::enable_if_t<TEnumTraits<T>::IsEnum, void*> = nullptr);

// SmallVector.
template <class T, unsigned N>
void Deserialize(SmallVector<T, N>& value, TYsonPullParserCursor* cursor);

template <class F, class S>
void Deserialize(std::pair<F, S>& value, TYsonPullParserCursor* cursor);

template <class T, size_t N>
void Deserialize(std::array<T, N>& value, TYsonPullParserCursor* cursor);

template <class... T>
void Deserialize(std::tuple<T...>& value, TYsonPullParserCursor* cursor);

// For any associative container.
template <template<typename...> class C, class... T, class K = typename C<T...>::key_type>
void Deserialize(C<T...>& value, TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

template <typename TTo>
TTo ExtractTo(TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PULL_PARSER_DESERIALIZE_INL_H_
#include "pull_parser_deserialize-inl.h"
#undef PULL_PARSER_DESERIALIZE_INL_H_

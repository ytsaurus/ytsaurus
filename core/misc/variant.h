#pragma once

#include <variant>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, class... Ts>
struct TIndexOf;

} // namespace NDetail

template <class T, class V>
struct TVariantIndex;

template <class T, class... Ts>
struct TVariantIndex<T, std::variant<Ts...>>
    : std::integral_constant<size_t, NDetail::TIndexOf<T, Ts...>::Value>
{ };

template <class T, class V>
constexpr size_t VariantIndexV = TVariantIndex<T, V>::value;

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
void FormatValue(TStringBuilder* builder, const std::variant<Ts...>& variant, TStringBuf spec);

template <class... Ts>
TString ToString(const std::variant<Ts...>& variant);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define VARIANT_INL_H_
#include "variant-inl.h"
#undef VARIANT_INL_H_

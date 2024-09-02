#pragma once

#include <util/str_stl.h>

#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TRorenHash
    : public std::hash<T>
{
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename = std::void_t<>>
struct IsHashable_ : std::false_type {};
template <typename T>
struct IsHashable_<T, std::void_t<decltype(std::declval<TRorenHash<T>>()(std::declval<T>()))>> : std::true_type {};
template <typename T>
constexpr bool IsHashable = IsHashable_<T>::value;

////////////////////////////////////////////////////////////////////////////////

template <class T, class V>
struct IsKeyHashRequired_ : std::false_type {};
template <typename T>
constexpr bool IsKeyHashRequired = IsKeyHashRequired_<T, void>::value;

////////////////////////////////////////////////////////////////////////////////

}  // NRoren::NPrivate


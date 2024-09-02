#pragma once

#include <util/str_stl.h>
#include <util/digest/multi.h>

#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TRorenHash
    : public std::hash<T>
{
};

template <typename... Ts>
struct TRorenHash<std::tuple<Ts...>>
{
    size_t operator()(const std::tuple<Ts...>& tuple) const noexcept
    {
        return std::apply(
            [] (const Ts&... args) {
                return MultiHash(args...);
            },
            tuple);
    }
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


#pragma once

#include <util/generic/typetraits.h>  // TDependentFalse

#include <tuple>  // std::forward_as_tuple
#include <utility>  // std::forward
#include <type_traits>  // std::integral_constant


namespace NRoren::NPrivate
{
////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TMemberPointerClass;
template<typename Class, typename Value>
struct TMemberPointerClass<Value Class::*>
{
    typedef Class type;
};

template <typename T>
using GetMemberPointerClass = TMemberPointerClass<T>::type;

template<typename T>
struct TMemberPointerValueType;
template<typename Class, typename Value>
struct TMemberPointerValueType<Value Class::*>
{
    typedef Value type;
};

template <typename T>
using GetMemberPointerValueType = TMemberPointerValueType<T>::type;

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename... Ts>
struct IndexOfType
{
    static_assert(TDependentFalse<T>, "Required function argument not provided.");
};

template <typename T, typename... Ts>
struct IndexOfType<T, T, Ts...> : std::integral_constant<std::size_t, 0> {};

template <typename T, typename U, typename... Ts>
struct IndexOfType<T, U, Ts...> : std::integral_constant<std::size_t, 1 + IndexOfType<T, Ts...>::value> {};

template <typename T, typename... Ts>
constexpr std::size_t IndexOfType_v = IndexOfType<std::decay_t<T>, std::decay_t<Ts>...>::value;

////////////////////////////////////////////////////////////////////////////////

template<typename TResult, typename... TExpectedArgs, typename... TArgs>
TResult InvokeUnordered(TResult (*func)(TExpectedArgs...), TArgs&& ...args)
{
    return std::invoke(func, std::forward<TExpectedArgs>(std::get<IndexOfType_v<TExpectedArgs, TArgs...>>(std::forward_as_tuple(args...)))...);
}

template<typename O, typename TResult, typename... TExpectedArgs, typename... TArgs>
TResult InvokeUnordered(TResult (O::*method)(TExpectedArgs...), O* obj, TArgs&& ...args)
{
    return std::invoke(method, obj, std::forward<TExpectedArgs>(std::get<IndexOfType_v<TExpectedArgs, TArgs...>>(std::forward_as_tuple(args...)))...);
}

template<typename O, typename TResult, typename... TExpectedArgs, typename... TArgs>
TResult InvokeUnordered(TResult (O::*method)(TExpectedArgs...) const, const O* obj, TArgs&& ...args)
{
    return std::invoke(method, obj, std::forward<TExpectedArgs>(std::get<IndexOfType_v<TExpectedArgs, TArgs...>>(std::forward_as_tuple(args...)))...);
}

} // namespace NRoren::NPrivate

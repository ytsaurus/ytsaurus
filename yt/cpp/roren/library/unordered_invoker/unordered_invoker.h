#pragma once

#include <util/generic/function.h>  // TFunctionSignature
#include <util/generic/typetraits.h>  // TDependentFalse

#include <tuple>  // std::forward_as_tuple
#include <utility>  // std::forward
#include <type_traits>  // std::integral_constant

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TMemberPointerClass;
template <typename Class, typename Value>
struct TMemberPointerClass<Value Class::*>
{
    using TType = Class;
};

template <typename T>
using GetMemberPointerClass = typename TMemberPointerClass<T>::TType;

template <typename T>
struct TMemberPointerValueType;
template <typename Class, typename Value>
struct TMemberPointerValueType<Value Class::*>
{
    using TType = Value;
};

template <typename T>
using GetMemberPointerValueType = typename TMemberPointerValueType<T>::TType;

////////////////////////////////////////////////////////////////////////////////

template <bool Found, typename T, typename... Ts>
struct TIndexOfType : public std::integral_constant<std::size_t, 0>
{
    static_assert(
        Found || TDependentFalse<T>,
        "Required function argument is not provided.");
};

template <bool Found, typename T, typename... Ts>
struct TIndexOfType<Found, T, T, Ts...> : public TIndexOfType<true, T, Ts...>
{
    static_assert(
        !Found || TDependentFalse<T>,
        "Same type arguments are not allowed for InvokeUnordered.");
};

template <bool Found, typename T, typename U, typename... Ts>
struct TIndexOfType<Found, T, U, Ts...> : public std::integral_constant<std::size_t, (Found ? 0 : 1) + TIndexOfType<Found, T, Ts...>::value> {};

template <typename T, typename... Ts>
constexpr std::size_t IndexOfType = TIndexOfType<false, std::decay_t<T>, std::decay_t<Ts>...>::value;

template <typename TFunc, typename TResult, typename... TExpectedArgs, typename... TArgs>
TResult InvokeUnorderedImpl(TFunc&& func, std::type_identity<TResult(TExpectedArgs...)>, TArgs&&... args)
{
    auto argsTuple = std::forward_as_tuple(std::forward<TArgs>(args)...);
    return std::invoke(
        std::forward<TFunc>(func),
        std::get<IndexOfType<TExpectedArgs, TArgs...>>(std::move(argsTuple))...);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TFunc, typename... TArgs>
    requires (!std::is_member_function_pointer_v<TFunc>)
decltype(auto) InvokeUnordered(TFunc&& func, TArgs&&... args)
{
    return InvokeUnorderedImpl(
        std::forward<TFunc>(func),
        std::type_identity<TFunctionSignature<std::decay_t<TFunc>>>{},
        std::forward<TArgs>(args)...);
}

template <typename O, typename TResult, typename... TExpectedArgs, typename... TArgs>
TResult InvokeUnordered(TResult (O::*method)(TExpectedArgs...), O* obj, TArgs&& ...args)
{
    auto argsTuple = std::forward_as_tuple(std::forward<TArgs>(args)...);
    return std::invoke(method, obj, std::get<IndexOfType<TExpectedArgs, TArgs...>>(std::move(argsTuple))...);
}

template <typename O, typename TResult, typename... TExpectedArgs, typename... TArgs>
TResult InvokeUnordered(TResult (O::*method)(TExpectedArgs...) const, const O* obj, TArgs&& ...args)
{
    auto argsTuple = std::forward_as_tuple(std::forward<TArgs>(args)...);
    return std::invoke(method, obj, std::get<IndexOfType<TExpectedArgs, TArgs...>>(std::move(argsTuple))...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

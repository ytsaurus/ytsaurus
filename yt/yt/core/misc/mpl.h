#pragma once

#include <util/generic/typetraits.h>

#include <tuple>
#include <type_traits>

// See the following references for an inspiration:
//   * http://llvm.org/viewvc/llvm-project/libcxx/trunk/include/type_traits?revision=HEAD&view=markup
//   * http://www.boost.org/doc/libs/1_48_0/libs/type_traits/doc/html/index.html
//   * http://www.boost.org/doc/libs/1_48_0/libs/mpl/doc/index.html

namespace NYT::NMpl {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, bool isPrimitive>
struct TCallTraitsHelper
{  };

template <class T>
struct TCallTraitsHelper<T, true>
{
    using TType = T;
};

template <class T>
struct TCallTraitsHelper<T, false>
{
    using TType = const T&;
};

template <template <class...> class TTemplate, class... TArgs>
void DerivedFromSpecializationImpl(const TTemplate<TArgs...>&);

} // namespace NDetail

//! A trait for choosing appropriate argument and return types for functions.
/*!
 *  All types except for primitive ones should be passed to functions
 *  and returned from const getters by const ref.
 */
template <class T>
struct TCallTraits
    : public NDetail::TCallTraitsHelper<T, !std::is_class<T>::value>
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TIsPod
    : std::integral_constant<bool, ::TTypeTraits<T>::IsPod>
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, template <class...> class TTemplatedBase>
concept DerivedFromSpecializationOf = requires(const TDerived& instance)
{
    NDetail::DerivedFromSpecializationImpl<TTemplatedBase>(instance);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
constexpr T Max(T x)
{
    return x;
}

template <class T>
constexpr T Max(T x, T y)
{
    return x < y ? y : x;
}

template <class T, class... Ts>
constexpr T Max(T x, Ts... args)
{
    return Max(x, Max(args...));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
constexpr T Min(T x)
{
    return x;
}

template <class T>
constexpr T Min(T x, T y)
{
    return x < y ? x : y;
}

template <class T, class... Ts>
constexpr T Min(T x, Ts... args)
{
    return Min(x, Min(args...));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMpl

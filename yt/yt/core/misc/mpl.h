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
    typedef T TType;
};

template <class T>
struct TCallTraitsHelper<T, false>
{
    typedef const T& TType;
};

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

//! Generates a type trait that checks existence of a member called |X|.
/*!
 * See http://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Member_Detector
 */
#define DEFINE_MPL_MEMBER_DETECTOR(X)                                                    \
    template <class T>                                                                   \
    class THas##X##Member                                                                \
    {                                                                                    \
    private:                                                                             \
        struct Fallback { int X; };                                                      \
        struct Derived : T, Fallback { };                                                \
                                                                                         \
        template <typename U, U> struct Check;                                           \
                                                                                         \
        typedef char ArrayOfOne[1];                                                      \
        typedef char ArrayOfTwo[2];                                                      \
                                                                                         \
        template <typename U> static ArrayOfOne & func(Check<int Fallback::*, &U::X> *); \
        template <typename U> static ArrayOfTwo & func(...);                             \
                                                                                         \
    public:                                                                              \
        enum                                                                             \
        {                                                                                \
            Value = sizeof(func<Derived>(0)) == 2                                        \
        };                                                                               \
    }

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

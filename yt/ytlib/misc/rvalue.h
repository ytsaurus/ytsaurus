#pragma once

/*!
 * \file rvalue.h
 * \brief Auxiliary functions from C++11 to work with rvalue references.
 */

#include "mpl.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
struct TIdentity {
    typedef T TType;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Implements #std::move<T>-like behaviour from C++11.
template <class T>
FORCED_INLINE typename NMpl::TRemoveReference<T>::TType&&
MoveRV(T&& x) // noexcept
{
    return static_cast<typename NMpl::TRemoveReference<T>::TType&&>(x);
}

// Implements #std::forward<T>-like behaviour from C++11.
// N2951, http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2009/n2951.html
// N3242, 20.2.3, for standard definition of C++11.
#if defined(__GNUC__)
template <class T, class U,
    class = typename NMpl::TEnableIfC<
        (
            NMpl::TIsLvalueReference<T>::Value ? 
            NMpl::TIsLvalueReference<U>::Value :
            true)
        && NMpl::TIsConvertible<
            typename NMpl::TRemoveReference<U>::TType*,
            typename NMpl::TRemoveReference<T>::TType*
        >::Value
    >::TType>
FORCED_INLINE T&& ForwardRV(U&& arg) // noexcept
{
    return static_cast<T&&>(arg);
}
#else
// MSVC does not support default template arguments so we fallback to
// compile time assertion. 
template <class T, class U>
FORCED_INLINE T&& ForwardRV(U&& arg) // noexcept
{
    static_assert(
        (
            NMpl::TIsLvalueReference<T>::Value ? 
            NMpl::TIsLvalueReference<U>::Value :
            true)
        && NMpl::TIsConvertible<
            typename NMpl::TRemoveReference<U>::TType*,
            typename NMpl::TRemoveReference<T>::TType*
        >::Value,
        "Incorrect usage of ForwardRV");
    return static_cast<T&&>(arg);
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

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
#if 0
// MSVC
// http://msdn.microsoft.com/en-us/library/ee390914.aspx
// http://blogs.msdn.com/b/vcblog/archive/2009/02/03/rvalue-references-c-0x-features-in-vc10-part-2.aspx
template <class T>
FORCED_INLINE T&& ForwardRV(typename NDetail::TIdentity<T>::TType&& x) // noexcept
{
    return x;
}

// XXX(sandello): This weird implementation of ForwardRV is intentional;
// the main rationale is to provide a correct build under MSVS ignoring
// any performance considerations.
template <class T>
FORCED_INLINE T&& ForwardRV(T&& x) // noexcept
{
    return x;
}

template <class T>
FORCED_INLINE const T& ForwardRV(const T& x) // noexcept
{
    return x;
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


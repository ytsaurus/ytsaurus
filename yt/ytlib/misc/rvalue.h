#pragma once

/*!
 * \file rvalue.h
 * \brief Auxiliary functions from C++11 to work with rvalue references.
 */

#include <util/generic/typetraits.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Implements #std::move<T> behaviour from C++11.
template<typename T>
FORCED_INLINE typename TTypeTraits<T>::TReferenceTo&& MoveRV(T&& x) throw()
{
    return static_cast<typename TTypeTraits<T>::TReferenceTo&&>(x);
}

#ifdef __GNUC__

//! Implements #std::forward<T> behaviour from C++11.
template<typename T>
FORCED_INLINE T&& ForwardRV(typename TTypeTraits<T>::TReferenceTo& x) throw()
{
    return x;
}

//! Fix instantiation errors (mainly due to reference collapsing).
//! \{
template<typename T>
FORCED_INLINE T& ForwardRV(T& x) throw()
{
    return x;
}

template<typename T>
FORCED_INLINE const T*&& ForwardRV(const T*& x) throw()
{
    return x;
}
//! \}

#else

// TODO: This weird implementation of ForwardRV is intentional;
// the main rationale is to provide a correct build under MSVS ignoring
// any performance considerations.

template<typename T>
FORCED_INLINE T& ForwardRV(T& x) throw()
{
    return x;
}

template<typename T>
FORCED_INLINE const T& ForwardRV(const T& x) throw()
{
    return x;
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


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
FORCED_INLINE typename TTypeTraits<T>::TReferenceTo&& MoveRV(T&& x)
{
    return static_cast<typename TTypeTraits<T>::TReferenceTo<T>::TType&&>(x);
}

//! Implements #std::forward<T> behaviour from C++11.
template<typename T>
FORCED_INLINE T&& ForwardRV(typename TTypeTraits<T>::TReferenceTo<T>::TType& x)
{
    return static_cast<T&&>(x);
}

//! Fix instantination errors (mainly due to reference collapsing).
//! \{
template<typename T>
FORCED_INLINE T& ForwardRV(T& x)
{
    return x;
}

template<typename T>
FORCED_INLINE const T*&& ForwardRV(const T*& x)
{
    return x;
}
//! \}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


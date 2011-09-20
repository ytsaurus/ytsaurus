#pragma once

/*!
 * \file rvalue.h
 * \brief Auxiliary functions from C++11 to work with rvalue references.
 */

#include <util/generic/typetraits.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Implements #std::identity behaviour from C++11.
template<class T>
struct TIdentityTraits
{
    typedef T TType;
};

//! Implements #std::move<T> behaviour from C++11.
template<class T>
typename TTypeTraits<T>::TReferenceTo&&
MoveRV(T&& x)
{
    return static_cast<typename TTypeTraits<T>::TReferenceTo&&>(x);
}

//! Implements #std::forward<T> behaviour from C++11.
template<class T>
T&& ForwardRV(typename TIdentityTraits<T>::TType&& x)
{
    return x;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


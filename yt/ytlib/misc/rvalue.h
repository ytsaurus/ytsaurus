#pragma once

/*!
 * \file rvalue.h
 * \brief Auxiliary functions from C++11 to work with rvalue references.
 */

#include <util/generic/typetraits.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template<typename T>
struct TIdentity {
    typedef T TType;
};

// N3242, 20.9.7.2
template<typename T>
struct TRemoveReference {
    typedef T TType;
};

template<typename T>
struct TRemoveReference<T&> {
    typedef T TType;
};

template<typename T>
struct TRemoveReference<const T&> {
    typedef const T TType;
};

template<typename T>
struct TRemoveReference<T&&> {
    typedef T TType;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Implements #std::move<T> behaviour from C++11.
template<typename T>
FORCED_INLINE typename NDetail::TRemoveReference<T>::TType&& MoveRV(T&& x) throw()
{
    return static_cast<typename NDetail::TRemoveReference<T>::TType&&>(x);
}

#ifdef __GNUC__ 
// GCC
// N3242, 20.2.3

//! Implements #std::forward<T> behaviour from C++11.
template<typename T>
FORCED_INLINE T&& ForwardRV(typename NDetail::TRemoveReference<T>::TType& x) throw()
{
    return static_cast<T&&>(x);
}

template<typename T>
FORCED_INLINE T&& ForwardRV(typename NDetail::TRemoveReference<T>::TType&& x) throw()
{
    return static_cast<T&&>(x);
}

//! Fix instantiation errors (mainly due to reference collapsing).
//! \{
template<class T>
FORCED_INLINE T&& ForwardRV(T&& x) throw()
{
    return x;
}
//! \}

#else
// MSVC
// http://msdn.microsoft.com/en-us/library/ee390914.aspx
// http://blogs.msdn.com/b/vcblog/archive/2009/02/03/rvalue-references-c-0x-features-in-vc10-part-2.aspx

template<typename T>
FORCED_INLINE T&& ForwardRV(typename NDetail::TIdentity<T>::TType&& x) throw()
{
    return x;
}

// TODO: This weird implementation of ForwardRV is intentional;
// the main rationale is to provide a correct build under MSVS ignoring
// any performance considerations.

template<typename T>
FORCED_INLINE T&& ForwardRV(T&& x) throw()
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


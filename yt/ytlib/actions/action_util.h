#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Constructs a delegate from functor.
template <class TFunctor>
auto FromFunctor(const TFunctor& functor) ->
decltype (TFunctorTraits<TFunctor, decltype(&TFunctor::operator ())>::Construct(functor))
{
    return TFunctorTraits<TFunctor, decltype(&TFunctor::operator ())>::Construct(functor);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TActionTargetTraits
{ };

template<class T>
struct TActionTargetTraits< TIntrusivePtr<T> >
{
    typedef T TUnderlying;

    static FORCED_INLINE TUnderlying* Get(const TIntrusivePtr<T>& ptr)
    {
        return ptr.Get();
    }

    static FORCED_INLINE TUnderlying* Get(const TIntrusivePtr<T>&& ptr)
    {
        return ptr.Get();
    }
};

template<class T>
struct TActionTargetTraits<T*>
{
    typedef T TUnderlying;

    static FORCED_INLINE TUnderlying* Get(T* ptr)
    {
        return ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TActionArgTraits
{
    typedef T TCopy;
};

template<class T>
struct TActionArgTraits<const T&>
{
    typedef T TCopy;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ACTION_UTIL_GEN_H_
#include "action_util-gen.h"
#undef ACTION_UTIL_GEN_H_

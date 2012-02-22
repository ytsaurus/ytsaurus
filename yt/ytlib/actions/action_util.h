#pragma once

#include "common.h"
#include "action.h"

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

template <class T>
struct TActionTargetTraits< TWeakPtr<T> >
{
    typedef TWeakPtr<T> TTargetPtr;
    typedef T TUnderlying;

    static const bool StrongReference = false;

    static FORCED_INLINE TIntrusivePtr<T> Lock(const TTargetPtr& ptr)
    {
        return ptr.Lock();
    }

    static FORCED_INLINE TUnderlying* Get(const TTargetPtr& ptr)
    {
        // Note that this line incurs extra Ref/UnRef pair.
        // This will be optimized in new-style closures.
        return ptr.Lock().Get();
    }
};

template <class T>
struct TActionTargetTraits< TIntrusivePtr<T> >
{
    typedef TIntrusivePtr<T> TTargetPtr;
    typedef T TUnderlying;

    static const bool StrongReference = true;

    static FORCED_INLINE int Lock(const TTargetPtr& ptr)
    {
        return 0;
    }

    static FORCED_INLINE TUnderlying* Get(const TTargetPtr& ptr)
    {
        return ptr.Get();
    }
};

template <class T>
struct TActionTargetTraits<T*>
{
    typedef T* TTargetPtr;
    typedef T TUnderlying;

    static const bool StrongReference = true;

    static FORCED_INLINE int Lock(const TTargetPtr& ptr)
    {
        return 0;
    }

    static FORCED_INLINE TUnderlying* Get(const TTargetPtr& ptr)
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

template <class T>
struct TActionArgTraits<const T&>
{
    typedef T TCopy;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ACTION_UTIL_GEN_H_
#include "action_util-gen.h"
#undef ACTION_UTIL_GEN_H_

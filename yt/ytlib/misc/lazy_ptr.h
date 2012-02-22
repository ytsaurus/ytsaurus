#pragma once

#include "common.h"

#include <ytlib/actions/action.h>

#include <util/system/spinlock.h>

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

// Intrusive ptr with lazy creation and double-checked locking.
template <class T, class TLock = TSpinLock>
class TLazyPtr
    : public TPointerCommon<TLazyPtr<T, TLock>, T>
{
public:
    typedef TIntrusivePtr< IFunc< TIntrusivePtr<T> > > TFactoryPtr;

    TLazyPtr(TFactoryPtr factory)
        : Factory(factory)
    { }

    TLazyPtr()
        : Factory(NULL)
    { }

    inline T* Get() const throw()
    {
        if (!Value) {
            TGuard<TLock> guard(Lock);
            if (!Value) {
                Value = !Factory ? New<T>() : Factory->Do();
            }
        }
        return ~Value;
    }

private:
    TLock Lock;
    TFactoryPtr Factory;
    mutable TIntrusivePtr<T> Value;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* operator ~ (const TLazyPtr<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

// Non-intrusive ptr with lazy creation and double-checked locking.
template <class T, class TLock = TSpinLock>
class TLazyHolder
    : public TPointerCommon<TLazyHolder<T, TLock>, T>
{
    TLock Lock;
    typename IFunc<T*>::TPtr Fabric;
    mutable TAutoPtr<T> Value;

public:
    TLazyHolder(typename IFunc<T*>::TPtr fabric)
        : Fabric(fabric)
    { }

    TLazyHolder()
        : Fabric(NULL)
    { }

    inline T* Get() const throw()
    {
        if (!Value) {
            TGuard<TLock> guard(Lock);
            if (!Value) {
                Value = !Fabric ? new T() : Fabric->Do();
            }
        }
        return ~Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* operator ~ (const TLazyHolder<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

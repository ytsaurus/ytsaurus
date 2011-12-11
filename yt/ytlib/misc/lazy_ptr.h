#pragma once

#include "common.h"
#include "../actions/action.h"

#include <util/generic/ptr.h>
#include <util/system/spinlock.h>

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

// Intrusive ptr with lazy creation and double-checked locking.
template<class T, class TLock = TSpinLock>
class TLazyPtr
    : public TPointerCommon<TLazyPtr<T, TLock>, T>
{
    TLock Lock;
    typename IFunc<T*>::TPtr Fabric;
    mutable TIntrusivePtr<T> Value;

public:
    TLazyPtr(typename IFunc<T*>::TPtr fabric)
        : Fabric(fabric)
    { }

    TLazyPtr()
        : Fabric(NULL)
    { }

    inline T* Get() const throw()
    {
        if (~Value == NULL) {
            TGuard<TLock> guard(Lock);
            if (~Value == NULL) {
                Value = ~Fabric == NULL ? New<T>() : Fabric->Do();
            }
        }
        return ~Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

template<class T>
T* operator ~ (const TLazyPtr<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

// Non-intrusive ptr with lazy creation and double-checked locking.
template<class T, class TLock = TSpinLock>
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
        if (~Value == NULL) {
            TGuard<TLock> guard(Lock);
            if (~Value == NULL) {
                Value = ~Fabric == NULL ? new T() : Fabric->Do();
            }
        }
        return ~Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

template<class T>
T* operator ~ (const TLazyHolder<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

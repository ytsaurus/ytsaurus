#pragma once

#include "common.h"

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
    mutable TIntrusivePtr<T> Value;

public:
    inline T* Get() const throw()
    {
        if (~Value == NULL) {
            TGuard<TLock> guard(Lock);
            if (~Value == NULL) {
                Value = New<T>();
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
    mutable TAutoPtr<T> Value;

public:
    inline T* Get() const throw()
    {
        if (~Value == NULL) {
            TGuard<TLock> guard(Lock);
            if (~Value == NULL) {
                Value = new T();
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


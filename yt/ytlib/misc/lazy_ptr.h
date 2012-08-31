#pragma once

#include "common.h"

#include <ytlib/actions/callback.h>

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
    typedef TCallback<TIntrusivePtr<T>()> TFactory;

    explicit TLazyPtr(TFactory factory)
        : Factory(MoveRV(factory))
    { }

    T* Get() const throw()
    {
        if (!Value) {
            TGuard<TLock> guard(Lock);
            if (!Value) {
                Value = Factory.Run();
            }
        }
        return ~Value;
    }

private:
    TLock Lock;
    TFactory Factory;
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
public:
    typedef TCallback<T*()> TFactory;

    TLazyHolder(TFactory fabric)
        : Factory(MoveRV(fabric))
    { }

    TLazyHolder()
        : Factory()
    { }

    inline T* Get() const throw()
    {
        static_assert(!NMpl::TIsConvertible<T*, TExtrinsicRefCounted*>::Value, "No RC here.");
        static_assert(!NMpl::TIsConvertible<T*, TIntrinsicRefCounted*>::Value, "No RC here.");
        if (!Value) {
            TGuard<TLock> guard(Lock);
            if (!Value) {
                Value = Factory.IsNull() ? new T() : Factory.Run();
            }
        }
        return ~Value;
    }

private:
    TLock Lock;
    TFactory Factory;
    mutable TAutoPtr<T> Value;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* operator ~ (const TLazyHolder<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

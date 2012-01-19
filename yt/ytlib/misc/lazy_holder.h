#pragma once

#include "ptr.h"

#include <util/system/spinlock.h>

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

// Holder with lazy creation and double-checked locking.
template<class T, class TLock = TSpinLock>
class TLazyHolder
    : public TPointerCommon<TLazyHolder<T, TLock>, T>
{
    TLock Lock;
    mutable THolder<T> Value;

public:
    inline T* Get() const throw()
    {
        if (!Value) {
            TGuard<TLock> guard(Lock);
            if (!Value) {
                Value.Reset(new T());
            }
        }
        return ~Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: implement operator~

} // namespace NYT


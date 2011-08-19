#pragma once

#include <util/generic/ptr.h>
#include <util/system/spinlock.h>
#include "ptr.h"

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
        if (~Value == NULL) {
            TGuard<TLock> guard(Lock);
            if (~Value == NULL) {
                Value.Reset(new T());
            }
        }
        return ~Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: implement operator~

} // namespace NYT


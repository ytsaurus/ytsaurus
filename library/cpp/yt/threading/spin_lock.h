#pragma once

#include "public.h"
#include "spin_lock_base.h"

#include <atomic>

#include <util/system/src_location.h>
#include <util/system/types.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

//! A slightly modified version of TAdaptiveLock.
/*!
 *  The lock is unfair.
 */
class TSpinLock
    : public TSpinLockBase
{
public:
    using TSpinLockBase::TSpinLockBase;

    //! Acquires the lock.
    void Acquire() noexcept;

    //! Tries acquiring the lock.
    //! Returns |true| on success.
    bool TryAcquire() noexcept;

    //! Releases the lock.
    void Release() noexcept;

    //! Returns true if the lock is taken.
    /*!
     *  This is inherently racy.
     *  Only use for debugging and diagnostic purposes.
     */
    bool IsLocked() const noexcept;

private:
    using TValue = ui32;
    static constexpr TValue UnlockedValue = 0;
    static constexpr TValue LockedValue = 1;

    std::atomic<TValue> Value_ = UnlockedValue;

    bool TryAndTryAcquire() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

//! A variant of TReaderWriterSpinLock occupyig the whole cache line.
class TPaddedSpinLock
    : public TSpinLock
{
private:
    [[maybe_unused]]
    char Padding_[CacheLineSize - sizeof(TSpinLock)];
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading

#define SPIN_LOCK_INL_H_
#include "spin_lock-inl.h"
#undef SPIN_LOCK_INL_H_


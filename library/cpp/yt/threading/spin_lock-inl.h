#pragma once
#ifndef SPIN_LOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include spin_lock.h"
// For the sake of sane code completion.
#include "spin_lock.h"
#endif
#undef SPIN_LOCK_INL_H_

#include "spin_wait.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

inline void TSpinLock::Acquire() noexcept
{
    if (TryAcquire()) {
        return;
    }
    TSpinWait spinWait;
    while (!TryAndTryAcquire()) {
        spinWait.Wait();
    }
}

inline void TSpinLock::Release() noexcept
{
#ifdef NDEBUG
    Value_.store(UnlockedValue, std::memory_order_relaxed);
#else
    YT_ASSERT(Value_.exchange(UnlockedValue, std::memory_order_relaxed) == LockedValue);
#endif
}

inline bool TSpinLock::IsLocked() const noexcept
{
    return Value_.load() == LockedValue;
}

inline bool TSpinLock::TryAcquire() noexcept
{
    auto expected = UnlockedValue;
    return Value_.compare_exchange_weak(expected, LockedValue);
}

inline bool TSpinLock::TryAndTryAcquire() noexcept
{
    if (Value_.load(std::memory_order_relaxed) != 0) {
        return false;
    }
    return TryAcquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading


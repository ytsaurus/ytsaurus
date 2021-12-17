#ifndef RECURSIVE_SPIN_LOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include recursive_spinlock.h"
// For the sake of sane code completion.
#include "recursive_spin_lock.h"
#endif
#undef RECURSIVE_SPIN_LOCK_INL_H_

#include "spin_wait.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

inline void TRecursiveSpinLock::Acquire() noexcept
{
    if (TryAcquire()) {
        return;
    }
    TSpinWait spinWait;
    while (!TryAndTryAcquire()) {
        spinWait.Wait();
    }
}

inline bool TRecursiveSpinLock::TryAcquire() noexcept
{
    auto currentThreadId = GetThreadId();
    auto oldValue = Value_.load();
    auto oldRecursionDepth = oldValue & RecursionDepthMask;
    if (oldRecursionDepth > 0 && (oldValue >> ThreadIdShift) != currentThreadId) {
        return false;
    }
    auto newValue = (oldRecursionDepth + 1) | (static_cast<TValue>(currentThreadId) << ThreadIdShift);
    return Value_.compare_exchange_weak(oldValue, newValue);
}

inline void TRecursiveSpinLock::Release() noexcept
{
#ifndef NDEBUG
    auto value = Value_.load();
    YT_ASSERT((value & RecursionDepthMask) > 0);
    YT_ASSERT((value >> ThreadIdShift) == GetThreadId());
#endif
    --Value_;
}

inline bool TRecursiveSpinLock::IsLocked() const noexcept
{
    auto value = Value_.load();
    return (value & RecursionDepthMask) > 0;
}

inline bool TRecursiveSpinLock::IsLockedByCurrentThread() const noexcept
{
    auto value = Value_.load();
    return (value & RecursionDepthMask) > 0 && (value >> ThreadIdShift) == GetThreadId();
}

inline bool TRecursiveSpinLock::TryAndTryAcquire() noexcept
{
    auto value = Value_.load(std::memory_order_relaxed);
    auto recursionDepth = value & RecursionDepthMask;
    if (recursionDepth > 0 && (value >> ThreadIdShift) != GetThreadId()) {
        return false;
    }
    return TryAcquire();
}

inline ui32 TRecursiveSpinLock::GetThreadId() noexcept
{
    thread_local ui32 ThreadId;
    if (Y_UNLIKELY(ThreadId == 0)) {
        static std::atomic<ui32> ThreadIdGenerator;
        ThreadId = ++ThreadIdGenerator;
    }
    return ThreadId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading


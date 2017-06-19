#pragma once
#ifndef EVENT_COUNT_INL_H_
#error "Direct inclusion of this file is not allowed, include event_count.h"
#endif
#undef EVENT_COUNT_INL_H_

#include "futex-inl.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

inline void TEventCount::NotifyOne()
{
    DoNotify(1);
}

inline void TEventCount::NotifyAll()
{
    DoNotify(std::numeric_limits<int>::max());
}

inline void TEventCount::DoNotify(int n)
{
    // The order is important: Epoch is incremented before Waiters is checked.
    // prepareWait() increments Waiters before checking Epoch, so it is
    // impossible to miss a wakeup.
#ifndef _linux_
    TGuard<TMutex> guard(Mutex_);
#endif

    ui64 prev = Value_.fetch_add(AddEpoch, std::memory_order_acq_rel);
    if (Y_UNLIKELY((prev & WaiterMask) != 0)) {
#ifdef _linux_
        NDetail::futex(
            reinterpret_cast<int*>(&Value_) + 1, // assume little-endian architecture
            FUTEX_WAKE_PRIVATE,
            n,
            nullptr,
            nullptr,
            0);
#else
        if (n == 1) {
            ConditionVariable_.Signal();
        } else {
            ConditionVariable_.BroadCast();
        }
#endif
    }
}

inline TEventCount::TCookie TEventCount::PrepareWait()
{
    ui64 prev = Value_.fetch_add(AddWaiter, std::memory_order_acq_rel);
    return TCookie(static_cast<ui32>(prev >> EpochShift));
}

inline void TEventCount::CancelWait()
{
    ui64 prev = Value_.fetch_add(SubWaiter, std::memory_order_seq_cst);
    Y_ASSERT((prev & WaiterMask) != 0);
}

inline void TEventCount::Wait(TCookie cookie)
{
#ifdef _linux_
    while ((Value_.load(std::memory_order_acquire) >> EpochShift) == cookie.Epoch_) {
        NDetail::futex(
            reinterpret_cast<int*>(&Value_) + 1, // assume little-endian architecture
            FUTEX_WAIT_PRIVATE,
            cookie.Epoch_,
            nullptr,
            nullptr,
            0);
    }
#else
    TGuard<TMutex> guard(Mutex_);
    if ((Value_.load(std::memory_order_acquire) >> EpochShift) == cookie.Epoch_) {
        ConditionVariable_.WaitI(Mutex_);
    }
#endif
    ui64 prev = Value_.fetch_add(SubWaiter, std::memory_order_seq_cst);
    Y_ASSERT((prev & WaiterMask) != 0);
}

template <class TCondition>
void TEventCount::Await(TCondition condition)
{
    if (condition()) {
        // Fast path.
        return;
    }

    // condition() is the only thing that may throw, everything else is
    // noexcept, so we can hoist the try/catch block outside of the loop
    try {
        for (;;) {
            auto cookie = PrepareWait();
            if (condition()) {
                CancelWait();
                break;
            } else {
                Wait(cookie);
            }
        }
    } catch (...) {
        CancelWait();
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

inline void TEvent::NotifyOne()
{
    Set_.store(true, std::memory_order_release);
    EventCount_.NotifyOne();
}

inline void TEvent::NotifyAll()
{
    Set_.store(true, std::memory_order_release);
    EventCount_.NotifyAll();
}

inline bool TEvent::Test() const
{
    return Set_.load(std::memory_order_acquire);
}

inline void TEvent::Wait()
{
    EventCount_.Await([=] () { return Set_.load(std::memory_order_acquire); });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

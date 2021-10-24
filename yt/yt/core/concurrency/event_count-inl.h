#pragma once
#ifndef EVENT_COUNT_INL_H_
#error "Direct inclusion of this file is not allowed, include event_count.h"
// For the sake of sane code completion.
#include "event_count.h"
#endif
#undef EVENT_COUNT_INL_H_

#include "futex-inl.h"

#include <errno.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

inline void TEventCount::NotifyOne()
{
    NotifyMany(1);
}

inline void TEventCount::NotifyAll()
{
    NotifyMany(std::numeric_limits<int>::max());
}

inline void TEventCount::NotifyMany(int count)
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
            count,
            nullptr,
            nullptr,
            0);
#else
        if (count == 1) {
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
    YT_ASSERT((prev & WaiterMask) != 0);
}

inline bool TEventCount::Wait(TCookie cookie, std::optional<TDuration> timeout)
{
    bool result = true;
#ifdef _linux_
    struct timespec timeoutSpec;
    if (timeout) {
        timeoutSpec.tv_sec = timeout->Seconds();
        timeoutSpec.tv_nsec = (*timeout - TDuration::Seconds(timeout->Seconds())).MicroSeconds() * 1000;
    }

    while ((Value_.load(std::memory_order_acquire) >> EpochShift) == cookie.Epoch_) {
        auto futexResult = NDetail::futex(
            reinterpret_cast<int*>(&Value_) + 1, // assume little-endian architecture
            FUTEX_WAIT_PRIVATE,
            cookie.Epoch_,
            timeout ? &timeoutSpec : nullptr,
            nullptr,
            0);

        if (futexResult != 0 && errno == ETIMEDOUT) {
            result = false;
            break;
        }
    }
#else
    TGuard<TMutex> guard(Mutex_);
    if ((Value_.load(std::memory_order_acquire) >> EpochShift) == cookie.Epoch_) {
        if (timeout) {
            result = ConditionVariable_.WaitT(Mutex_, *timeout);
        } else {
            ConditionVariable_.WaitI(Mutex_);
        }
    }
#endif
    ui64 prev = Value_.fetch_add(SubWaiter, std::memory_order_seq_cst);
    YT_ASSERT((prev & WaiterMask) != 0);
    return result;
}

template <class TCondition>
bool TEventCount::Await(TCondition condition, std::optional<TDuration> timeout)
{
    if (condition()) {
        // Fast path.
        return true;
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
                auto result = Wait(cookie, timeout);
                if (!result) {
                    return false;
                }
            }
        }
    } catch (...) {
        CancelWait();
        throw;
    }
    return true;
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

inline bool TEvent::Wait(std::optional<TDuration> timeout)
{
    return EventCount_.Await([=] () {
            return Set_.load(std::memory_order_acquire);
        },
        timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#include "count_down_latch.h"
#include "futex-inl.h"

#include <yt/core/misc/error.h>

#include <cerrno>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TCountDownLatch::TCountDownLatch(size_t count)
    : Count_(count)
{ }

void TCountDownLatch::CountDown()
{
#ifndef _linux_
    TGuard<TMutex> guard(Mutex_);
#endif
    auto previous = Count_.fetch_sub(1, std::memory_order_release);
    if (previous == 1) {
#ifdef _linux_
        int rv = NDetail::futex(
            reinterpret_cast<int*>(&Count_),
            FUTEX_WAKE_PRIVATE,
            std::numeric_limits<int>::max(),
            nullptr,
            nullptr,
            0);
        Y_ASSERT(rv >= 0);
        Y_UNUSED(rv);
#else
        ConditionVariable_.BroadCast();
#endif
    }
}

void TCountDownLatch::Wait() const
{
    while (true) {
#ifndef _linux_
        TGuard<TMutex> guard(Mutex_);
#endif
        auto count = Count_.load(std::memory_order_acquire);
        if (count == 0) {
            return;
        }
#ifdef _linux_
        int rv = NDetail::futex(
            const_cast<int*>(reinterpret_cast<const int*>(&Count_)),
            FUTEX_WAIT_PRIVATE,
            count,
            nullptr,
            nullptr,
            0);
        Y_ASSERT(rv >= 0 || errno == EWOULDBLOCK || errno == EINTR);
        Y_UNUSED(rv);
#else
        ConditionVariable_.WaitI(Mutex_);
#endif
    }
}

bool TCountDownLatch::TryWait() const
{
    return Count_.load(std::memory_order_acquire) == 0;
}

size_t TCountDownLatch::GetCount() const
{
    return Count_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency


#include "count_down_latch.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/threading/futex.h>

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
        int rv = NThreading::FutexWake(
            reinterpret_cast<int*>(&Count_),
            std::numeric_limits<int>::max());
        YT_VERIFY(rv >= 0);
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
        int rv = NThreading::FutexWait(
            const_cast<int*>(reinterpret_cast<const int*>(&Count_)),
            count);
        YT_VERIFY(rv >= 0 || errno == EWOULDBLOCK || errno == EINTR);
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


#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/threading/event_count.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TNotifyManager
{
public:
    explicit TNotifyManager(TIntrusivePtr<NThreading::TEventCount> eventCount);

    TCpuInstant GetMinEnqueuedAt() const;

    TCpuInstant UpdateMinEnqueuedAt(TCpuInstant newMinEnqueuedAt);

    void ResetMinEnqueuedAtIfEqual(TCpuInstant expected);

    void NotifyFromInvoke(TCpuInstant cpuInstant);

    // Must be called after DoCancelWait.
    void NotifyAfterFetch(TCpuInstant cpuInstant, TCpuInstant newMinEnqueuedAt);

    void Wait(NThreading::TEventCount::TCookie cookie, std::function<bool()> isStopping);

    void CancelWait();

    NThreading::TEventCount* GetEventCount();

    virtual int GetQueueSize() const = 0;

    int GetWaiters();
    void IncrementWaiters();
    void DecrementWaiters();

private:
    const TIntrusivePtr<NThreading::TEventCount> EventCount_;

    std::atomic<bool> NotifyLock_ = false;
    // LockedInstant is used for debug and check purpose.
    std::atomic<TCpuInstant> LockedInstant_ = 0;
    std::atomic<bool> PollingWaiterLock_ = false;

    std::atomic<TCpuInstant> MinEnqueuedAt_ = 0;

#ifndef NDEBUG
    std::atomic<int> WaitingThreads_ = 0;
#endif

    // Returns true if was locked.
    bool UnlockNotifies();

    void NotifyOne(TCpuInstant cpuInstant);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

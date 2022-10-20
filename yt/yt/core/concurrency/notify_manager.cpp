#include "notify_manager.h"
#include "private.h"

#define PERIODIC_POLLING

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitLimit = TDuration::MicroSeconds(64);
constexpr auto PollingPeriod = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

TNotifyManager::TNotifyManager(
    TIntrusivePtr<NThreading::TEventCount> eventCount,
    const NProfiling::TTagSet& tagSet)
    : EventCount_(std::move(eventCount))
    , WakeupCounter_(NProfiling::TProfiler("/action_queue")
        .WithTags(tagSet)
        .WithHot()
        .Counter("/wakeup"))
{ }

TCpuInstant TNotifyManager::GetMinEnqueuedAt() const
{
    return MinEnqueuedAt_.load(std::memory_order_acquire);
}

TCpuInstant TNotifyManager::UpdateMinEnqueuedAt(TCpuInstant newMinEnqueuedAt)
{
    auto minEnqueuedAt = MinEnqueuedAt_.load();

    while (newMinEnqueuedAt > minEnqueuedAt) {
        if (MinEnqueuedAt_.compare_exchange_weak(minEnqueuedAt, newMinEnqueuedAt)) {
            minEnqueuedAt = newMinEnqueuedAt;
            break;
        }
    }

    return minEnqueuedAt;
}

void TNotifyManager::ResetMinEnqueuedAtIfEqual(TCpuInstant expected)
{
    // Resetting MinEnqueuedAt is needed to force NotifyOne from NotifyFromInvoke when there are no
    // running threads.
    // MinEnqueuedAt denotes no running threads.
    MinEnqueuedAt_.compare_exchange_strong(expected, 0);
}

void TNotifyManager::NotifyFromInvoke(TCpuInstant cpuInstant)
{
    auto minEnqueuedAt = GetMinEnqueuedAt();
    auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

    auto waiters = GetWaiters();

    YT_LOG_TRACE("Notify from invoke (Waiters: %v, WaitTime: %v, MinEnqueuedAt: %v, Decision: %v)",
        waiters,
        waitTime,
        minEnqueuedAt,
        waitTime > WaitLimit);

    if (waitTime > WaitLimit) {
        NotifyOne(cpuInstant);
    }
}

void TNotifyManager::NotifyAfterFetch(TCpuInstant cpuInstant, TCpuInstant newMinEnqueuedAt)
{
    auto minEnqueuedAt = UpdateMinEnqueuedAt(newMinEnqueuedAt);

    YT_VERIFY(minEnqueuedAt > 0);

    // If there are actions and wait time is small do not wakeup other threads.
    auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

    if (waitTime > WaitLimit) {
        auto waiters = GetWaiters();
        YT_LOG_TRACE("Notify after fetch (Waiters: %v, WaitTime: %v, MinEnqueuedAt: %v)",
            waiters,
            waitTime,
            minEnqueuedAt);

        NotifyOne(cpuInstant);
    }

    // Reset LockedInstant to suppress action stuck warnings in case of progress.
    LockedInstant_ = cpuInstant;
}

void TNotifyManager::Wait(NThreading::TEventCount::TCookie cookie, std::function<bool()> isStopping)
{
    if (UnlockNotifies()) {
        // We must call either Wait or CancelWait.
        EventCount_->CancelWait();
        return;
    }

#ifdef PERIODIC_POLLING
    // One waiter makes periodic polling.
    bool firstWaiter = !PollingWaiterLock_.exchange(true);
    if (firstWaiter) {
        while (true) {
            bool notified = EventCount_->Wait(cookie, PollingPeriod);
            if (notified) {
                break;
            }

            if (GetQueueSize() > 0) {
                // Check wait time.
                auto minEnqueuedAt = GetMinEnqueuedAt();
                auto cpuInstant = GetCpuInstant();
                auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

                if (waitTime > WaitLimit) {
                    YT_LOG_DEBUG("Wake up by timeout (WaitTime: %v, MinEnqueuedAt: %v)",
                        waitTime,
                        minEnqueuedAt);

                    break;
                }
            }

            cookie = EventCount_->PrepareWait();

            // We have to check stopping between Prepare and Wait.
            // If we check before PrepareWait stop can occur (and notify) after check and before prepare
            // wait. In this case we can miss it and go waiting.
            if (isStopping()) {
                EventCount_->CancelWait();
                break;
            }
        }

        PollingWaiterLock_.store(false);
    } else {
        EventCount_->Wait(cookie);
    }
#else
    Y_UNUSED(isStopping);
    Y_UNUSED(PollingPeriod);
    EventCount_->Wait(cookie);
#endif

    UnlockNotifies();

    WakeupCounter_.Increment();
}

void TNotifyManager::CancelWait()
{
    EventCount_->CancelWait();

#ifdef PERIODIC_POLLING
    // If we got an action and PollingWaiterLock_ is not locked (no polling waiter) wake up other thread.
    if (!PollingWaiterLock_.load()) {
        EventCount_->NotifyOne();
    }
#endif
}

NThreading::TEventCount* TNotifyManager::GetEventCount()
{
    return EventCount_.Get();
}

// Returns true if was locked.
bool TNotifyManager::UnlockNotifies()
{
    return NotifyLock_.exchange(false);
}

void TNotifyManager::NotifyOne(TCpuInstant cpuInstant)
{
    if (!NotifyLock_.exchange(true)) {
        LockedInstant_ = cpuInstant;
        YT_LOG_TRACE("Futex Wake (MinEnqueuedAt: %v)",
            GetMinEnqueuedAt());
        EventCount_->NotifyOne();
    } else {
        auto lockedInstant = LockedInstant_.load();
        auto waitTime = CpuDurationToDuration(cpuInstant - lockedInstant);
        if (waitTime > TDuration::Seconds(30)) {
            // Notifications are locked during more than 90 seconds.
            YT_LOG_WARNING("Action is probably stuck (MinEnqueuedAt: %v, LockedInstant: %v, WaitTime: %v, QueueSize: %v)",
                GetMinEnqueuedAt(),
                lockedInstant,
                waitTime,
                GetQueueSize());
        }
    }
}

int TNotifyManager::GetWaiters()
{
#ifndef NDEBUG
    return WaitingThreads_.load();
#else
    return -1;
#endif
}

void TNotifyManager::IncrementWaiters()
{
#ifndef NDEBUG
    ++WaitingThreads_;
#endif
}
void TNotifyManager::DecrementWaiters()
{
#ifndef NDEBUG
    --WaitingThreads_;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

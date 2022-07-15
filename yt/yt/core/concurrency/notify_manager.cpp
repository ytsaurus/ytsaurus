#include "notify_manager.h"
#include "private.h"

#define PERIODIC_POLLING

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitLimit = TDuration::MicroSeconds(64);
constexpr auto PollingPeriod = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

TNotifyManager::TNotifyManager(TIntrusivePtr<NThreading::TEventCount> eventCount)
    : EventCount_(std::move(eventCount))
{ }

TCpuInstant TNotifyManager::ResetMinEnqueuedAt()
{
    // Disables notifies of already enqueued actions from invoke and
    // allows to set MinEnqueuedAt in NotifyFromInvoke for new actions.
    return MinEnqueuedAt_.exchange(std::numeric_limits<TCpuInstant>::max());
}

void TNotifyManager::NotifyFromInvoke(TCpuInstant cpuInstant, bool updateEnqueuedAt, int threadCount)
{
    if (updateEnqueuedAt) {
        auto minEnqueuedAt = std::numeric_limits<TCpuInstant>::max();
        MinEnqueuedAt_.compare_exchange_strong(minEnqueuedAt, cpuInstant);
    }

    auto minEnqueuedAt = MinEnqueuedAt_.load(std::memory_order_acquire);
    auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

    // TODO(lukyan): Explicitly check running thread count.
    auto waiters = WaitingThreads.load();

    bool noRunningThreads = waiters == threadCount;

    YT_LOG_TRACE("Notify from invoke (Waiters: %v, WaitTime: %v, MinEnqueuedAt: %v, Decision: %v)",
        waiters,
        waitTime,
        minEnqueuedAt,
        noRunningThreads || waitTime > WaitLimit);

    if (noRunningThreads || waitTime > WaitLimit) {
        NotifyOne(cpuInstant);
    }
}

void TNotifyManager::NotifyAfterFetch(TCpuInstant cpuInstant, TCpuInstant newMinEnqueuedAt)
{
    // Can be set in NotifyFromInvoke.
    auto minEnqueuedAt = MinEnqueuedAt_.load();

    // Expecting max value sentinel in minEnqueuedAt or minEnqueuedAt from NotifyFromInvoke.
    while (newMinEnqueuedAt < minEnqueuedAt) {
        YT_VERIFY(newMinEnqueuedAt != std::numeric_limits<TCpuInstant>::max());
        if (MinEnqueuedAt_.compare_exchange_weak(minEnqueuedAt, newMinEnqueuedAt)) {
            minEnqueuedAt = newMinEnqueuedAt;
            break;
        }
    }

    // If there are actions and wait time is small do not wakeup other threads.
    auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

    if (waitTime > WaitLimit) {
        auto waiters = WaitingThreads.load();
        YT_LOG_TRACE("Notify after fetch (Waiters: %v, WaitTime: %v, MinEnqueuedAt: %v)",
            waiters,
            waitTime,
            minEnqueuedAt);

        NotifyOne(cpuInstant);
    }
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

            // Check wait time.
            auto minEnqueuedAt = MinEnqueuedAt_.load();
            auto cpuInstant = GetCpuInstant();
            auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

            if (waitTime > WaitLimit) {
                YT_LOG_DEBUG("Wake up by timeout (WaitTime: %v, MinEnqueuedAt: %v)",
                    waitTime,
                    minEnqueuedAt);

                break;
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
    LockedInstant_ = std::numeric_limits<TCpuInstant>::max();
    return NotifyLock_.exchange(false);
}

void TNotifyManager::NotifyOne(TCpuInstant cpuInstant)
{
    if (!NotifyLock_.exchange(true)) {
        LockedInstant_ = cpuInstant;
        YT_LOG_TRACE("Futex Wake (MinEnqueuedAt: %v)",
            MinEnqueuedAt_.load());
        EventCount_->NotifyOne();
    } else {
        auto lockedInstant = LockedInstant_.load();
        auto waitTime = CpuDurationToDuration(cpuInstant - lockedInstant);
        if (waitTime > TDuration::Seconds(30)) {
            // Notifications are locked during more than 30 seconds.
            YT_LOG_WARNING("Action is probably stuck (MinEnqueuedAt: %v, LockedInstant: %v, WaitTime: %v, QueueSize: %v)",
                MinEnqueuedAt_.load(),
                lockedInstant,
                waitTime,
                GetQueueSize());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

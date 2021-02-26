#include "delayed_executor.h"
#include "action_queue.h"
#include "scheduler.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>
#include <yt/core/misc/proc.h>

#include <util/datetime/base.h>

#if defined(_linux_) && !defined(_bionic_)

#define HAVE_TIMERFD

#include "notification_handle.h"

#include <util/network/pollerimpl.h>

#include <sys/timerfd.h>

#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#if !defined(HAVE_TIMERFD)
static constexpr auto SleepQuantum = TDuration::MilliSeconds(10);
#endif

static constexpr auto CoalescingInterval = TDuration::MicroSeconds(100);
static constexpr auto LateWarningThreshold = TDuration::Seconds(1);

static const auto& Logger = ConcurrencyLogger;

const TDelayedExecutorCookie NullDelayedExecutorCookie;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TDelayedExecutorEntry
    : public TRefCounted
{
    struct TComparer
    {
        bool operator()(const TDelayedExecutorCookie& lhs, const TDelayedExecutorCookie& rhs) const
        {
            if (lhs->Deadline != rhs->Deadline) {
                return lhs->Deadline < rhs->Deadline;
            }
            // Break ties.
            return lhs < rhs;
        }
    };

    TDelayedExecutorEntry(
        TDelayedExecutor::TDelayedCallback callback,
        TInstant deadline,
        IInvokerPtr invoker)
        : Callback(std::move(callback))
        , Deadline(deadline)
        , Invoker(std::move(invoker))
    { }

    TDelayedExecutor::TDelayedCallback Callback;
    TInstant Deadline;
    IInvokerPtr Invoker;

    bool Canceled = false;
    std::optional<std::set<TDelayedExecutorCookie, TComparer>::iterator> Iterator;
};

DEFINE_REFCOUNTED_TYPE(TDelayedExecutorEntry)

} // namespace NDetail

using NDetail::TDelayedExecutorEntry;
using NDetail::TDelayedExecutorEntryPtr;

////////////////////////////////////////////////////////////////////////////////

class TDelayedExecutor::TImpl
{
public:
    TImpl()
        : PollerThread_(&PollerThreadMain, static_cast<void*>(this))
#if defined(HAVE_TIMERFD)
        , TimerFD_(timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK))
#endif
    {
#if defined(HAVE_TIMERFD)
        YT_VERIFY(TimerFD_ >= 0);
        Poller_.Set(&TimerFD_, TimerFD_, CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
        Poller_.Set(&WakeupHandle_, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
#endif
    }

#if defined(HAVE_TIMERFD)
    ~TImpl()
    {
        YT_VERIFY(close(TimerFD_) == 0);
    }
#endif

    TFuture<void> MakeDelayed(TDuration delay, IInvokerPtr invoker)
    {
        auto promise = NewPromise<void>();

        auto cookie = Submit(
            BIND_DONT_CAPTURE_TRACE_CONTEXT([=] (bool aborted) mutable {
                if (aborted) {
                    promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed promise aborted"));
                } else {
                    promise.TrySet();
                }
            }),
            delay,
            std::move(invoker));

        promise.OnCanceled(BIND_DONT_CAPTURE_TRACE_CONTEXT([=, cookie = std::move(cookie)] (const TError& error) {
            TDelayedExecutor::Cancel(cookie);
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed callback canceled")
                << error);
        }));

        return promise;
    }

    void WaitForDuration(TDuration duration)
    {
        if (duration == TDuration::Zero()) {
            return;
        }

        auto error = WaitFor(MakeDelayed(duration, nullptr));
        if (error.GetCode() == NYT::EErrorCode::Canceled) {
            throw TFiberCanceledException();
        }

        error.ThrowOnError();
    }

    TDelayedExecutorCookie Submit(TClosure closure, TDuration delay, IInvokerPtr invoker)
    {
        YT_VERIFY(closure);
        return Submit(
            BIND_DONT_CAPTURE_TRACE_CONTEXT(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            delay.ToDeadLine(),
            std::move(invoker));
    }

    TDelayedExecutorCookie Submit(TClosure closure, TInstant deadline, IInvokerPtr invoker)
    {
        YT_VERIFY(closure);
        return Submit(
            BIND_DONT_CAPTURE_TRACE_CONTEXT(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            deadline,
            std::move(invoker));
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TDuration delay, IInvokerPtr invoker)
    {
        YT_VERIFY(callback);
        return Submit(
            std::move(callback),
            delay.ToDeadLine(),
            std::move(invoker));
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TInstant deadline, IInvokerPtr invoker)
    {
        YT_VERIFY(callback);
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline, std::move(invoker));
        SubmitQueue_.Enqueue(entry);

#if defined(HAVE_TIMERFD)
        ScheduleImmediateWakeup();
#endif

        if (!EnsureStarted()) {
            // Failure in #EnsureStarted indicates that the Poller Thread has already been
            // shut down. It is guaranteed that no access to #entry is possible at this point
            // and it is thus safe to run the handler right away. Checking #TDelayedExecutorEntry::Callback
            // for null ensures that we don't attempt to re-run the callback (cf. #PollerThreadStep).
            if (entry->Callback) {
                entry->Callback.Run(true);
                entry->Callback.Reset();
            }
        }
        return entry;
    }

    void Cancel(TDelayedExecutorEntryPtr entry)
    {
        if (!entry) {
            return;
        }
        CancelQueue_.Enqueue(std::move(entry));
        // No #EnsureStarted call is needed here: having #entry implies that #Submit call has been previously made.
        // Also in contrast to #Submit we have no special handling for #entry in case the Poller Thread
        // has been already terminated.
    }

    void Shutdown()
    {
        {
            auto guard = Guard(SpinLock_);
            Stopping_ = true;
            if (!Started_) {
                return;
            }
        }
#if defined(HAVE_TIMERFD)
        ScheduleImmediateWakeup();
#endif
        PollerThread_.Join();
    }

private:
    //! Only touched from DelayedPoller thread.
    std::set<TDelayedExecutorEntryPtr, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

    //! Enqueued from any thread, dequeued from DelayedPoller thread.
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> SubmitQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> CancelQueue_;

    TThread PollerThread_;

#if defined(HAVE_TIMERFD)
    int TimerFD_;

    TNotificationHandle WakeupHandle_;
    std::atomic<bool> WakeupScheduled_ = false;

    struct TMutexLocking
    {
        using TMyMutex = TMutex;
    };
    TPollerImpl<TMutexLocking> Poller_;
#endif

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    TActionQueuePtr DelayedQueue_;
    IInvokerPtr DelayedInvoker_;

    std::atomic<bool> Started_ = false;
    std::atomic<bool> Stopping_ = false;
    const TPromise<void> Stopped_ = NewPromise<void>();

    static thread_local bool InDelayedPollerThread_;

    NProfiling::TGauge ScheduledCallbacksGauge_ = ConcurrencyProfiler.Gauge("/delayed_executor/scheduled_callbacks");
    NProfiling::TCounter SubmittedCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/submitted_callbacks");
    NProfiling::TCounter CanceledCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/canceled_callbacks");
    NProfiling::TCounter StaleCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/stale_callbacks");

    /*!
     * If |true| is returned then it is guaranteed that all entries enqueued up to this call
     * are (or will be) dequeued and taken care of by the Poller Thread.
     *
     * If |false| is returned then the Poller Thread has been already shut down.
     * It is guaranteed that no further handling will take place.
     */
    bool EnsureStarted()
    {
        auto handleStarted = [&] (TSpinlockGuard<TAdaptiveLock>* guard) {
            if (Stopping_) {
                if (guard) {
                    guard->Release();
                }
                // Must wait for the Poller Thread to finish to prevent simultaneous access to shared state.
                // Also must avoid deadlock when EnsureStarted in called within Poller Thread; cf. YT-10766.
                if (!InDelayedPollerThread_) {
                    Stopped_.Get();
                }
                return false;
            } else {
                return true;
            }
        };

        if (Started_) {
            return handleStarted(nullptr);
        }

        auto guard = Guard(SpinLock_);

        if (Started_) {
            return handleStarted(&guard);
        }

        if (Stopping_) {
            // Stopped without being started.
            return false;
        }

        // Boot the Delayed Executor thread up.
        // Do it here to avoid weird crashes when execl is being used in another thread.
        DelayedQueue_ = New<TActionQueue>("DelayedExecutor");
        DelayedInvoker_ = DelayedQueue_->GetInvoker();

        // Finally boot the Poller Thread up.
        // It is crucial for DelayedQueue_ and DelayedInvoker_ to be initialized when
        // PollerThreadMain starts running.
        PollerThread_.Start();

        Started_ = true;

        return true;
    }

    static void* PollerThreadMain(void* opaque)
    {
        static_cast<TImpl*>(opaque)->PollerThreadMain();
        return nullptr;
    }

    void PollerThreadMain()
    {
        TThread::SetCurrentThreadName("DelayedPoller");
        InDelayedPollerThread_ = true;

        // Run the main loop.
        while (!Stopping_) {
#if defined(HAVE_TIMERFD)
            RunPoll();
#else
            Sleep(SleepQuantum);
#endif
            PollerThreadStep();
        }

        // Perform graceful shutdown.

        // First run the scheduled callbacks with |aborted = true|.
        // NB: The callbacks are forwarded to the DelayedExecutor thread to prevent any user-code
        // from leaking to the Delayed Poller thread, which is, e.g., fiber-unfriendly.
        auto runAbort = [&] (const TDelayedExecutorEntryPtr& entry) {
            if (entry->Callback) {
                RunCallback(entry, true);
            }
        };
        for (const auto& entry : ScheduledEntries_) {
            runAbort(entry);
        }
        ScheduledEntries_.clear();

        // Now we handle the queued callbacks similarly.
        SubmitQueue_.DequeueAll(false, runAbort);

        // As for the cancelation queue, we just drop these entries.
        CancelQueue_.DequeueAll(false, [] (const TDelayedExecutorEntryPtr&) { });

        // From now on, shared state is not touched.
        Stopped_.Set();

        // Finally we wait for all callbacks in the Delayed Executor thread to finish running.
        // This certainly cannot prevent any malicious code in the callbacks from starting new fibers there
        // but we don't care.
        BIND_DONT_CAPTURE_TRACE_CONTEXT([] { })
            .AsyncVia(DelayedInvoker_)
            .Run()
            .Get();

        // Shut the Delayed Executor thread down.
        DelayedQueue_->Shutdown();
        DelayedQueue_.Reset();
        DelayedInvoker_.Reset();
    }

#if defined(HAVE_TIMERFD)
    void ScheduleImmediateWakeup()
    {
        if (!WakeupScheduled_.exchange(true)) {
            WakeupHandle_.Raise();
        }
    }

    void ScheduleDelayedWakeup(TDuration delay)
    {
        itimerspec timerValue;
        timerValue.it_value.tv_sec = delay.Seconds();
        timerValue.it_value.tv_nsec = delay.NanoSecondsOfSecond();
        timerValue.it_interval.tv_sec = 0;
        timerValue.it_interval.tv_nsec = 0;
        YT_VERIFY(timerfd_settime(TimerFD_, 0, &timerValue, &timerValue) == 0);
    }

    void RunPoll()
    {
        std::array<decltype(Poller_)::TEvent, 2> events;
        int eventCount = Poller_.Wait(events.data(), events.size(), std::numeric_limits<int>::max());
        for (int index = 0; index < eventCount; ++index) {
            const auto& event = events[index];
            auto* cookie = Poller_.ExtractEvent(&event);
            if (cookie == &TimerFD_) {
                uint64_t value;
                YT_VERIFY(HandleEintr(read, TimerFD_, &value, sizeof(value)) == sizeof(value));
            } else if (cookie == &WakeupHandle_) {
                WakeupHandle_.Clear();
            } else {
                YT_ABORT();
            }
        }
        WakeupScheduled_.store(false);
    }
#endif

    void PollerThreadStep()
    {
        ProcessQueues();
#if defined(HAVE_TIMERFD)
        if (!ScheduledEntries_.empty()) {
            auto deadline = (*ScheduledEntries_.begin())->Deadline;
            auto delay = std::max(CoalescingInterval, deadline - NProfiling::GetInstant());
            ScheduleDelayedWakeup(delay);
        }
#endif
    }

    void ProcessQueues()
    {
        auto now = TInstant::Now();

        int submittedCallbacks = 0;
        SubmitQueue_.DequeueAll(false, [&] (const TDelayedExecutorEntryPtr& entry) {
            if (entry->Canceled) {
                return;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                StaleCallbacksCounter_.Increment();
                YT_LOG_DEBUG("Found a late delayed submitted callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            YT_VERIFY(entry->Callback);
            auto [it, inserted] = ScheduledEntries_.insert(entry);
            YT_VERIFY(inserted);
            entry->Iterator = it;
            ++submittedCallbacks;
        });
        SubmittedCallbacksCounter_.Increment(submittedCallbacks);

        int canceledCallbacks = 0;
        CancelQueue_.DequeueAll(false, [&] (const TDelayedExecutorEntryPtr& entry) {
            if (entry->Canceled) {
                return;
            }
            entry->Canceled = true;
            entry->Callback.Reset();
            if (entry->Iterator) {
                ScheduledEntries_.erase(*entry->Iterator);
                entry->Iterator.reset();
            }
            ++canceledCallbacks;
        });
        CanceledCallbacksCounter_.Increment(submittedCallbacks);

        ScheduledCallbacksGauge_.Update(ScheduledEntries_.size());

        while (!ScheduledEntries_.empty()) {
            auto it = ScheduledEntries_.begin();
            const auto& entry = *it;
            if (entry->Deadline > now + CoalescingInterval) {
                break;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                StaleCallbacksCounter_.Increment();
                YT_LOG_DEBUG("Found a late delayed scheduled callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            YT_VERIFY(entry->Callback);
            RunCallback(entry, false);
            entry->Iterator.reset();
            ScheduledEntries_.erase(it);
        }
    }

    void RunCallback(const TDelayedExecutorEntryPtr& entry, bool abort)
    {
        (entry->Invoker ? entry->Invoker : DelayedInvoker_)->Invoke(BIND_DONT_CAPTURE_TRACE_CONTEXT(std::move(entry->Callback), abort));
    }

    static void ClosureToDelayedCallbackAdapter(const TClosure& closure, bool aborted)
    {
        if (aborted) {
            return;
        }
        closure.Run();
    }
};

thread_local bool TDelayedExecutor::TImpl::InDelayedPollerThread_;

////////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TDelayedExecutor() = default;
TDelayedExecutor::~TDelayedExecutor() = default;

TDelayedExecutor::TImpl* TDelayedExecutor::GetImpl()
{
    return LeakySingleton<TDelayedExecutor::TImpl>();
}

TFuture<void> TDelayedExecutor::MakeDelayed(
    TDuration delay,
    IInvokerPtr invoker)
{
    return GetImpl()->MakeDelayed(delay, std::move(invoker));
}

void TDelayedExecutor::WaitForDuration(TDuration duration)
{
    GetImpl()->WaitForDuration(duration);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TDelayedCallback callback,
    TDuration delay,
    IInvokerPtr invoker)
{
    return GetImpl()->Submit(std::move(callback), delay, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TClosure closure,
    TDuration delay,
    IInvokerPtr invoker)
{
    return GetImpl()->Submit(std::move(closure), delay, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TDelayedCallback callback,
    TInstant deadline,
    IInvokerPtr invoker)
{
    return GetImpl()->Submit(std::move(callback), deadline, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TClosure closure,
    TInstant deadline,
    IInvokerPtr invoker)
{
    return GetImpl()->Submit(std::move(closure), deadline, std::move(invoker));
}

void TDelayedExecutor::Cancel(const TDelayedExecutorCookie& cookie)
{
    GetImpl()->Cancel(cookie);
}

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& cookie)
{
    GetImpl()->Cancel(std::move(cookie));
}

void TDelayedExecutor::StaticShutdown()
{
    GetImpl()->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(3, TDelayedExecutor::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency


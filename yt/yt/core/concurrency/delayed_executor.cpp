#include "delayed_executor.h"
#include "action_queue.h"
#include "scheduler.h"
#include "thread.h"
#include "private.h"

#include <yt/yt/core/misc/mpsc_queue.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/proc.h>

#include <util/datetime/base.h>

#include <util/system/compiler.h>

#if defined(_linux_) && !defined(_bionic_)
#define USE_TIMERFD
#elif defined(_darwin_)
#define USE_KQUEUE
#else
#define USE_SLEEP
#endif

#if defined(USE_TIMERFD) || defined(USE_KQUEUE)

#include "notification_handle.h"

#endif

#if defined(USE_TIMERFD)

#include <util/network/pollerimpl.h>

#include <sys/timerfd.h>

#elif defined(USE_KQUEUE)

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#if defined(USE_SLEEP)
static constexpr auto SleepQuantum = TDuration::MilliSeconds(10);
#endif

static constexpr auto CoalescingInterval = TDuration::MicroSeconds(100);
static constexpr auto LateWarningThreshold = TDuration::Seconds(1);

static const auto& Logger = ConcurrencyLogger;

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
    std::atomic<bool> CallbackTaken = false;
    TInstant Deadline;
    IInvokerPtr Invoker;

    bool Canceled = false;
    std::optional<std::set<TDelayedExecutorCookie, TComparer>::iterator> Iterator;
};

DEFINE_REFCOUNTED_TYPE(TDelayedExecutorEntry)

////////////////////////////////////////////////////////////////////////////////

class TDelayedExecutorImpl
{
public:
    using TDelayedCallback = TDelayedExecutor::TDelayedCallback;

    static TDelayedExecutorImpl* Get()
    {
        return LeakySingleton<TDelayedExecutorImpl>();
    }

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
        PollerThread_->EnqueueSubmission(entry);

#if defined(USE_TIMERFD) || defined(USE_KQUEUE)
        PollerThread_->ScheduleImmediateWakeup();
#endif

        if (!PollerThread_->Start()) {
            if (auto callback = TakeCallback(entry)) {
                callback(/*aborted*/ true);
            }
#if defined(_asan_enabled_)
            NSan::MarkAsIntentionallyLeaked(entry.Get());
#endif
        }

        return entry;
    }

    void Cancel(TDelayedExecutorEntryPtr entry)
    {
        if (!entry) {
            return;
        }
        PollerThread_->EnqueueCancelation(std::move(entry));
        // No #EnsureStarted call is needed here: having #entry implies that #Submit call has been previously made.
        // Also in contrast to #Submit we have no special handling for #entry in case the Poller Thread
        // has been already terminated.
    }

private:
    class TPollerThread
        : public TThread
    {
    public:
        TPollerThread()
            : TThread(
                "DelayedPoller",
                /*shutdownPriority*/ 200)
#if defined(USE_TIMERFD)
            , TimerFD_(timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK))
#elif defined(USE_KQUEUE)
            , KQueueFD_(kqueue())
#endif
        {
#if defined(USE_TIMERFD)
            YT_VERIFY(TimerFD_ >= 0);
            Poller_.Set(&TimerFD_, TimerFD_, CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
            Poller_.Set(&WakeupHandle_, WakeupHandle_.GetFD(), CONT_POLL_EDGE_TRIGGERED | CONT_POLL_READ);
#elif defined(USE_KQUEUE)
            YT_VERIFY(KQueueFD_ >= 0);
            struct kevent event {
                .ident = static_cast<uintptr_t>(WakeupHandle_.GetFD()),
                .filter = EVFILT_READ,
                .flags = EV_ADD | EV_ENABLE,
                .fflags = 0,
                .data = static_cast<intptr_t>(0),
                .udata = nullptr,
            };
            auto result = HandleEintr(
                kevent,
                KQueueFD_,
                /*changelist*/ &event,
                /*nchanges*/ 1,
                /*eventlist*/ nullptr,
                /*nevents*/ 0,
                /*timeout*/ nullptr);
            YT_VERIFY(result >= 0);
#endif
        }

        ~TPollerThread()
        {
#if defined(USE_TIMERFD)
            YT_VERIFY(close(TimerFD_) == 0);
#elif defined(USE_KQUEUE)
            YT_VERIFY(close(KQueueFD_) == 0);
#endif
        }

        void EnqueueSubmission(TDelayedExecutorEntryPtr entry)
        {
            SubmitQueue_.Enqueue(std::move(entry));
        }

        void EnqueueCancelation(TDelayedExecutorEntryPtr entry)
        {
            CancelQueue_.Enqueue(std::move(entry));
        }

#if defined(USE_TIMERFD) || defined(USE_KQUEUE)
        void ScheduleImmediateWakeup()
        {
            if (WakeupScheduled_.load(std::memory_order_relaxed)) {
                return;
            }
            if (!WakeupScheduled_.exchange(true)) {
                WakeupHandle_.Raise();
            }
        }
#endif

    private:
        //! Only touched from DelayedPoller thread.
        std::set<TDelayedExecutorEntryPtr, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

        //! Enqueued from any thread, dequeued from DelayedPoller thread.
        TMpscQueue<TDelayedExecutorEntryPtr> SubmitQueue_;
        TMpscQueue<TDelayedExecutorEntryPtr> CancelQueue_;

#if defined(USE_TIMERFD)
        struct TMutexLocking
        {
            using TMyMutex = TMutex;
        };
        TPollerImpl<TMutexLocking> Poller_;
        int TimerFD_ = -1;
#elif defined(USE_KQUEUE)
        int KQueueFD_ = -1;
#endif

#if defined(USE_TIMERFD) || defined(USE_KQUEUE)
        TNotificationHandle WakeupHandle_;
        std::atomic<bool> WakeupScheduled_ = false;
#endif

        YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
        TActionQueuePtr DelayedQueue_;
        IInvokerPtr DelayedInvoker_;

        NProfiling::TGauge ScheduledCallbacksGauge_ = ConcurrencyProfiler.Gauge("/delayed_executor/scheduled_callbacks");
        NProfiling::TCounter SubmittedCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/submitted_callbacks");
        NProfiling::TCounter CanceledCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/canceled_callbacks");
        NProfiling::TCounter StaleCallbacksCounter_ = ConcurrencyProfiler.Counter("/delayed_executor/stale_callbacks");


        void StartPrologue() override
        {
            // Boot the Delayed Executor thread up.
            // Do it here to avoid weird crashes when execl is being used in another thread.
            DelayedQueue_ = New<TActionQueue>("DelayedExecutor");
            DelayedInvoker_ = DelayedQueue_->GetInvoker();
        }

        void StopPrologue() override
        {
#if defined(USE_TIMERFD)
            ScheduleImmediateWakeup();
#endif
        }

        void ThreadMain() override
        {
            // Run the main loop.
            while (!IsStopping()) {
#if defined(USE_TIMERFD) || defined(USE_KQUEUE)
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
                RunCallback(entry, /*aborted*/ true);
            };
            for (const auto& entry : ScheduledEntries_) {
                runAbort(entry);
            }
            ScheduledEntries_.clear();

            // Now we handle the queued callbacks similarly.
            {
                TDelayedExecutorEntryPtr entry;
                while (SubmitQueue_.TryDequeue(&entry)) {
                    runAbort(entry);
                }
            }

            // As for the cancelation queue, we just drop these entries.
            {
                TDelayedExecutorEntryPtr entry;
                while (CancelQueue_.TryDequeue(&entry)) {
                }
            }

            // Gracefully (sic!) shut the Delayed Executor thread down
            // to ensure invocation of the callbacks scheduled above.
            DelayedQueue_->Shutdown(/*graceful*/ true);
        }

#if defined(USE_TIMERFD)
        void ScheduleDelayedWakeup(TDuration delay)
        {
            itimerspec timerValue;
            timerValue.it_value.tv_sec = delay.Seconds();
            timerValue.it_value.tv_nsec = delay.NanoSecondsOfSecond();
            timerValue.it_interval.tv_sec = 0;
            timerValue.it_interval.tv_nsec = 0;
            YT_VERIFY(timerfd_settime(TimerFD_, 0, &timerValue, &timerValue) == 0);
        }
#elif defined(USE_KQUEUE)
        void ScheduleDelayedWakeup(TDuration delay)
        {
            struct kevent event {
                .ident = static_cast<uintptr_t>(-1),
                .filter = EVFILT_TIMER,
                .flags = EV_ADD | EV_ENABLE | EV_ONESHOT,
                .fflags = 0,
                .data = static_cast<intptr_t>(delay.MilliSeconds()),
                .udata = nullptr,
            };
            auto result = HandleEintr(
                kevent,
                KQueueFD_,
                /*changelist*/ &event,
                /*nchanges*/ 1,
                /*eventlist*/ nullptr,
                /*nevents*/ 0,
                /*timeout*/ nullptr);
            YT_VERIFY(result >= 0);
        }
#endif

#if defined(USE_TIMERFD)
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
#elif defined(USE_KQUEUE)
        void RunPoll()
        {
            std::array<struct kevent, 2> events;
            int eventCount = HandleEintr(
                kevent,
                KQueueFD_,
                /*changelist*/ nullptr,
                /*nchanges*/ 0,
                /*eventlist*/ events.data(),
                /*nevents*/ events.size(),
                /*timeout*/ nullptr);
            for (int index = 0; index < eventCount; ++index) {
                const auto& event = events[index];
                if (event.ident == static_cast<uintptr_t>(-1)) {
                    // Delayed wakeup.
                } else if (event.ident == static_cast<uintptr_t>(WakeupHandle_.GetFD())) {
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
#if defined(USE_TIMERFD) || defined(USE_KQUEUE)
            if (!ScheduledEntries_.empty()) {
                auto deadline = (*ScheduledEntries_.begin())->Deadline;
                auto delay = std::max(CoalescingInterval, deadline - GetInstant());
                ScheduleDelayedWakeup(delay);
            }
#endif
        }

        void ProcessQueues()
        {
            auto now = TInstant::Now();

            {
                int submittedCallbacks = 0;
                TDelayedExecutorEntryPtr entry;
                while (SubmitQueue_.TryDequeue(&entry)) {
                    if (entry->Canceled) {
                        return;
                    }
                    if (entry->Deadline + LateWarningThreshold < now) {
                        StaleCallbacksCounter_.Increment();
                        YT_LOG_DEBUG("Found a late delayed submitted callback (Deadline: %v, Now: %v)",
                            entry->Deadline,
                            now);
                    }
                    auto [it, inserted] = ScheduledEntries_.insert(entry);
                    YT_VERIFY(inserted);
                    entry->Iterator = it;
                    ++submittedCallbacks;
                }
                SubmittedCallbacksCounter_.Increment(submittedCallbacks);
            }

            {
                int canceledCallbacks = 0;
                TDelayedExecutorEntryPtr entry;
                while (CancelQueue_.TryDequeue(&entry)) {
                    if (entry->Canceled) {
                        return;
                    }
                    entry->Canceled = true;
                    TakeCallback(entry);
                    if (entry->Iterator) {
                        ScheduledEntries_.erase(*entry->Iterator);
                        entry->Iterator.reset();
                    }
                    ++canceledCallbacks;
                }
                CanceledCallbacksCounter_.Increment(canceledCallbacks);
            }

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
                RunCallback(entry, false);
                entry->Iterator.reset();
                ScheduledEntries_.erase(it);
            }
        }

        void RunCallback(const TDelayedExecutorEntryPtr& entry, bool abort)
        {
            if (auto callback = TakeCallback(entry)) {
                (entry->Invoker ? entry->Invoker : DelayedInvoker_)->Invoke(BIND_DONT_CAPTURE_TRACE_CONTEXT(std::move(callback), abort));
            }
        }
    };

    using TPollerThreadPtr = TIntrusivePtr<TPollerThread>;
    const TPollerThreadPtr PollerThread_ = New<TPollerThread>();


    static void ClosureToDelayedCallbackAdapter(const TClosure& closure, bool aborted)
    {
        if (aborted) {
            return;
        }
        closure.Run();
    }

    static TDelayedExecutor::TDelayedCallback TakeCallback(const TDelayedExecutorEntryPtr& entry)
    {
        if (entry->CallbackTaken.exchange(true)) {
            return {};
        } else {
            return std::move(entry->Callback);
        }
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TDelayedExecutor::MakeDelayed(
    TDuration delay,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->MakeDelayed(delay, std::move(invoker));
}

void TDelayedExecutor::WaitForDuration(TDuration duration)
{
    NDetail::TDelayedExecutorImpl::Get()->WaitForDuration(duration);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TDelayedCallback callback,
    TDuration delay,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(callback), delay, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TClosure closure,
    TDuration delay,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(closure), delay, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TDelayedCallback callback,
    TInstant deadline,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(callback), deadline, std::move(invoker));
}

TDelayedExecutorCookie TDelayedExecutor::Submit(
    TClosure closure,
    TInstant deadline,
    IInvokerPtr invoker)
{
    return NDetail::TDelayedExecutorImpl::Get()->Submit(std::move(closure), deadline, std::move(invoker));
}

void TDelayedExecutor::Cancel(const TDelayedExecutorCookie& cookie)
{
    NDetail::TDelayedExecutorImpl::Get()->Cancel(cookie);
}

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& cookie)
{
    NDetail::TDelayedExecutorImpl::Get()->Cancel(std::move(cookie));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency


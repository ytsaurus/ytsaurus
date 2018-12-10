#include "delayed_executor.h"
#include "action_queue.h"
#include "scheduler.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/optional.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(10);
static const auto LateWarningThreshold = TDuration::Seconds(1);
static const auto PeriodicPrecisionWarningThreshold = TDuration::MilliSeconds(100);
static const auto& Logger = ConcurrencyLogger;

const TDelayedExecutorCookie NullDelayedExecutorCookie;

////////////////////////////////////////////////////////////////////////////////

struct TDelayedExecutorEntry
    : public TIntrinsicRefCounted
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

    TDelayedExecutorEntry(TDelayedExecutor::TDelayedCallback callback, TInstant deadline)
        : Callback(std::move(callback))
        , Deadline(deadline)
    { }

    TDelayedExecutor::TDelayedCallback Callback;
    TInstant Deadline;

    bool Canceled = false;
    std::optional<std::set<TDelayedExecutorCookie, TComparer>::iterator> Iterator;
};

DEFINE_REFCOUNTED_TYPE(TDelayedExecutorEntry)

////////////////////////////////////////////////////////////////////////////////

class TDelayedExecutor::TImpl
{
public:
    TImpl()
        : SleeperThread_(&SleeperThreadMain, static_cast<void*>(this))
    { }

    TFuture<void> MakeDelayed(TDuration delay)
    {
        auto promise = NewPromise<void>();

        auto cookie = Submit(
            BIND([=] (bool aborted) mutable {
                if (aborted) {
                    promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed promise aborted"));
                } else {
                    promise.TrySet();
                }
            }),
            delay);

        promise.OnCanceled(BIND([=] () mutable {
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed promise was canceled"));
        }));

        return promise;
    }

    void WaitForDuration(TDuration duration)
    {
        auto error = WaitFor(MakeDelayed(duration));
        if (error.GetCode() == NYT::EErrorCode::Canceled) {
            throw TFiberCanceledException{};
        }

        error.ThrowOnError();
    }

    TDelayedExecutorCookie Submit(TClosure closure, TDuration delay)
    {
        YCHECK(closure);
        return Submit(
            BIND(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TClosure closure, TInstant deadline)
    {
        YCHECK(closure);
        return Submit(
            BIND(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            deadline);
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TDuration delay)
    {
        YCHECK(callback);
        return Submit(std::move(callback), delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TInstant deadline)
    {
        YCHECK(callback);
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline);
        SubmitQueue_.Enqueue(entry);
        if (!EnsureStarted()) {
            // Failure in #EnsureStarted indicates that the Sleeper Thread has already been
            // shut down. It is guaranteed that no access to #entry is possible at this point
            // and it is thus safe to run the handler right away. Checking #TDelayedExecutorEntry::Callback
            // for null ensures that we don't attempt to re-run the callback (cf. #SleeperThreadStep).
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
        // Also in contrast to #Submit we have no special handling for #entry in case the Sleeper Thread
        // has been already terminated.
    }

    void Shutdown()
    {
        auto guard = Guard(SpinLock_);
        Stopping_ = true;
        if (Started_) {
            guard.Release();
            Exited_.Get();
        }
    }

private:
    //! Only touched from DelayedSleeper thread.
    std::set<TDelayedExecutorEntryPtr, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

    //! Enqueued from any thread, dequeued from DelayedSleeper thread.
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> SubmitQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> CancelQueue_;

    TInstant PrevOnTimerInstant_;

    TThread SleeperThread_;

    TSpinLock SpinLock_;
    TActionQueuePtr DelayedQueue_;
    IInvokerPtr DelayedInvoker_;

    std::atomic<bool> Started_ = {false};
    std::atomic<bool> Stopping_ = {false};
    TPromise<void> Stopped_ = NewPromise<void>();
    TPromise<void> Exited_ = NewPromise<void>();

    /*!
     * If |true| is returned then it is guaranteed that all entries enqueued up to this call
     * are (or will be) dequeued and taken care of by the Sleeper Thread.
     *
     * If |false| is returned then the Sleeper Thread has been already shut down.
     * It is guaranteed that no further handling will take place.
     */
    bool EnsureStarted()
    {
        auto handleStarted = [&] (TGuard<TSpinLock>* guard) {
            if (Stopping_) {
                if (guard) {
                    guard->Release();
                }
                // Must wait for the Sleeper Thread to finish to prevent simultaneous access to shared state.
                Stopped_.Get();
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
        DelayedQueue_ = New<TActionQueue>("DelayedExecutor", false, false);
        DelayedInvoker_ = DelayedQueue_->GetInvoker();

        // Finally boot the Sleeper Thread up.
        // It is crucial for DelayedQueue_ and DelayedInvoker_ to be initialized when
        // SleeperThreadMain starts running.
        SleeperThread_.Start();

        Started_ = true;

        return true;
    }

    static void* SleeperThreadMain(void* opaque)
    {
        static_cast<TImpl*>(opaque)->SleeperThreadMain();
        return nullptr;
    }

    void SleeperThreadMain()
    {
        TThread::CurrentThreadSetName("DelayedSleeper");

        // Run the main loop.
        while (!Stopping_) {
            Sleep(SleepQuantum);
            SleeperThreadStep();
        }

        // Perform graceful shutdown.

        // First run the scheduled callbacks with |aborted = true|.
        // NB: The callbacks are forwarded to the DelayedExecutor thread to prevent any user-code
        // from leaking to the Delayed Sleeper thread, which is, e.g., fiber-unfriendly.
        auto runAbort = [&] (const TDelayedExecutorEntryPtr& entry) {
            if (entry->Callback) {
                DelayedInvoker_->Invoke(BIND(std::move(entry->Callback), true));
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
        BIND([] () { })
            .AsyncVia(DelayedInvoker_)
            .Run()
            .Get();

        // Shut the Delayed Executor thread down.
        DelayedQueue_->Shutdown();
        DelayedQueue_.Reset();
        DelayedInvoker_.Reset();

        // Release those waiting for shutdown.
        Exited_.Set();
    }

    void SleeperThreadStep()
    {
        auto now = TInstant::Now();

        if (PrevOnTimerInstant_ != TInstant::Zero() && now - PrevOnTimerInstant_ > PeriodicPrecisionWarningThreshold) {
            LOG_DEBUG("Delayed executor stall detected (Delta: %v)",
                now - PrevOnTimerInstant_);
        }

        PrevOnTimerInstant_ = now;

        SubmitQueue_.DequeueAll(false, [&] (const TDelayedExecutorEntryPtr& entry) {
            if (entry->Canceled) {
                return;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                LOG_DEBUG("Found a late delayed submitted callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            YCHECK(entry->Callback);
            auto pair = ScheduledEntries_.insert(entry);
            YCHECK(pair.second);
            entry->Iterator = pair.first;
        });

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
        });

        while (!ScheduledEntries_.empty()) {
            auto it = ScheduledEntries_.begin();
            const auto& entry = *it;
            if (entry->Deadline > now) {
                break;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                LOG_DEBUG("Found a late delayed scheduled callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            YCHECK(entry->Callback);
            DelayedInvoker_->Invoke(BIND(std::move(entry->Callback), false));
            entry->Iterator.reset();
            ScheduledEntries_.erase(it);
        }
    }

    static void ClosureToDelayedCallbackAdapter(const TClosure& closure, bool aborted)
    {
        if (aborted) {
            return;
        }
        closure.Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TDelayedExecutor() = default;
TDelayedExecutor::~TDelayedExecutor() = default;

TDelayedExecutor::TImpl* TDelayedExecutor::GetImpl()
{
    return Singleton<TDelayedExecutor::TImpl>();
}

TFuture<void> TDelayedExecutor::MakeDelayed(TDuration delay)
{
    return GetImpl()->MakeDelayed(delay);
}

void TDelayedExecutor::WaitForDuration(TDuration duration)
{
    GetImpl()->WaitForDuration(duration);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TDelayedCallback callback, TDuration delay)
{
    return GetImpl()->Submit(std::move(callback), delay);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure closure, TDuration delay)
{
    return GetImpl()->Submit(std::move(closure), delay);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TDelayedCallback callback, TInstant deadline)
{
    return GetImpl()->Submit(std::move(callback), deadline);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure closure, TInstant deadline)
{
    return GetImpl()->Submit(std::move(closure), deadline);
}

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& cookie)
{
    GetImpl()->Cancel(cookie);
    cookie.Reset();
}

void TDelayedExecutor::StaticShutdown()
{
    GetImpl()->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(3, TDelayedExecutor::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

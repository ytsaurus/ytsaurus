#include "delayed_executor.h"
#include "action_queue.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/singleton.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(1);
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
    TNullable<std::set<TDelayedExecutorCookie, TComparer>::iterator> Iterator;
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

    static void ClosureToDelayedCallbackAdapter(const TClosure& closure, bool aborted)
    {
        if (aborted) {
            return;
        }
        closure.Run();
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TDuration delay)
    {
        return Submit(std::move(callback), delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TClosure closure, TDuration delay)
    {
        return Submit(
            BIND(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TClosure closure, TInstant deadline)
    {
        return Submit(
            BIND(&ClosureToDelayedCallbackAdapter, std::move(closure)),
            deadline);
    }

    TDelayedExecutorCookie Submit(TDelayedCallback callback, TInstant deadline)
    {
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline);
        SubmitQueue_.Enqueue(entry);
        EnsureStarted();
        PurgeQueuesIfFinished();
        return entry;
    }

    void Cancel(TDelayedExecutorCookie entry)
    {
        if (!entry) {
            return;
        }
        EnsureStarted();
        CancelQueue_.Enqueue(std::move(entry));
        PurgeQueuesIfFinished();
    }

    void Shutdown()
    {
        bool doJoinSleeper;

        {
            auto guard = Guard(SpinLock_);

            if (Finished_) {
                return;
            }

            Finished_ = true;
            doJoinSleeper = Started_;
        }

        if (doJoinSleeper) {
            SleeperThread_.Join();
            DelayedQueue_->Shutdown();
            DelayedQueue_.Reset();
            DelayedInvoker_.Reset();
        }

        PurgeQueues();
    }

private:
    //! Only touched from DelayedSleeper thread.
    std::set<TDelayedExecutorCookie, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

    //! Enqueued from any thread, dequeued from DelayedSleeper thread.
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> SubmitQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> CancelQueue_;

    TInstant PrevOnTimerInstant_;

    TThread SleeperThread_;
    TActionQueuePtr DelayedQueue_;
    IInvokerPtr DelayedInvoker_;

    std::atomic<bool> Started_ = {false};
    std::atomic<bool> Finished_ = {false};
    TSpinLock SpinLock_;


    bool EnsureStarted()
    {
        if (Started_) {
            return true;
        }

        auto guard = Guard(SpinLock_);

        if (Started_) {
            return true;
        }

        if (Finished_) {
            return false;
        }

        DelayedQueue_ = New<TActionQueue>("DelayedExecutor", false, false);
        DelayedInvoker_ = DelayedQueue_->GetInvoker();
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

        while (!Finished_) {
            Sleep(SleepQuantum);
            SleeperThreadStep();
        }

        for (const auto& entry : ScheduledEntries_) {
            PurgeEntry(entry);
        }

        ScheduledEntries_.clear();
    }

    void SleeperThreadStep()
    {
        auto now = TInstant::Now();

        if (PrevOnTimerInstant_ != TInstant::Zero() && now - PrevOnTimerInstant_ > PeriodicPrecisionWarningThreshold) {
            LOG_WARNING("Delayed executor stall detected (Delta: %v)",
                now - PrevOnTimerInstant_);
        }

        PrevOnTimerInstant_ = now;

        SubmitQueue_.DequeueAll(false, [&] (const TDelayedExecutorEntryPtr& entry) {
            if (entry->Canceled) {
                return;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                LOG_WARNING("Found a late delayed submitted callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
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
                entry->Iterator.Reset();
            }
        });

        while (!ScheduledEntries_.empty()) {
            auto it = ScheduledEntries_.begin();
            const auto& entry = *it;
            if (entry->Deadline > now) {
                break;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                LOG_WARNING("Found a late delayed scheduled callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            auto boundFalseCallback = BIND(entry->Callback, false);
            auto boundTrueCallback = BIND(entry->Callback, true);
            entry->Callback.Reset();
            entry->Iterator.Reset();
            ScheduledEntries_.erase(it);
            GuardedInvoke(DelayedInvoker_, std::move(boundFalseCallback), std::move(boundTrueCallback));
        }
    }

    void PurgeQueues()
    {
        SubmitQueue_.DequeueAll(false, &TImpl::PurgeEntry);
        CancelQueue_.DequeueAll(false, &TImpl::PurgeEntry);
    }

    void PurgeQueuesIfFinished()
    {
        if (Finished_) {
            PurgeQueues();
        }
    }

    static void PurgeEntry(const TDelayedExecutorEntryPtr &entry)
    {
        if (entry->Callback) {
            entry->Callback.Run(true);
            entry->Callback.Reset();
        }
        if (entry->Iterator) {
            entry->Iterator.Reset();
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

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

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& entry)
{
    GetImpl()->Cancel(entry);
    entry.Reset();
}

void TDelayedExecutor::StaticShutdown()
{
    GetImpl()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

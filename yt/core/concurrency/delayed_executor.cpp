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

    TDelayedExecutorEntry(TClosure callback, TInstant deadline)
        : Deadline(deadline)
        , Callback(std::move(callback))
    { }

    bool Canceled = false;
    TInstant Deadline;
    TClosure Callback;
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
        Submit(
            BIND([=] () mutable {
                promise.TrySet();
            }),
            delay);
        promise.OnCanceled(
            BIND([=] () mutable {
                promise.TrySet(TError(NYT::EErrorCode::Canceled, "Delayed promise canceled"));
            }));
        return promise;
    }

    TDelayedExecutorCookie Submit(TClosure callback, TDuration delay)
    {
        return Submit(std::move(callback), delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TClosure callback, TInstant deadline)
    {
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline);
        if (!EnsureStarted()) {
            return entry;
        }
        SubmitQueue_.Enqueue(std::move(entry));
        PurgeQueuesIfFinished();
        return entry;
    }

    void Cancel(TDelayedExecutorCookie entry)
    {
        if (!entry) {
            return;
        }
        if (!EnsureStarted()) {
            return;
        }
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
            DelayedQueue_->Shutdown();
        }

        if (doJoinSleeper) {
            SleeperThread_.Join();
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
        DelayedQueue_ = New<TActionQueue>("DelayedExecutor");
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
            usleep(SleepQuantum.MicroSeconds());
            SleeperThreadStep();
        }
    }

    void SleeperThreadStep()
    {
        auto now = TInstant::Now();
        if (PrevOnTimerInstant_ != TInstant::Zero() && now - PrevOnTimerInstant_ > PeriodicPrecisionWarningThreshold) {
            LOG_WARNING("Periodic watcher stall detected (Delta: %v)",
                now - PrevOnTimerInstant_);
        }
        PrevOnTimerInstant_ = now;

        TDelayedExecutorEntryPtr entry;

        while (SubmitQueue_.Dequeue(&entry)) {
            if (entry->Canceled) {
                continue;
            }
            if (entry->Deadline + LateWarningThreshold < now) {
                LOG_WARNING("Found a late delayed submitted callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            auto pair = ScheduledEntries_.insert(entry);
            YCHECK(pair.second);
            entry->Iterator = pair.first;
        }

        while (CancelQueue_.Dequeue(&entry)) {
            if (entry->Canceled) {
                continue;
            }
            entry->Canceled = true;
            entry->Callback.Reset();
            if (entry->Iterator) {
                ScheduledEntries_.erase(*entry->Iterator);
                entry->Iterator.Reset();
            }
        }

        auto invoker = DelayedQueue_->GetInvoker();
        while (!ScheduledEntries_.empty()) {
            auto it = ScheduledEntries_.begin();
            const auto& entry = *it;
            if (entry->Deadline > now)
                break;
            if (entry->Deadline + LateWarningThreshold < now) {
                LOG_WARNING("Found a late delayed scheduled callback (Deadline: %v, Now: %v)",
                    entry->Deadline,
                    now);
            }
            invoker->Invoke(entry->Callback);
            entry->Callback.Reset();
            entry->Iterator.Reset();
            ScheduledEntries_.erase(it);
        }
    }

    void PurgeQueues()
    {
        SubmitQueue_.DequeueAll();
        CancelQueue_.DequeueAll();
    }

    void PurgeQueuesIfFinished()
    {
        if (Finished_) {
            PurgeQueues();
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TDelayedExecutor() = default;
TDelayedExecutor::~TDelayedExecutor() = default;

TDelayedExecutor::TImpl* const TDelayedExecutor::GetImpl()
{
    return Singleton<TDelayedExecutor::TImpl>();
}

TFuture<void> TDelayedExecutor::MakeDelayed(TDuration delay)
{
    return GetImpl()->MakeDelayed(delay);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure callback, TDuration delay)
{
    return GetImpl()->Submit(std::move(callback), delay);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure callback, TInstant deadline)
{
    return GetImpl()->Submit(std::move(callback), deadline);
}

void TDelayedExecutor::Cancel(TDelayedExecutorCookie entry)
{
    GetImpl()->Cancel(std::move(entry));
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

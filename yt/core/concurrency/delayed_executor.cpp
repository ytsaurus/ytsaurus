#include "delayed_executor.h"
#include "ev_scheduler_thread.h"
#include "private.h"

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/singleton.h>

#include <util/datetime/base.h>

#include <yt/contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto TimeQuantum = TDuration::MilliSeconds(1);
static const auto LateWarningThreshold = TDuration::Seconds(1);
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
    : public TEVSchedulerThread
{
public:
    TImpl()
        : TEVSchedulerThread("DelayedExecutor", false)
        , PeriodicWatcher_(EventLoop)
    {
        PeriodicWatcher_.set<TImpl, &TImpl::OnTimer>(this);
        PeriodicWatcher_.start(0, TimeQuantum.SecondsFloat());
    }

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
        if (!IsShutdown()) {
            if (!IsStarted()) {
                Start();
            }
            SubmitQueue_.Enqueue(std::move(entry));
        }
        if (IsShutdown()) {
            PurgeQueues();
        }
        return entry;
    }

    void Cancel(TDelayedExecutorCookie entry)
    {
        if (entry && !IsShutdown()) {
            if (!IsStarted()) {
                Start();
            }
            CancelQueue_.Enqueue(std::move(entry));
        }
        if (IsShutdown()) {
            PurgeQueues();
        }
    }

private:
    ev::periodic PeriodicWatcher_;

    //! Only touched from the dedicated thread.
    std::set<TDelayedExecutorCookie, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

    //! Enqueued from any thread, dequeued from the dedicated thread.
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> SubmitQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> CancelQueue_;


    virtual void AfterShutdown() override
    {
        TEVSchedulerThread::AfterShutdown();
        PurgeQueues();
    }

    void OnTimer(ev::periodic&, int)
    {
        TDelayedExecutorEntryPtr entry;

        auto now = TInstant::Now();

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
            EnqueueCallback(entry->Callback);
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
};

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TDelayedExecutor()
    : Impl_(New<TImpl>())
{ }

TDelayedExecutor::~TDelayedExecutor() = default;

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TImpl* const TDelayedExecutor::GetImpl()
{
    return RefCountedSingleton<TDelayedExecutor::TImpl>().Get();
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

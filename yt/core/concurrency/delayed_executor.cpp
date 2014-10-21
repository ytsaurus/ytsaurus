#include "stdafx.h"
#include "delayed_executor.h"
#include "action_queue_detail.h"

#include <core/misc/singleton.h>
#include <core/misc/lock_free.h>
#include <core/misc/nullable.h>

#include <util/datetime/base.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto TimeQuantum = TDuration::MilliSeconds(10);

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
        : TEVSchedulerThread(
            "DelayedExecutor",
            false)
        , PeriodicWatcher_(EventLoop)
    {
        PeriodicWatcher_.set<TImpl, &TImpl::OnTimer>(this);
        PeriodicWatcher_.start(0, TimeQuantum.SecondsFloat());

        Start();
    }

    TDelayedExecutorCookie Submit(TClosure callback, TDuration delay)
    {
        return Submit(std::move(callback), delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TClosure callback, TInstant deadline)
    {
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline);
        if (IsRunning()) {
            SubmitQueue_.Enqueue(std::move(entry));
        }
        if (!IsRunning()) {
            PurgeQueues();
        }
        return entry;
    }

    void Cancel(TDelayedExecutorCookie entry)
    {
        if (entry && IsRunning()) {
            CancelQueue_.Enqueue(std::move(entry));
        }
        if (!IsRunning()) {
            PurgeQueues();
        }
    }

    void CancelAndClear(TDelayedExecutorCookie& entry)
    {
        Cancel(entry);
        entry.Reset();
    }

private:
    ev::periodic PeriodicWatcher_;

    //! Only touched from the dedicated thread.
    std::set<TDelayedExecutorCookie, TDelayedExecutorEntry::TComparer> ScheduledEntries_;

    //! Enqueued from any thread, dequeued from the dedicated thread.
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> SubmitQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TDelayedExecutorEntryPtr> CancelQueue_;


    virtual void OnShutdown() override
    {
        TEVSchedulerThread::OnShutdown();
        PurgeQueues();
    }

    void OnTimer(ev::periodic&, int)
    {
        TDelayedExecutorEntryPtr entry;

        while (SubmitQueue_.Dequeue(&entry)) {
            if (entry->Canceled)
                continue;
            auto pair = ScheduledEntries_.insert(entry);
            YCHECK(pair.second);
            entry->Iterator = pair.first;
        }

        while (CancelQueue_.Dequeue(&entry)) {
            if (entry->Canceled)
                continue;
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
            if (entry->Canceled)
                continue;
            if (entry->Deadline > TInstant::Now())
                break;
            entry->Callback.Run();
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

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure callback, TDuration delay)
{
    return RefCountedSingleton<TImpl>()->Submit(std::move(callback), delay);
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure callback, TInstant deadline)
{
    return RefCountedSingleton<TImpl>()->Submit(std::move(callback), deadline);
}

void TDelayedExecutor::Cancel(TDelayedExecutorCookie entry)
{
    RefCountedSingleton<TImpl>()->Cancel(std::move(entry));
}

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& entry)
{
    RefCountedSingleton<TImpl>()->CancelAndClear(entry);
}

void TDelayedExecutor::Shutdown()
{
    RefCountedSingleton<TImpl>()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#include "stdafx.h"
#include "delayed_executor.h"
#include "ev_scheduler_thread.h"

#include <core/misc/singleton.h>
#include <core/misc/lock_free.h>
#include <core/misc/nullable.h>

#include <util/datetime/base.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto TimeQuantum = TDuration::MilliSeconds(1);

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

class TDelayedExecutorThread
    : public TEVSchedulerThread
{
public:
    TDelayedExecutorThread()
        : TEVSchedulerThread(
            "DelayedExecutor",
            false)
        , PeriodicWatcher_(EventLoop)
    {
        PeriodicWatcher_.set<TDelayedExecutorThread, &TDelayedExecutorThread::OnTimer>(this);
        PeriodicWatcher_.start(0, TimeQuantum.SecondsFloat());

        Start();
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

        auto now = TInstant::Now();
        while (!ScheduledEntries_.empty()) {
            auto it = ScheduledEntries_.begin();
            const auto& entry = *it;
            if (entry->Canceled)
                continue;
            if (entry->Deadline > now)
                break;
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

struct TDelayedExecutor::TImpl
{
    TIntrusivePtr<TDelayedExecutorThread> Thread = New<TDelayedExecutorThread>();
};

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TDelayedExecutor()
    : Impl_(std::make_unique<TDelayedExecutor::TImpl>())
{ }

TDelayedExecutor::~TDelayedExecutor()
{ }

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TImpl* TDelayedExecutor::GetImpl()
{
    return TSingletonWithFlag<TDelayedExecutor>::Get()->Impl_.get();
}

TFuture<void> TDelayedExecutor::MakeDelayed(TDuration delay)
{
    auto impl = GetImpl();
    if (impl) {
        return impl->Thread->MakeDelayed(delay);
    } else {
        return MakeFuture(TError("System was shut down"));
    }
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure callback, TDuration delay)
{
    auto impl = GetImpl();
    if (impl) {
        return impl->Thread->Submit(std::move(callback), delay);
    } else {
        return NullDelayedExecutorCookie;
    }
}

TDelayedExecutorCookie TDelayedExecutor::Submit(TClosure callback, TInstant deadline)
{
    auto impl = GetImpl();
    if (impl) {
        return impl->Thread->Submit(std::move(callback), deadline);
    } else {
        return NullDelayedExecutorCookie;
    }
}

void TDelayedExecutor::Cancel(TDelayedExecutorCookie entry)
{
    auto impl = GetImpl();
    if (impl) {
        impl->Thread->Cancel(std::move(entry));
    }
}

void TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& entry)
{
    auto impl = GetImpl();
    if (impl) {
        impl->Thread->Cancel(entry);
    }
    entry.Reset();
}

void TDelayedExecutor::StaticShutdown()
{
    if (TSingletonWithFlag<TDelayedExecutor>::WasCreated()) {
        auto& impl = TSingletonWithFlag<TDelayedExecutor>::Get()->Impl_;
        if (impl) {
            impl->Thread->Shutdown();
            impl.reset();
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#include "stdafx.h"
#include "delayed_executor.h"
#include "action_queue_detail.h"
#include "fork_aware_spinlock.h"

#include <core/misc/singleton.h>

#include <util/datetime/base.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static auto DeadlinePrecision = TDuration::MilliSeconds(1);

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

    TInstant Deadline;
    TClosure Callback; // if null then the entry is invalidated
    std::set<TDelayedExecutorCookie, TComparer>::iterator Iterator;

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
        , TimerWatcher_(EventLoop)
    {
        TimerWatcher_.set<TImpl, &TImpl::OnTimer>(this);

        Start();
    }

    TDelayedExecutorCookie Submit(TClosure callback, TDuration delay)
    {
        return Submit(std::move(callback), delay.ToDeadLine());
    }

    TDelayedExecutorCookie Submit(TClosure callback, TInstant deadline)
    {
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline);

        {
            TGuard<TForkAwareSpinLock> guard(SpinLock_);
            auto pair = Entries_.insert(entry);
            YCHECK(pair.second);    
            entry->Iterator = pair.first;
            if (*Entries_.begin() == entry) {
                StartWatcher(entry);
            }
        }

        if (!IsRunning()) {
            PurgeEntries();
        }

        return entry;
    }

    bool Cancel(TDelayedExecutorCookie entry)
    {
        TClosure callback;
        {
            TGuard<TForkAwareSpinLock> guard(SpinLock_);
            if (!entry || !entry->Callback) {
                return false;
            }
            Entries_.erase(entry->Iterator);
            callback = std::move(entry->Callback); // prevent destruction under spin lock
        }
        // |callback| dies here
        return true;
    }

    bool CancelAndClear(TDelayedExecutorCookie& entry)
    {
        if (!entry) {
            return false;
        }
        bool result = Cancel(entry);
        entry.Reset();
        return result;
    }

private:
    ev::timer TimerWatcher_;

    TForkAwareSpinLock SpinLock_;
    std::set<TDelayedExecutorCookie, TDelayedExecutorEntry::TComparer> Entries_;


    virtual void OnShutdown() override
    {
        TEVSchedulerThread::OnShutdown();
        PurgeEntries();
    }

    void StartWatcher(TDelayedExecutorCookie entry)
    {
        GetInvoker()->Invoke(BIND(
            &TImpl::DoStartWatcher,
            MakeStrong(this),
            entry->Deadline));
    }

    void DoStartWatcher(TInstant deadline)
    {
        double delay = std::max(deadline.SecondsFloat() - TInstant::Now().SecondsFloat(), 0.0);
        TimerWatcher_.start(delay + ev_now(EventLoop) - ev_time(), 0.0);
    }

    void OnTimer(ev::timer&, int)
    {
        while (true) {
            TClosure callback;
            {
                TGuard<TForkAwareSpinLock> guard(SpinLock_);
                if (Entries_.empty()) {
                    break;
                }
                auto entry = *Entries_.begin();
                if (entry->Deadline > TInstant::Now() + DeadlinePrecision) {
                    StartWatcher(entry);
                    break;
                }
                Entries_.erase(entry->Iterator);
                
                callback = std::move(entry->Callback); // prevent destruction under spin lock
            }
            if (callback) {
                callback.Run();
            }
        }
    }

    void PurgeEntries()
    {
        std::vector<TClosure> callbacks;
        {
            TGuard<TForkAwareSpinLock> guard(SpinLock_);
            for (auto& entry : Entries_) {
                callbacks.push_back(std::move(entry->Callback)); // prevent destruction under spin lock
            }            
        }
        // |callbacks| die here
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

bool TDelayedExecutor::Cancel(TDelayedExecutorCookie entry)
{
    return RefCountedSingleton<TImpl>()->Cancel(std::move(entry));
}

bool TDelayedExecutor::CancelAndClear(TDelayedExecutorCookie& entry)
{
    return RefCountedSingleton<TImpl>()->CancelAndClear(entry);
}

void TDelayedExecutor::Shutdown()
{
    RefCountedSingleton<TImpl>()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

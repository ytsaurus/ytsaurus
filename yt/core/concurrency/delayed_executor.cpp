#include "stdafx.h"
#include "delayed_executor.h"
#include "action_queue_detail.h"

#include <core/misc/singleton.h>

#include <util/datetime/base.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static auto DeadlinePrecision = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

struct TDelayedExecutorEntry
    : public TIntrinsicRefCounted
{
    typedef TDelayedExecutor::TCookie TCookie;

    struct TComparer
    {
        bool operator()(const TCookie& lhs, const TCookie& rhs) const
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
    std::set<TCookie, TComparer>::iterator Iterator;

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
        , TimerWatcher(EventLoop)
    {
        TimerWatcher.set<TImpl, &TImpl::OnTimer>(this);

        Start();
    }

    TCookie Submit(TClosure callback, TDuration delay)
    {
        return Submit(std::move(callback), delay.ToDeadLine());
    }

    TCookie Submit(TClosure callback, TInstant deadline)
    {
        auto entry = New<TDelayedExecutorEntry>(std::move(callback), deadline);

        {
            TGuard<TSpinLock> guard(SpinLock);
            auto pair = Entries.insert(entry);
            YCHECK(pair.second);    
            entry->Iterator = pair.first;
            if (*Entries.begin() == entry) {
                StartWatcher(entry);
            }
        }

        if (!IsRunning()) {
            PurgeEntries();
        }

        return entry;
    }

    bool Cancel(TCookie entry)
    {
        TClosure callback;
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (!entry || !entry->Callback) {
                return false;
            }
            Entries.erase(entry->Iterator);
            callback = std::move(entry->Callback); // prevent destruction under spin lock
        }
        // |callback| dies here
        return true;
    }

    bool CancelAndClear(TCookie& entry)
    {
        if (!entry) {
            return false;
        }
        bool result = Cancel(entry);
        entry.Reset();
        return result;
    }

private:
    ev::timer TimerWatcher;

    TSpinLock SpinLock;
    std::set<TCookie, TDelayedExecutorEntry::TComparer> Entries;


    virtual void OnShutdown() override
    {
        TEVSchedulerThread::OnShutdown();
        PurgeEntries();
    }

    void StartWatcher(TCookie entry)
    {
        GetInvoker()->Invoke(BIND(
            &TImpl::DoStartWatcher,
            MakeStrong(this),
            entry->Deadline));
    }

    void DoStartWatcher(TInstant deadline)
    {
        double delay = std::max(deadline.SecondsFloat() - TInstant::Now().SecondsFloat(), 0.0);
        TimerWatcher.start(delay + ev_now(EventLoop) - ev_time(), 0.0);        
    }

    void OnTimer(ev::timer&, int)
    {
        while (true) {
            TClosure callback;
            {
                TGuard<TSpinLock> guard(SpinLock);
                if (Entries.empty()) {
                    break;
                }
                auto entry = *Entries.begin();
                if (entry->Deadline > TInstant::Now() + DeadlinePrecision) {
                    StartWatcher(entry);
                    break;
                }
                Entries.erase(entry->Iterator);
                
                callback = std::move(entry->Callback); // prevent destruction under spin lock
            }
            callback.Run();
        }
    }

    void PurgeEntries()
    {
        std::vector<TClosure> callbacks;
        {
            TGuard<TSpinLock> guard(SpinLock);
            for (auto& entry : Entries) {
                callbacks.push_back(std::move(entry->Callback)); // prevent destruction under spin lock
            }            
        }
        // |callbacks| die here
    }

};

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TCookie TDelayedExecutor::Submit(TClosure callback, TDuration delay)
{
    return RefCountedSingleton<TImpl>()->Submit(std::move(callback), delay);
}

TDelayedExecutor::TCookie TDelayedExecutor::Submit(TClosure callback, TInstant deadline)
{
    return RefCountedSingleton<TImpl>()->Submit(std::move(callback), deadline);
}

bool TDelayedExecutor::Cancel(TCookie entry)
{
    return RefCountedSingleton<TImpl>()->Cancel(std::move(entry));
}

bool TDelayedExecutor::CancelAndClear(TCookie& entry)
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

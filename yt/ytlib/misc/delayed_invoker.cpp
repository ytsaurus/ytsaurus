#include "stdafx.h"
#include "common.h"
#include "delayed_invoker.h"

#include <ytlib/logging/log.h>
#include <ytlib/actions/action_queue.h>

#include <util/generic/set.h>
#include <util/system/thread.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("DelayedInvoker");
static const TDuration SleepQuantum = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

class TDelayedInvoker::TImpl
    : private TNonCopyable
{
public:
    TImpl()
        : Thread(&ThreadFunc, (void*)this)
        , Finished(false)
    {
        Thread.Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    TCookie Submit(IAction::TPtr action, TDuration delay)
    {
        return Submit(action, delay.ToDeadLine());
    }

    TCookie Submit(IAction::TPtr action, TInstant deadline)
    {
        auto entry = New<TEntry>(action, deadline);

        LOG_TRACE("Submitted delayed action (Action: %p, Cookie: %p, Deadline: %s)",
            ~action,
            ~entry,
            ~entry->Deadline.ToString());

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (!Finished) {
                Entries.insert(entry);
            }
        }

        return entry;
    }

    bool Cancel(TCookie cookie)
    {
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (Entries.erase(cookie) == 0) {
                return false;
            }
        }

        LOG_TRACE("Canceled delayed action (Cookie: %p)", ~cookie);
        return true;
    }

    bool CancelAndClear(TCookie& cookie)
    {
        if (!cookie)
            return false;
        auto cookie_ = cookie;
        cookie.Reset();
        return Cancel(cookie_);
    }

    void Shutdown()
    {
        Finished = true;
        Thread.Join();
    }

private:
    struct TEntryLess
    {
        bool operator()(TEntry::TPtr lhs, TEntry::TPtr rhs) const
        {
            return
                lhs->Deadline < rhs->Deadline ||
                lhs->Deadline == rhs->Deadline &&
                lhs->Action < rhs->Action;
        }
    };

    yset<TEntry::TPtr, TEntryLess> Entries;
    TThread Thread;
    TSpinLock SpinLock;
    volatile bool Finished;


    static void* ThreadFunc(void* param)
    {
        auto* impl = reinterpret_cast<TImpl*>(param);
        impl->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        while (!Finished) {
            while (true) {
                TEntry::TPtr entry;
                {
                    TInstant now = TInstant::Now();
                    TGuard<TSpinLock> guard(SpinLock);
                    if (Entries.empty()) {
                        break;
                    }
                    entry = *Entries.begin();
                    if (entry->Deadline > now) {
                        break;
                    }
                    Entries.erase(Entries.begin());
                }

                LOG_TRACE("Running task %p", ~entry);
                entry->Action->Do();
            }
            Sleep(SleepQuantum);
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

TDelayedInvoker::TCookie TDelayedInvoker::Submit(IAction::TPtr action, TDuration delay)
{
    return Singleton<TDelayedInvoker::TImpl>()->Submit(action, delay);
}

TDelayedInvoker::TCookie TDelayedInvoker::Submit(IAction::TPtr action, TInstant deadline)
{
    return Singleton<TDelayedInvoker::TImpl>()->Submit(action, deadline);
}

bool TDelayedInvoker::Cancel(TCookie cookie)
{
    return Singleton<TDelayedInvoker::TImpl>()->Cancel(cookie);
}

bool TDelayedInvoker::CancelAndClear(TCookie& cookie)
{
    return Singleton<TDelayedInvoker::TImpl>()->CancelAndClear(cookie);
}

void TDelayedInvoker::Shutdown()
{
    Singleton<TDelayedInvoker::TImpl>()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

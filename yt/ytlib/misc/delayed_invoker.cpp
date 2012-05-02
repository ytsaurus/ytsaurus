#include "stdafx.h"
#include "common.h"
#include "delayed_invoker.h"
#include "thread.h"

#include <ytlib/logging/log.h>
#include <ytlib/actions/action_queue.h>

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

    TCookie Submit(TClosure action, TDuration delay)
    {
        return Submit(action, delay.ToDeadLine());
    }

    TCookie Submit(TClosure action, TInstant deadline)
    {
        auto cookie = New<TEntry>(action, deadline);

        TGuard<TSpinLock> guard(SpinLock);
        cookie->Iterator = Entries.insert(MakePair(cookie->Deadline, cookie));

        LOG_TRACE("Submitted delayed action (Action: %p, Cookie: %p, Deadline: %s, Count: %d)",
            action.GetHandle(),
            ~cookie,
            ~cookie->Deadline.ToString(),
            static_cast<int>(Entries.size()));

        return cookie;
    }

    bool Cancel(TCookie cookie)
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (!cookie->Valid) {
            return false;
        }

        Entries.erase(cookie->Iterator);
        cookie->Valid = false;

        LOG_TRACE("Canceled delayed action (Cookie: %p, Count: %d)",
            ~cookie,
            static_cast<int>(Entries.size()));

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
    TEntries Entries;
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
        NThread::SetCurrentThreadName("DelayedInvoker");
        while (!Finished) {
            auto now = TInstant::Now();
            LOG_TRACE("Iteration started at %s", ~ToString(now));
            while (true) {
                TCookie cookie;
                {
                    TGuard<TSpinLock> guard(SpinLock);
                    if (Entries.empty()) {
                        LOG_TRACE("Nothing to execute");
                        break;
                    }
                    cookie = Entries.begin()->second;
                    if (cookie->Deadline > now) {
                        LOG_TRACE("Deadline is not reached yet (NextCookie: %p, NextDeadline: %s, Count: %d)",
                            ~cookie,
                            ~ToString(cookie->Deadline),
                            static_cast<int>(Entries.size()));
                        break;
                    }
                    Entries.erase(Entries.begin());
                    cookie->Valid = false;
                }

                LOG_TRACE("Action started (Cookie: %p)", ~cookie);
                cookie->Action.Run();
                LOG_TRACE("Action completed (Cookie: %p)", ~cookie);
            }
            Sleep(SleepQuantum);
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

TDelayedInvoker::TCookie TDelayedInvoker::Submit(TClosure action, TDuration delay)
{
    return Singleton<TImpl>()->Submit(action, delay);
}

TDelayedInvoker::TCookie TDelayedInvoker::Submit(TClosure action, TInstant deadline)
{
    return Singleton<TImpl>()->Submit(action, deadline);
}

bool TDelayedInvoker::Cancel(TCookie cookie)
{
    return Singleton<TImpl>()->Cancel(cookie);
}

bool TDelayedInvoker::CancelAndClear(TCookie& cookie)
{
    return Singleton<TImpl>()->CancelAndClear(cookie);
}

void TDelayedInvoker::Shutdown()
{
    Singleton<TImpl>()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

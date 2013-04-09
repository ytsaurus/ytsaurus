#include "stdafx.h"
#include "common.h"
#include "delayed_invoker.h"
#include "thread.h"

#include <ytlib/actions/action_queue.h>

#include <util/system/thread.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const TDuration SleepQuantum = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

bool TDelayedInvoker::TEntryComparer::operator()(const TEntryPtr& lhs, const TEntryPtr& rhs) const
{
    if (lhs->Deadline != rhs->Deadline) {
        return lhs->Deadline < rhs->Deadline;
    }
    // Break ties.
    return lhs < rhs;
}

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

        {
            TGuard<TSpinLock> guard(SpinLock);
            auto pair = Entries.insert(cookie);
            YCHECK(pair.second);
            cookie->Iterator = pair.first;
        }

        return cookie;
    }

    bool Cancel(TCookie cookie)
    {
        {
            TGuard<TSpinLock> guard(SpinLock);

            if (!cookie) {
                return false;
            }

            if (!cookie->Valid) {
                return false;
            }

            Entries.erase(cookie->Iterator);
            cookie->Valid = false;
        }

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
    std::set<TEntryPtr, TEntryComparer> Entries;
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
            while (true) {
                TCookie cookie;
                {
                    TGuard<TSpinLock> guard(SpinLock);
                    if (Entries.empty()) {
                        break;
                    }
                    cookie = *Entries.begin();
                    if (cookie->Deadline > now) {
                        break;
                    }
                    Entries.erase(cookie->Iterator);
                    cookie->Valid = false;
                }
                cookie->Action.Run();
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

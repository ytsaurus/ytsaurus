#include "stdafx.h"
#include "delayed_invoker.h"

#include <ytlib/concurrency/action_queue.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

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
        auto entry = CookieToEntry(cookie);

        {
            TGuard<TSpinLock> guard(SpinLock);

            if (!entry) {
                return false;
            }

            if (!entry->Valid) {
                return false;
            }

            Entries.erase(entry->Iterator);
            entry->Valid = false;
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
    struct TEntry;
    typedef TIntrusivePtr<TEntry> TEntryPtr;

    struct TEntry
        : public TEntryBase
    {
        struct TComparer
        {
            bool operator()(const TEntryPtr& lhs, const TEntryPtr& rhs) const
            {
                if (lhs->Deadline != rhs->Deadline) {
                    return lhs->Deadline < rhs->Deadline;
                }
                // Break ties.
                return lhs < rhs;
            }
        };

        bool Valid;
        TInstant Deadline;
        TClosure Action;
        std::set<TEntryPtr, TComparer>::iterator Iterator;

        TEntry(TClosure action, TInstant deadline)
            : Valid(true)
            , Deadline(deadline)
            , Action(std::move(action))
        { }
    };

    std::set<TEntryPtr, TEntry::TComparer> Entries;
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
                TEntryPtr entry;
                {
                    TGuard<TSpinLock> guard(SpinLock);
                    if (Entries.empty()) {
                        break;
                    }
                    entry = *Entries.begin();
                    if (entry->Deadline > now) {
                        break;
                    }
                    Entries.erase(entry->Iterator);
                    entry->Valid = false;
                }
                entry->Action.Run();
            }
            Sleep(SleepQuantum);
        }
    }

    static TEntryPtr CookieToEntry(TCookie cookie)
    {
        return static_cast<TEntry*>(~cookie);
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

} // namespace NConcurrency
} // namespace NYT

#include "stdafx.h"
#include "delayed_executor.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/thread.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const TDuration SleepQuantum = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

class TDelayedExecutor::TImpl
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

        if (AtomicGet(Finished)) {
            PurgeEntries();
        }

        return cookie;
    }

    bool Cancel(TCookie cookie)
    {
        auto entry = CookieToEntry(cookie);

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (!entry || !entry->Action) {
                return false;
            }
            Entries.erase(entry->Iterator);
            entry->Action.Reset();
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
        AtomicSet(Finished, true);
        Thread.Join();
        PurgeEntries();
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

        TInstant Deadline;
        TClosure Action; // if null then the entry is invalidated
        std::set<TEntryPtr, TComparer>::iterator Iterator;

        TEntry(TClosure action, TInstant deadline)
            : Deadline(deadline)
            , Action(std::move(action))
        { }
    };

    std::set<TEntryPtr, TEntry::TComparer> Entries;
    TThread Thread;
    TSpinLock SpinLock;
    TAtomic Finished;


    static void* ThreadFunc(void* param)
    {
        auto* impl = reinterpret_cast<TImpl*>(param);
        impl->ThreadMain();
        return nullptr;
    }

    void ThreadMain()
    {
        SetCurrentThreadName("DelayedInvoker");
        while (!AtomicGet(Finished)) {
            auto now = TInstant::Now();
            while (true) {
                TClosure action;
                {
                    TGuard<TSpinLock> guard(SpinLock);
                    if (Entries.empty()) {
                        break;
                    }
                    auto entry = *Entries.begin();
                    if (entry->Deadline > now) {
                        break;
                    }
                    Entries.erase(entry->Iterator);
                    
                    action = std::move(entry->Action);
                    entry->Action.Reset(); // typically redundant
                }
                action.Run();
            }
            Sleep(SleepQuantum);
        }
    }

    static TEntryPtr CookieToEntry(TCookie cookie)
    {
        return static_cast<TEntry*>(~cookie);
    }

    void PurgeEntries()
    {
        std::vector<TClosure> actions;
        {
            TGuard<TSpinLock> guard(SpinLock);
            for (const auto& entry : Entries) {
                actions.push_back(std::move(entry->Action)); // prevent destruction under spin lock
                entry->Action.Reset(); // typically redundant
            }            
        }
        // |actions| die here
    }
};

///////////////////////////////////////////////////////////////////////////////

TDelayedExecutor::TCookie TDelayedExecutor::Submit(TClosure action, TDuration delay)
{
    return Singleton<TImpl>()->Submit(action, delay);
}

TDelayedExecutor::TCookie TDelayedExecutor::Submit(TClosure action, TInstant deadline)
{
    return Singleton<TImpl>()->Submit(action, deadline);
}

bool TDelayedExecutor::Cancel(TCookie cookie)
{
    return Singleton<TImpl>()->Cancel(cookie);
}

bool TDelayedExecutor::CancelAndClear(TCookie& cookie)
{
    return Singleton<TImpl>()->CancelAndClear(cookie);
}

void TDelayedExecutor::Shutdown()
{
    Singleton<TImpl>()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

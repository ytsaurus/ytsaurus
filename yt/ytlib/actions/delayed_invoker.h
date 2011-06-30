#pragma once

#include "invoker.h"
#include "action_queue.h"

#include <util/system/thread.h>
#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/system/spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: move to misc
class TDelayedInvoker
    : private TNonCopyable
{
    struct TEntry
        : public TRefCountedBase
    {
        typedef TIntrusivePtr<TEntry> TPtr;

        TInstant Deadline;
        IAction::TPtr Action;

        TEntry(IAction::TPtr action, TInstant deadline)
            : Deadline(deadline)
            , Action(action)
        { }
    };

    struct TEntryLess
    {
        bool operator()(const TEntry::TPtr& left, const TEntry::TPtr& right) const
        {
            return left->Deadline < right->Deadline ||
                   left->Deadline == right->Deadline && 
                   ~left->Action < ~right->Action;
        }
    };

    yset<TEntry::TPtr, TEntryLess> Entries;
    TThread Thread;
    TSpinLock SpinLock;
    volatile bool Finished;

public:
    typedef TEntry::TPtr TCookie;

    TDelayedInvoker();
    ~TDelayedInvoker();

    static TDelayedInvoker* Get();

    TCookie Submit(IAction::TPtr action, TDuration delay);
    bool Cancel(TCookie cookie);

private:
    static void* ThreadFunc(void* param);
    void ThreadMain();

};

////////////////////////////////////////////////////////////////////////////////

}

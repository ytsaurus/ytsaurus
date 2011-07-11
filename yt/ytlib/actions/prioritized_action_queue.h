#pragma once

#include "action.h"

#include "../misc/common.h"

#include <util/autoarray.h>

#include <util/generic/ptr.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedActionQueue
{
public:
    typedef TIntrusivePtr<TPrioritizedActionQueue> TPtr;

    TPrioritizedActionQueue(i32 priorityCount);

    ~TPrioritizedActionQueue();
    
    IInvoker* GetInvoker(i32 priority);
    void Shutdown();

protected:
    virtual void OnIdle();
    bool IsEmpty();

private:
    i32 PriorityCount;

    typedef TLockFreeQueue<IAction::TPtr>  TQueue;
    autoarray< THolder<TQueue> > Queues;

    class TQueueInvoker;
    autoarray < THolder<TQueueInvoker> > Invokers;

    TThread Thread;
    volatile bool Finished;

    static void* ThreadFunc(void* param);
    void ThreadMain();
    Event WakeupEvent;
};

////////////////////////////////////////////////////////////////////////////////

}


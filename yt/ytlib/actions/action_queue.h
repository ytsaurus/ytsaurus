#pragma once

#include "action.h"

#include "../misc/common.h"

#include <util/generic/ptr.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TQueueInvoker
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TQueueInvoker> TPtr;

    typedef TLockFreeQueue<IAction::TPtr> TQueue;

    TQueueInvoker(Event* wakeupEvent, bool enableLogging = true);

    TQueue* GetQueue() const;
    void Invoke(IAction::TPtr action);
    void Shutdown();

private:
    THolder<TQueue> Queue;
    Event* WakeupEvent;
    bool EnableLogging;
    volatile bool Finished;
};

class TActionQueue
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TActionQueue> TPtr;

    // TActionQueue is used internally by the logging infrastructure,
    // which passes enableLogging = false to prevent infinite recursion.
    TActionQueue(bool enableLogging = true);

    virtual ~TActionQueue();

    IInvoker::TPtr GetInvoker();
    void Shutdown();

protected:
    virtual void OnIdle();
    bool IsEmpty() const;

private:
    bool EnableLogging;
    volatile bool Finished;
    TThread Thread;
    Event WakeupEvent;
    
    TQueueInvoker::TPtr QueueInvoker; // Don't move it above - we need it to be initialized after Thread and WakeupEvent

    static void* ThreadFunc(void* param);
    void ThreadMain();
};

class TPrioritizedActionQueue
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TPrioritizedActionQueue> TPtr;

    TPrioritizedActionQueue(i32 priorityCount);

    ~TPrioritizedActionQueue();

    IInvoker::TPtr GetInvoker(i32 priority);
    void Shutdown();

protected:
    virtual void OnIdle();
    bool IsEmpty();

private:
    i32 PriorityCount;

    autoarray<TQueueInvoker::TPtr> QueueInvokers;

    TThread Thread;
    volatile bool Finished;

    static void* ThreadFunc(void* param);
    void ThreadMain();
    Event WakeupEvent;
};

////////////////////////////////////////////////////////////////////////////////

}


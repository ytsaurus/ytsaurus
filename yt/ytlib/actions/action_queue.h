#pragma once

#include "common.h"
#include "action.h"

#include <ytlib/misc/common.h>

#include <util/generic/ptr.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TActionQueueBase;

class TQueueInvoker
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TQueueInvoker> TPtr;

    TQueueInvoker(TActionQueueBase* owner, bool enableLogging);

    void Invoke(IAction::TPtr action);
    void OnShutdown();
    bool OnDequeueAndExecute();

private:
    typedef TPair<IAction::TPtr, ui64> TItem;

    bool EnableLogging;
    TActionQueueBase* Owner;
    TLockFreeQueue<TItem> Queue;
    TAtomic QueueSize;
};

////////////////////////////////////////////////////////////////////////////////

class TActionQueueBase
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TActionQueueBase> TPtr;

    virtual ~TActionQueueBase();

    void Shutdown();

protected:
    TActionQueueBase(const Stroka& threadName, bool enableLogging);

    void Start();

    virtual bool DequeueAndExecute() = 0;
    virtual void OnIdle() = 0;

private:
    friend class TQueueInvoker;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    bool EnableLogging;
    volatile bool Finished;
    Event WakeupEvent;
    TThread Thread;
    Stroka ThreadName;

};

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public TActionQueueBase
{
public:
    typedef TIntrusivePtr<TActionQueue> TPtr;

    // TActionQueue is used internally by the logging infrastructure,
    // which passes enableLogging = false to prevent infinite recursion.
    TActionQueue(const Stroka& threadName = "<ActionQueue>", bool enableLogging = true);
    virtual ~TActionQueue();

    IInvoker::TPtr GetInvoker();

    static IFunc<TPtr>::TPtr CreateFactory(const Stroka& threadName);
    
protected:
    virtual bool DequeueAndExecute();
    virtual void OnIdle();

private:
    TIntrusivePtr<TQueueInvoker> QueueInvoker;

};

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedActionQueue
    : public TActionQueueBase
{
public:
    typedef TIntrusivePtr<TPrioritizedActionQueue> TPtr;

    TPrioritizedActionQueue(int priorityCount, const Stroka& threadName = "<PrActionQueue>");
    virtual ~TPrioritizedActionQueue();

    IInvoker::TPtr GetInvoker(int priority);

protected:
    virtual bool DequeueAndExecute();
    virtual void OnIdle();

private:
    autoarray< TIntrusivePtr<TQueueInvoker> > QueueInvokers;

};

////////////////////////////////////////////////////////////////////////////////

}


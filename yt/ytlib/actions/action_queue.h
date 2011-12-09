#pragma once

#include "common.h"
#include "action.h"

#include "../misc/common.h"

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

    TQueueInvoker(TActionQueueBase* owner);

    void Invoke(IAction::TPtr action);
    void Shutdown();
    bool DequeueAndExecute();

private:
    TLockFreeQueue<IAction::TPtr> Queue;
    TIntrusivePtr<TActionQueueBase> Owner;

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
    TActionQueueBase(const char* threadName, bool enableLogging);

    void Start();

    virtual bool DequeueAndExecute() = 0;
    virtual void OnIdle() = 0;
    virtual void OnShutdown() = 0;

private:
    friend class TQueueInvoker;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    bool EnableLogging;
    volatile bool Finished;
    Event WakeupEvent;
    TThread Thread;
    const char* ThreadName;

};

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public TActionQueueBase
{
public:
    typedef TIntrusivePtr<TActionQueue> TPtr;

    // TActionQueue is used internally by the logging infrastructure,
    // which passes enableLogging = false to prevent infinite recursion.
    TActionQueue(const char* threadName = "<ActionQueue>", bool enableLogging = true);

    IInvoker::TPtr GetInvoker();

protected:
    virtual bool DequeueAndExecute();
    virtual void OnIdle();
    virtual void OnShutdown();

private:
    TIntrusivePtr<TQueueInvoker> QueueInvoker;

};

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedActionQueue
    : public TActionQueueBase
{
public:
    typedef TIntrusivePtr<TPrioritizedActionQueue> TPtr;

    TPrioritizedActionQueue(int priorityCount, const char* threadName = "<PrActionQueue>");

    ~TPrioritizedActionQueue();

    IInvoker::TPtr GetInvoker(int priority);

protected:
    virtual bool DequeueAndExecute();
    virtual void OnIdle();
    virtual void OnShutdown();

private:
    autoarray< TIntrusivePtr<TQueueInvoker> > QueueInvokers;

};

////////////////////////////////////////////////////////////////////////////////

}


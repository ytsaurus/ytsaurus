#pragma once

#include "action.h"

#include "../misc/common.h"

#include <util/generic/ptr.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TQueueInvoker;

class TActionQueueBase
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TActionQueueBase> TPtr;

protected:
    friend class TQueueInvoker;

    TActionQueueBase(bool enableLogging);

    bool EnableLogging;
    volatile bool IsFinished;
    Event WakeupEvent;

};

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public TActionQueueBase
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
    TThread Thread;
    
    // Don't move it above: we need it to be initialized after #Thread and #WakeupEvent.
    TIntrusivePtr<TQueueInvoker> QueueInvoker;

    static void* ThreadFunc(void* param);
    void ThreadMain();

};

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedActionQueue
    : public TActionQueueBase
{
public:
    typedef TIntrusivePtr<TPrioritizedActionQueue> TPtr;

    TPrioritizedActionQueue(int priorityCount);

    ~TPrioritizedActionQueue();

    IInvoker::TPtr GetInvoker(int priority);
    void Shutdown();

protected:
    virtual void OnIdle();

private:
    autoarray< TIntrusivePtr<TQueueInvoker> > QueueInvokers;

    TThread Thread;

    static void* ThreadFunc(void* param);
    void ThreadMain();
};

////////////////////////////////////////////////////////////////////////////////

}


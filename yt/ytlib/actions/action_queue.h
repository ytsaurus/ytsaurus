#pragma once

#include "common.h"
#include "action.h"

#include <ytlib/profiling/profiler.h>

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
    TQueueInvoker(TActionQueueBase* owner, bool enableLogging);

    void Invoke(IAction::TPtr action);
    bool IsEmpty() const;
    void Shutdown();
    bool DequeueAndExecute();

private:
    struct TItem
    {
        NProfiling::TTimer Timer;
        IAction::TPtr Action;
    };

    TActionQueueBase* Owner;
    bool EnableLogging;
    NProfiling::TProfiler Profiler;

    TLockFreeQueue<TItem> Queue;
    TAtomic QueueSize;
};

typedef TIntrusivePtr<TQueueInvoker> TQueueInvokerPtr;

////////////////////////////////////////////////////////////////////////////////

class TActionQueueBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TActionQueueBase> TPtr;

    virtual ~TActionQueueBase();

protected:
    TActionQueueBase(const Stroka& threadName, bool enableLogging);

    void Start();
    void Shutdown();
    void Signal();

    virtual bool DequeueAndExecute() = 0;
    virtual void OnIdle();

    bool IsRunning() const;

private:
    friend class TQueueInvoker;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    bool EnableLogging;
    volatile bool Running;
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

    void Shutdown();

    IInvoker* GetInvoker();

    static IFunc<TPtr>::TPtr CreateFactory(const Stroka& threadName);
    
protected:
    virtual bool DequeueAndExecute();

private:
    TQueueInvokerPtr QueueInvoker;

};

////////////////////////////////////////////////////////////////////////////////

class TMultiActionQueue
    : public TRefCounted
{
public:
    TMultiActionQueue(int queueCount, const Stroka& threadName);

    void Shutdown();

    IInvoker* GetInvoker(int queueIndex);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

typedef TIntrusivePtr<TMultiActionQueue> TMultiActionQueuePtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


#pragma once

#include "common.h"
#include "invoker.h"
#include "callback.h"

#include <ytlib/profiling/profiler.h>

#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TActionQueueBase;

// TODO(sandello): Consider moving this into .cpp.
class TQueueInvoker
    : public IInvoker
{
public:
    TQueueInvoker(
        const Stroka& name,
        TActionQueueBase* owner,
        bool enableLogging);

    void Invoke(const TClosure& action);
    bool IsEmpty() const;
    void Shutdown();
    bool DequeueAndExecute();

private:
    struct TItem
    {
        NProfiling::TCpuInstant StartInstant;
        TClosure Action;
    };

    TActionQueueBase* Owner;
    bool EnableLogging;
    NProfiling::TProfiler Profiler;

    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter DequeueCounter;
    TAtomic QueueSize;
    NProfiling::TAggregateCounter QueueSizeCounter;
    NProfiling::TAggregateCounter WaitTimeCounter;
    NProfiling::TAggregateCounter ExecTimeCounter;
    NProfiling::TAggregateCounter TotalTimeCounter;

    TLockFreeQueue<TItem> Queue;
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

    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

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
    // TODO: TActionQueue::TPtr -> TActionQueuePtr, place to public.h
    typedef TIntrusivePtr<TActionQueue> TPtr;

    // TActionQueue is used internally by the logging infrastructure,
    // which passes enableLogging = false to prevent infinite recursion.
    TActionQueue(const Stroka& threadName = "<ActionQueue>", bool enableLogging = true);
    virtual ~TActionQueue();

    void Shutdown();

    IInvokerPtr GetInvoker();

    static TCallback<TPtr()> CreateFactory(const Stroka& threadName);
    
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
    virtual ~TMultiActionQueue();

    void Shutdown();

    IInvokerPtr GetInvoker(int queueIndex);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

typedef TIntrusivePtr<TMultiActionQueue> TMultiActionQueuePtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


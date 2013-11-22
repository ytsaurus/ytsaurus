#pragma once

#include "common.h"
#include "invoker.h"
#include "callback.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/thread.h>

#include <ytlib/profiling/profiler.h>

#include <ytlib/ypath/public.h>

#include <ytlib/misc/thread.h>

#include <util/system/thread.h>
#include <util/system/event.h>

#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TInvokerQueue;
typedef TIntrusivePtr<TInvokerQueue> TInvokerQueuePtr;

class TExecutorThread;
typedef TIntrusivePtr<TExecutorThread> TExecutorThreadPtr;

class TExecutorThreadWithQueue;
typedef TIntrusivePtr<TExecutorThreadWithQueue> TExecutorThreadWithQueuePtr;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EBeginExecuteResult,
    (Success)
    (QueueEmpty)
    (Terminated)
);

class TInvokerQueue
    : public IInvoker
{
public:
    TInvokerQueue(
        TExecutorThread* owner,
        IInvoker* currentInvoker,
        const NYPath::TYPath& profilingPath,
        bool enableLogging);

    bool Invoke(const TClosure& action);
    void Shutdown();

    EBeginExecuteResult BeginExecute();
    void EndExecute();

    int GetSize() const;
    bool IsEmpty() const;

private:
    struct TItem
    {
        NProfiling::TCpuInstant EnqueueInstant;
        NProfiling::TCpuInstant StartInstant;
        TClosure Action;
    };

    TExecutorThread* Owner;
    IInvoker* CurrentInvoker;
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
    TItem CurrentItem;
};


///////////////////////////////////////////////////////////////////////////////

class TExecutorThread
    : public TRefCounted
{
public:
    virtual ~TExecutorThread();

protected:
    TExecutorThread(
        const Stroka& threadName,
        bool enableLogging);

    void Start();
    void Shutdown();
    void Signal();

    virtual EBeginExecuteResult BeginExecute() = 0;
    virtual void EndExecute() = 0;
    
    virtual void OnIdle();

    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

    bool IsRunning() const;

private:
    friend class TInvokerQueue;

    static void* ThreadMain(void* opaque);
    void ThreadMain();
    void FiberMain();

    EBeginExecuteResult CheckedExecute();

    Stroka ThreadName;
    bool EnableLogging;

    NProfiling::TProfiler Profiler;

    volatile bool Running;
    int FibersCreated;
    int FibersAlive;

    NThread::TThreadId ThreadId;

    Event WakeupEvent;

    TThread Thread;
    
};

////////////////////////////////////////////////////////////////////////////////

class TExecutorThreadWithQueue
    : public TExecutorThread
{
public:
    TExecutorThreadWithQueue(
        IInvoker* currentInvoker,
        const Stroka& threadName,
        const Stroka& profilingName,
        bool enableLogging);

    ~TExecutorThreadWithQueue();

    void Shutdown();

    IInvokerPtr GetInvoker();

    int GetSize();

private:
    TInvokerQueuePtr Queue;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


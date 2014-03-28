#pragma once

#include "public.h"
#include "event_count.h"

#include <core/actions/invoker.h>
#include <core/actions/callback.h>

#include <core/profiling/profiler.h>

#include <core/ypath/public.h>

#include <util/system/thread.h>
#include <util/system/event.h>

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TInvokerQueue;
typedef TIntrusivePtr<TInvokerQueue> TInvokerQueuePtr;

class TExecutorThread;
typedef TIntrusivePtr<TExecutorThread> TExecutorThreadPtr;

class TSingleQueueExecutorThread;
typedef TIntrusivePtr<TSingleQueueExecutorThread> TSingleQueueExecutorThreadPtr;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EBeginExecuteResult,
    (Success)
    (QueueEmpty)
    (Terminated)
);

struct TEnqueuedAction
{
    TEnqueuedAction()
        : Finished(true)
    { }

    bool Finished;
    NProfiling::TCpuInstant EnqueueInstant;
    NProfiling::TCpuInstant StartInstant;
    TClosure Callback;
};

class TInvokerQueue
    : public IInvoker
{
public:
    TInvokerQueue(
        TEventCount* eventCount,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    void SetThreadId(TThreadId threadId);

    virtual bool Invoke(const TClosure& callback) override;
    virtual TThreadId GetThreadId() const override;

    void Shutdown();

    EBeginExecuteResult BeginExecute(TEnqueuedAction* action);
    void EndExecute(TEnqueuedAction* action);

    int GetSize() const;
    bool IsEmpty() const;

private:
    TEventCount* EventCount;
    TThreadId ThreadId;
    bool EnableLogging;

    bool Running;

    NProfiling::TProfiler Profiler;

    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter DequeueCounter;
    TAtomic QueueSize;
    NProfiling::TAggregateCounter QueueSizeCounter;
    NProfiling::TAggregateCounter WaitTimeCounter;
    NProfiling::TAggregateCounter ExecTimeCounter;
    NProfiling::TAggregateCounter TotalTimeCounter;

    TLockFreeQueue<TEnqueuedAction> Queue;

};

///////////////////////////////////////////////////////////////////////////////

class TExecutorThread
    : public TRefCounted
{
public:
    virtual ~TExecutorThread();
    
    void Start();
    void Shutdown();

    TThreadId GetId() const;
    bool IsRunning() const;

protected:
    TExecutorThread(
        TEventCount* eventCount,
        const Stroka& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    virtual EBeginExecuteResult BeginExecute() = 0;
    virtual void EndExecute() = 0;
    
    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

private:
    friend class TInvokerQueue;

    static void* ThreadMain(void* opaque);
    void ThreadMain();
    void FiberMain();

    EBeginExecuteResult Execute();

    TEventCount* EventCount;
    Stroka ThreadName;
    bool EnableLogging;

    NProfiling::TProfiler Profiler;

    volatile bool Running;
    TPromise<void> Started;
    int FibersCreated;
    int FibersAlive;

    TThreadId ThreadId;
    TThread Thread;
    
};

////////////////////////////////////////////////////////////////////////////////

class TSingleQueueExecutorThread
    : public TExecutorThread
{
public:
    TSingleQueueExecutorThread(
        TInvokerQueuePtr queue,
        TEventCount* eventCount,
        const Stroka& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    ~TSingleQueueExecutorThread();

    IInvokerPtr GetInvoker();

protected:
    TInvokerQueuePtr Queue;

    TEnqueuedAction CurrentAction;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

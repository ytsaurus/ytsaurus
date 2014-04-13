#pragma once

#include "public.h"
#include "event_count.h"
#include "scheduler.h"
#include "execution_context.h"
#include "thread_affinity.h"

#include <core/actions/invoker.h>
#include <core/actions/callback.h>
#include <core/actions/future.h>

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

class TSchedulerThread;
typedef TIntrusivePtr<TSchedulerThread> TSchedulerThreadPtr;

class TSingleQueueSchedulerThread;
typedef TIntrusivePtr<TSingleQueueSchedulerThread> TSingleQueueSchedulerThreadPtr;

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
    NProfiling::TCpuInstant EnqueuedAt;
    NProfiling::TCpuInstant StartedAt;
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

    virtual void Invoke(const TClosure& callback) override;
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

    std::atomic_bool Running;

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

class TSchedulerThread
    : public TRefCounted
    , public IScheduler
{
public:
    virtual ~TSchedulerThread();

    void Start();
    void Shutdown();

    TThreadId GetId() const;
    bool IsRunning() const;

    virtual TFiber* GetCurrentFiber() override;
    virtual void Return() override;
    virtual void Yield() override;
    virtual void YieldTo(TFiberPtr&& other) override;
    virtual void SwitchTo(IInvokerPtr invoker) override;
    virtual void WaitFor(TFuture<void> future, IInvokerPtr invoker) override;

protected:
    TSchedulerThread(
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
    static void* ThreadMain(void* opaque);
    void ThreadMain();
    void ThreadMainLoop();
    void FiberMain(unsigned int epoch);

    EBeginExecuteResult Execute(unsigned int spawnedEpoch);

    void Reschedule(TFiberPtr fiber, TFuture<void> future, IInvokerPtr invoker);
    void Crash(std::exception_ptr exception);

    TEventCount* EventCount;
    Stroka ThreadName;
    bool EnableLogging;

    NProfiling::TProfiler Profiler;

    // If (Epoch & 0x1) == 0x1 then the thread is running.
    std::atomic_uint Epoch;

    TPromise<void> Started;

    TThreadId ThreadId;
    TThread Thread;

    TExecutionContext SchedulerContext;

    std::list<TFiberPtr> RunQueue;
    int FibersCreated;
    int FibersAlive;

    TFiberPtr IdleFiber;
    TFiberPtr CurrentFiber;

    TFuture<void> WaitForFuture;
    IInvokerPtr SwitchToInvoker;

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);
};

////////////////////////////////////////////////////////////////////////////////

class TSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSingleQueueSchedulerThread(
        TInvokerQueuePtr queue,
        TEventCount* eventCount,
        const Stroka& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    ~TSingleQueueSchedulerThread();

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

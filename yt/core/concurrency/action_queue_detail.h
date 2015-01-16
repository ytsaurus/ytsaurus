#pragma once

#include "public.h"
#include "event_count.h"
#include "scheduler.h"
#include "execution_context.h"
#include "thread_affinity.h"

#include <core/actions/callback.h>
#include <core/actions/future.h>
#include <core/actions/signal.h>

#include <core/profiling/profiler.h>

#include <core/ypath/public.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

#include <contrib/libev/ev++.h>

#include <atomic>

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

DEFINE_ENUM(EBeginExecuteResult,
    (Success)
    (QueueEmpty)
    (Terminated)
);

struct TEnqueuedAction
{
    bool Finished = true;
    NProfiling::TCpuInstant EnqueuedAt;
    NProfiling::TCpuInstant StartedAt;
    TClosure Callback;
};

class TInvokerQueue
    : public IInvoker
{
public:
    TInvokerQueue(
        TEventCount* callbackEventCount,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    void SetThreadId(TThreadId threadId);

    virtual void Invoke(const TClosure& callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual TThreadId GetThreadId() const override;
    virtual bool CheckAffinity(IInvokerPtr invoker) const override;
#endif

    void Shutdown();

    EBeginExecuteResult BeginExecute(TEnqueuedAction* action);
    void EndExecute(TEnqueuedAction* action);

    int GetSize() const;
    bool IsEmpty() const;

    bool IsRunning() const;

private:
    TEventCount* CallbackEventCount;
    bool EnableLogging;

    NConcurrency::TThreadId ThreadId = NConcurrency::InvalidThreadId;
    std::atomic<bool> Running;

    NProfiling::TProfiler Profiler;

    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter DequeueCounter;
    std::atomic<int> QueueSize;
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
    virtual void SubscribeContextSwitched(TClosure callback) override;
    virtual void UnsubscribeContextSwitched(TClosure callback) override;
    virtual void WaitFor(TFuture<void> future, IInvokerPtr invoker) override;

protected:
    TSchedulerThread(
        TEventCount* callbackEventCount,
        const Stroka& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    virtual EBeginExecuteResult BeginExecute() = 0;
    virtual void EndExecute() = 0;

    virtual void OnStart();
    virtual void OnShutdown();

    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

    static void* ThreadMain(void* opaque);
    void ThreadMain();
    void ThreadMainStep();

    void FiberMain(unsigned int spawnedEpoch);
    bool FiberMainStep(unsigned int spawnedEpoch);

    void Reschedule(TFiberPtr fiber, TFuture<void> future, IInvokerPtr invoker);

    void OnContextSwitch();

    TEventCount* CallbackEventCount;
    Stroka ThreadName;
    bool EnableLogging;

    NProfiling::TProfiler Profiler;

    // If (Epoch & 0x1) == 0x1 then the thread is stopping.
    std::atomic<ui32> Epoch;

    TEvent ThreadStartedEvent;

    TThreadId ThreadId = InvalidThreadId;
    TThread Thread;

    TExecutionContext SchedulerContext;

    std::list<TFiberPtr> RunQueue;
    int FibersCreated = 0;
    int FibersAlive = 0;

    TFiberPtr IdleFiber;
    TFiberPtr CurrentFiber;

    TFuture<void> WaitForFuture;
    IInvokerPtr SwitchToInvoker;

    TCallbackList<void()> ContextSwitchCallbacks;

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);
};

////////////////////////////////////////////////////////////////////////////////

class TSingleQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TSingleQueueSchedulerThread(
        TInvokerQueuePtr queue,
        TEventCount* callbackEventCount,
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

class TEVSchedulerThread
    : public TSchedulerThread
{
public:
    TEVSchedulerThread(
        const Stroka& threadName,
        bool enableLogging);

    IInvokerPtr GetInvoker();

protected:
    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TEVSchedulerThread* owner);

        virtual void Invoke(const TClosure& callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual TThreadId GetThreadId() const override;
        virtual bool CheckAffinity(IInvokerPtr invoker) const override;
#endif

    private:
        TEVSchedulerThread* Owner;

    };

    TEventCount CallbackEventCount; // fake

    ev::dynamic_loop EventLoop;
    ev::async CallbackWatcher;

    TIntrusivePtr<TInvoker> Invoker;
    TLockFreeQueue<TClosure> Queue;

    virtual void OnShutdown() override;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;

    EBeginExecuteResult BeginExecuteCallbacks();
    void OnCallback(ev::async&, int);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#pragma once

#include "private.h"
#include "event_count.h"
#include "execution_context.h"
#include "invoker_queue.h"
#include "scheduler.h"
#include "thread_affinity.h"

#include <yt/core/actions/callback.h>
#include <yt/core/actions/future.h>
#include <yt/core/actions/invoker.h>
#include <yt/core/actions/signal.h>

#include <yt/core/misc/shutdownable.h>

#include <yt/core/profiling/profiler.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThread
    : public TRefCounted
    , public IScheduler
    , public IShutdownable
{
public:
    virtual ~TSchedulerThread();

    void Start();
    virtual void Shutdown() override;

    TThreadId GetId() const;
    bool IsStarted() const;
    bool IsShutdown() const;

    virtual TFiber* GetCurrentFiber() override;
    virtual void Return() override;
    virtual void YieldTo(TFiberPtr&& other) override;
    virtual void SwitchTo(IInvokerPtr invoker) override;
    virtual void PushContextSwitchHandler(std::function<void()> out, std::function<void()> in) override;
    virtual void PopContextSwitchHandler() override;
    virtual void WaitFor(TFuture<void> future, IInvokerPtr invoker) override;

protected:
    TSchedulerThread(
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    virtual EBeginExecuteResult BeginExecute() = 0;
    virtual void EndExecute() = 0;

    virtual void OnStart();

    virtual void BeforeShutdown();
    virtual void AfterShutdown();

    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

    static void* ThreadMain(void* opaque);
    void ThreadMain();
    void ThreadMainStep();

    void FiberMain(ui64 spawnedEpoch);
    bool FiberMainStep(ui64 spawnedEpoch);

    void Reschedule(TFiberPtr fiber, TFuture<void> future, IInvokerPtr invoker);

    const std::shared_ptr<TEventCount> CallbackEventCount_;
    const TString ThreadName_;
    const bool EnableLogging_;

    NProfiling::TProfiler Profiler;

    // First bit is an indicator whether startup was performed.
    // Second bit is an indicator whether shutdown was requested.
    std::atomic<ui64> Epoch_ = {0};
    static constexpr ui64 StartedEpochMask = 0x1;
    static constexpr ui64 ShutdownEpochMask = 0x2;
    static constexpr ui64 TurnShift = 2;
    static constexpr ui64 TurnDelta = 1 << TurnShift;

    TEvent ThreadStartedEvent_;
    TEvent ThreadShutdownEvent_;

    TThreadId ThreadId_ = InvalidThreadId;
    TThread Thread_;

    TExecutionContext SchedulerContext_;

    std::list<TFiberPtr> RunQueue_;
    NProfiling::TMonotonicCounter CreatedFibersCounter_;
    NProfiling::TSimpleGauge AliveFibersCounter_;

    TFiberPtr IdleFiber_;
    TFiberPtr CurrentFiber_;

    TFuture<void> WaitForFuture_;
    IInvokerPtr SwitchToInvoker_;

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);

private:
    void SwitchContextFrom(TFiber* currentFiber);
    void SetCurrentFiber(TFiberPtr fiber);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#pragma once

#include "public.h"
#include "private.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInvokerQueueType,
    (SingleLockFreeQueue)
    (MultiLockQueue)
);

struct IActionQueue;

class TInvokerQueue
    : public IInvoker
    , public IShutdownable
{
public:
    TInvokerQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling,
        EInvokerQueueType type = EInvokerQueueType::SingleLockFreeQueue);

    ~TInvokerQueue();

    void SetThreadId(TThreadId threadId);

    void Configure(int threadCount);

    virtual void Invoke(TClosure callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual TThreadId GetThreadId() const override;
    virtual bool CheckAffinity(const IInvokerPtr& invoker) const override;
#endif

    virtual void Shutdown() override;

    void Drain();

    EBeginExecuteResult BeginExecute(TEnqueuedAction* action, int index = 0);
    void EndExecute(TEnqueuedAction* action);

    int GetSize() const;

    bool IsEmpty() const;

    bool IsRunning() const;

private:
    const std::shared_ptr<TEventCount> CallbackEventCount;
    bool EnableLogging;

    NConcurrency::TThreadId ThreadId = NConcurrency::InvalidThreadId;

    std::atomic<bool> Running = {true};

    std::unique_ptr<IActionQueue> Queue;
    std::atomic<int> QueueSize = {0};

    NProfiling::TProfiler Profiler;
    NProfiling::TMonotonicCounter EnqueuedCounter;
    NProfiling::TMonotonicCounter DequeuedCounter;
    NProfiling::TAggregateGauge SizeCounter;
    NProfiling::TAggregateGauge WaitTimeCounter;
    NProfiling::TAggregateGauge ExecTimeCounter;
    NProfiling::TMonotonicCounter CumulativeTimeCounter;
    NProfiling::TAggregateGauge TotalTimeCounter;
};

DEFINE_REFCOUNTED_TYPE(TInvokerQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

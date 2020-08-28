#pragma once

#include "public.h"
#include "private.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/profiling/profiler.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

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

    virtual void Invoke(TClosure callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual TThreadId GetThreadId() const override;
    virtual bool CheckAffinity(const IInvokerPtr& invoker) const override;
#endif

    virtual void Shutdown() override;

    void Drain();

    TClosure BeginExecute(TEnqueuedAction* action);
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

    NProfiling::TProfiler Profiler;
    NProfiling::TShardedMonotonicCounter EnqueuedCounter;
    NProfiling::TShardedMonotonicCounter DequeuedCounter;
    NProfiling::TAtomicShardedAggregateGauge SizeGauge;
    NProfiling::TShardedAggregateGauge WaitTimeGauge;
    NProfiling::TShardedAggregateGauge ExecTimeGauge;
    NProfiling::TShardedMonotonicCounter CumulativeTimeCounter;
    NProfiling::TShardedAggregateGauge TotalTimeGauge;
};

DEFINE_REFCOUNTED_TYPE(TInvokerQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

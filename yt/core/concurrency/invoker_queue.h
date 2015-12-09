#pragma once

#include "public.h"
#include "private.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/profiling/profiler.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TInvokerQueue
    : public IInvoker
    , public IShutdownable
{
public:
    TInvokerQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        const NProfiling::TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling);

    ~TInvokerQueue();

    void SetThreadId(TThreadId threadId);

    virtual void Invoke(const TClosure& callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual TThreadId GetThreadId() const override;
    virtual bool CheckAffinity(IInvokerPtr invoker) const override;
#endif

    virtual void Shutdown() override;

    EBeginExecuteResult BeginExecute(TEnqueuedAction* action);
    void EndExecute(TEnqueuedAction* action);

    int GetSize() const;

    bool IsEmpty() const;

    bool IsRunning() const;

private:
    std::shared_ptr<TEventCount> CallbackEventCount;
    bool EnableLogging;

    NConcurrency::TThreadId ThreadId = NConcurrency::InvalidThreadId;

    std::atomic<bool> Running = {true};

    TLockFreeQueue<TEnqueuedAction> Queue;
    std::atomic<int> QueueSize = {0};

    NProfiling::TProfiler Profiler;
    NProfiling::TSimpleCounter EnqueuedCounter;
    NProfiling::TSimpleCounter DequeuedCounter;
    NProfiling::TAggregateCounter SizeCounter;
    NProfiling::TAggregateCounter WaitTimeCounter;
    NProfiling::TAggregateCounter ExecTimeCounter;
    NProfiling::TAggregateCounter TotalTimeCounter;
};

DEFINE_REFCOUNTED_TYPE(TInvokerQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#pragma once

#include "public.h"

#include <core/actions/callback.h>
#include <core/actions/invoker.h>

#include <core/profiling/profiler.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TEventCount;

class TInvokerQueue;
typedef TIntrusivePtr<TInvokerQueue> TInvokerQueuePtr;

DEFINE_ENUM(EBeginExecuteResult,
    (Success)
    (QueueEmpty)
    (Terminated)
);

struct TEnqueuedAction
{
    bool Finished = true;
    NProfiling::TCpuInstant EnqueuedAt = 0;
    NProfiling::TCpuInstant StartedAt = 0;
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

    ~TInvokerQueue();

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

    std::atomic<bool> Running = {true};

    TLockFreeQueue<TEnqueuedAction> Queue;
    std::atomic<int> QueueSize = {0};

    NProfiling::TProfiler Profiler;
    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter DequeueCounter;
    NProfiling::TAggregateCounter QueueSizeCounter;
    NProfiling::TAggregateCounter WaitTimeCounter;
    NProfiling::TAggregateCounter ExecTimeCounter;
    NProfiling::TAggregateCounter TotalTimeCounter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

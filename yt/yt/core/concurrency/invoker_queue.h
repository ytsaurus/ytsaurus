#pragma once

#include "private.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TEnqueuedAction
{
    bool Finished = true;
    NProfiling::TCpuInstant EnqueuedAt = 0;
    NProfiling::TCpuInstant StartedAt = 0;
    NProfiling::TCpuInstant FinishedAt = 0;
    TClosure Callback;
};

////////////////////////////////////////////////////////////////////////////////

class TMpmcQueueImpl
{
public:
    void Enqueue(TEnqueuedAction action);
    bool TryDequeue(TEnqueuedAction* action);

private:
    moodycamel::ConcurrentQueue<TEnqueuedAction> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

class TMpscQueueImpl
{
public:
    void Enqueue(TEnqueuedAction action);
    bool TryDequeue(TEnqueuedAction* action);

private:
    TLockFreeQueue<TEnqueuedAction> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TInvokerQueue
    : public IInvoker
    , public IShutdownable
{
public:
    TInvokerQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        const NProfiling::TTagSet& tagSet);

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
    const std::shared_ptr<TEventCount> CallbackEventCount_;

    TQueueImpl QueueImpl_;

    NConcurrency::TThreadId ThreadId_ = NConcurrency::InvalidThreadId;
    std::atomic<bool> Running_ = true;
    std::atomic<int> Size_ = 0;

    NProfiling::TCounter EnqueuedCounter_;
    NProfiling::TCounter DequeuedCounter_;
    NProfiling::TEventTimer WaitTimer_;
    NProfiling::TEventTimer ExecTimer_;
    NProfiling::TTimeCounter CumulativeTimeCounter_;
    NProfiling::TEventTimer TotalTimer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

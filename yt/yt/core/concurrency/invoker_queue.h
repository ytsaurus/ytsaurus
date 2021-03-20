#pragma once

#include "private.h"

#include <yt/yt/core/misc/shutdownable.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

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
    int ProfilingTag = 0;
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
        const NProfiling::TTagSet& counterTagSet);

    TInvokerQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        const std::vector<NProfiling::TTagSet>& counterTagSets,
        const NProfiling::TTagSet& cumulativeCounterTagSet);

    void SetThreadId(TThreadId threadId);

    virtual void Invoke(TClosure callback) override;
    void Invoke(TClosure callback, int profilingTag);

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

    IInvokerPtr GetProfilingTagSettingInvoker(int profilingTag);

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_;

    TQueueImpl QueueImpl_;

    NConcurrency::TThreadId ThreadId_ = NConcurrency::InvalidThreadId;
    std::atomic<bool> Running_ = true;
    std::atomic<int> Size_ = 0;

    struct TCounters
    {
        NProfiling::TCounter EnqueuedCounter;
        NProfiling::TCounter DequeuedCounter;
        NProfiling::TEventTimer WaitTimer;
        NProfiling::TEventTimer ExecTimer;
        NProfiling::TTimeCounter CumulativeTimeCounter;
        NProfiling::TEventTimer TotalTimer;
        std::atomic<int> ActiveCallbacks = 0;
    };
    using TCountersPtr = std::unique_ptr<TCounters>;

    std::vector<TCountersPtr> Counters_;
    TCountersPtr CumulativeCounters_;

    std::vector<IInvokerPtr> ProfilingTagSettingInvokers_;

    TCountersPtr CreateCounters(const NProfiling::TTagSet& tagSet);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

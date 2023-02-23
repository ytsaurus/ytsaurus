#pragma once

#include "private.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/ytprof/api/api.h>

#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/misc/mpsc_queue.h>

#include <library/cpp/yt/threading/event_count.h>

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
    NYTProf::TProfilerTagPtr ProfilerTag;
};

////////////////////////////////////////////////////////////////////////////////

class TMpmcQueueImpl
{
public:
    using TConsumerToken = moodycamel::ConsumerToken;

    void Enqueue(TEnqueuedAction action);
    void Enqueue(TMutableRange<TEnqueuedAction> actions);
    bool TryDequeue(TEnqueuedAction *action, TConsumerToken* token = nullptr);

    void DrainProducer();
    void DrainConsumer();

    TConsumerToken MakeConsumerToken();

    bool IsEmpty() const;

    bool HasSingleConsumer() const;

private:
    moodycamel::ConcurrentQueue<TEnqueuedAction> Queue_;
    std::atomic<int> Size_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMpscQueueImpl
{
public:
    using TConsumerToken = std::monostate;

    void Enqueue(TEnqueuedAction action);
    void Enqueue(TMutableRange<TEnqueuedAction> actions);
    bool TryDequeue(TEnqueuedAction* action, TConsumerToken* token = nullptr);

    void DrainProducer();
    void DrainConsumer();

    TConsumerToken MakeConsumerToken();

    bool IsEmpty() const;

    bool HasSingleConsumer() const;

private:
    TMpscQueue<TEnqueuedAction> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
class TInvokerQueue
    : public IInvoker
{
public:
    TInvokerQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const NProfiling::TTagSet& counterTagSet);

    TInvokerQueue(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const std::vector<NProfiling::TTagSet>& counterTagSets,
        const std::vector<NYTProf::TProfilerTagPtr>& profilerTags,
        const NProfiling::TTagSet& cumulativeCounterTagSet);

    void SetThreadId(TThreadId threadId);

    void Invoke(TClosure callback) override;

    void Invoke(TMutableRange<TClosure> callbacks) override;

    void Invoke(
        TClosure callback,
        NProfiling::TTagId profilingTag,
        NYTProf::TProfilerTagPtr profilerTag);

    TEnqueuedAction MakeAction(
        TClosure callback,
        NProfiling::TTagId profilingTag,
        NYTProf::TProfilerTagPtr profilerTag,
        TCpuInstant cpuInstant);

    TCpuInstant EnqueueCallback(
        TClosure callback,
        NProfiling::TTagId profilingTag,
        NYTProf::TProfilerTagPtr profilerTag);

    TCpuInstant EnqueueCallbacks(
        TMutableRange<TClosure> callbacks,
        NProfiling::TTagId profilingTag = 0,
        NYTProf::TProfilerTagPtr profilerTag = nullptr);

    TThreadId GetThreadId() const override;
    bool CheckAffinity(const IInvokerPtr& invoker) const override;
    bool IsSerialized() const override;

    void Shutdown();

    void DrainProducer();
    void DrainConsumer();

    TClosure BeginExecute(TEnqueuedAction* action, typename TQueueImpl::TConsumerToken* token = nullptr);
    void EndExecute(TEnqueuedAction* action);

    typename TQueueImpl::TConsumerToken MakeConsumerToken();

    bool IsEmpty() const;
    bool IsRunning() const;

    IInvoker* GetProfilingTagSettingInvoker(int profilingTag);

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;

    TQueueImpl QueueImpl_;

    NConcurrency::TThreadId ThreadId_ = NConcurrency::InvalidThreadId;
    std::atomic<bool> Running_ = true;

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

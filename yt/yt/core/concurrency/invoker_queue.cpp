#include "invoker_queue.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TMpmcQueueImpl::Enqueue(TEnqueuedAction action)
{
    Queue_.enqueue(std::move(action));
}

Y_FORCE_INLINE bool TMpmcQueueImpl::TryDequeue(TEnqueuedAction* action)
{
    return Queue_.try_dequeue(*action);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TMpscQueueImpl::Enqueue(TEnqueuedAction action)
{
    Queue_.Enqueue(std::move(action));
}

Y_FORCE_INLINE bool TMpscQueueImpl::TryDequeue(TEnqueuedAction* action)
{
    return Queue_.Dequeue(action);
}

////////////////////////////////////////////////////////////////////////////////

template <class TQueueImpl>
TInvokerQueue<TQueueImpl>::TInvokerQueue(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TTagSet& counterTagSet)
    : CallbackEventCount_(std::move(callbackEventCount))
{
    Counters_.push_back(CreateCounters(counterTagSet));
}

template <class TQueueImpl>
TInvokerQueue<TQueueImpl>::TInvokerQueue(
    std::shared_ptr<TEventCount> callbackEventCount,
    const std::vector<TTagSet>& counterTagSets,
    const TTagSet& cumulativeCounterTagSet)
    : CallbackEventCount_(std::move(callbackEventCount))
{
    Counters_.reserve(counterTagSets.size());
    for (const auto& tagSet : counterTagSets) {
        Counters_.push_back(CreateCounters(tagSet));
    }

    CumulativeCounters_ = CreateCounters(cumulativeCounterTagSet);
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::SetThreadId(TThreadId threadId)
{
    ThreadId_ = threadId;
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Invoke(TClosure callback)
{
    Invoke(std::move(callback), CurrentProfilingTag_);
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Invoke(TClosure callback, int profilingTag)
{
    YT_ASSERT(callback);
    YT_ASSERT(profilingTag >= 0 && profilingTag < Counters_.size());

    if (!Running_.load(std::memory_order_relaxed)) {
        YT_LOG_TRACE(
            "Queue had been shut down, incoming action ignored (Callback: %v)",
            callback.GetHandle());
        return;
    }

    YT_LOG_TRACE("Callback enqueued (Callback: %v, ProfilingTag: %v)",
        callback.GetHandle(),
        profilingTag);

    auto tscp = TTscp::Get();

    Size_ += 1;

    auto updateCounters = [&] (const TCountersPtr& counters) {
        if (counters) {
            counters->ActiveCallbacks += 1;
            counters->EnqueuedCounter.Increment();
        }
    };
    updateCounters(Counters_[profilingTag]);
    updateCounters(CumulativeCounters_);

    TEnqueuedAction action{
        .Finished = false,
        .EnqueuedAt = tscp.Instant,
        .Callback = std::move(callback),
        .ProfilingTag = profilingTag
    };
    QueueImpl_.Enqueue(std::move(action));

    CallbackEventCount_->NotifyOne();
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
template <class TQueueImpl>
TThreadId TInvokerQueue<TQueueImpl>::GetThreadId() const
{
    return ThreadId_;
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::CheckAffinity(const IInvokerPtr& invoker) const
{
    return invoker.Get() == this;
}
#endif

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Shutdown()
{
    Running_.store(false, std::memory_order_relaxed);
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Drain()
{
    YT_VERIFY(!Running_.load(std::memory_order_relaxed));

    TEnqueuedAction action;
    while (QueueImpl_.TryDequeue(&action));

    Size_.store(0);
}

template <class TQueueImpl>
TClosure TInvokerQueue<TQueueImpl>::BeginExecute(TEnqueuedAction* action)
{
    YT_ASSERT(action && action->Finished);

    auto tscp = TTscp::Get();
    if (!QueueImpl_.TryDequeue(action)) {
        return {};
    }

    action->StartedAt = tscp.Instant;

    auto waitTime = CpuDurationToDuration(action->StartedAt - action->EnqueuedAt);

    auto updateCounters = [&] (const TCountersPtr& counters) {
        if (counters) {
            counters->DequeuedCounter.Increment();
            counters->WaitTimer.Record(waitTime);
        }
    };
    updateCounters(Counters_[action->ProfilingTag]);
    updateCounters(CumulativeCounters_);

    SetCurrentInvoker(this);
    CurrentProfilingTag_ = action->ProfilingTag;

    return std::move(action->Callback);
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::EndExecute(TEnqueuedAction* action)
{
    SetCurrentInvoker(nullptr);

    YT_ASSERT(action);

    if (action->Finished) {
        return;
    }

    auto tscp = TTscp::Get();
    action->FinishedAt = tscp.Instant;
    action->Finished = true;

    auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
    auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);

    Size_ -= 1;

    auto updateCounters = [&] (const TCountersPtr& counters) {
        if (counters) {
            counters->ExecTimer.Record(timeFromStart);
            counters->CumulativeTimeCounter.Add(timeFromStart);
            counters->TotalTimer.Record(timeFromEnqueue);
            counters->ActiveCallbacks -= 1;
        }
    };
    updateCounters(Counters_[action->ProfilingTag]);
    updateCounters(CumulativeCounters_);
}

template <class TQueueImpl>
int TInvokerQueue<TQueueImpl>::GetSize() const
{
    return Size_.load();
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::IsEmpty() const
{
    return GetSize() == 0;
}

template <class TQueueImpl>
bool TInvokerQueue<TQueueImpl>::IsRunning() const
{
    return Running_.load(std::memory_order_relaxed);
}

template <class TQueueImpl>
typename TInvokerQueue<TQueueImpl>::TCountersPtr TInvokerQueue<TQueueImpl>::CreateCounters(const TTagSet& tagSet)
{
    auto profiler = TProfiler("/action_queue").WithTags(tagSet);

    auto counters = std::make_unique<TCounters>();
    counters->EnqueuedCounter = profiler.Counter("/enqueued");
    counters->DequeuedCounter = profiler.Counter("/dequeued");
    counters->WaitTimer = profiler.Timer("/time/wait");
    counters->ExecTimer = profiler.Timer("/time/exec");
    counters->CumulativeTimeCounter = profiler.TimeCounter("/time/cumulative");
    counters->TotalTimer = profiler.Timer("/time/total");

    profiler.AddFuncGauge("/size", MakeStrong(this), [counters = counters.get()] {
        return counters->ActiveCallbacks.load();
    });

    return counters;
}

////////////////////////////////////////////////////////////////////////////////

template class TInvokerQueue<TMpmcQueueImpl>;
template class TInvokerQueue<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

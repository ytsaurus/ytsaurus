#include "invoker_queue.h"
#include "private.h"

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
    const TTagSet& tags)
    : CallbackEventCount_(std::move(callbackEventCount))
{
    auto profiler = TRegistry("/action_queue").WithTags(tags);

    EnqueuedCounter_ = profiler.Counter("/enqueued");
    DequeuedCounter_ = profiler.Counter("/dequeued");
    profiler.AddFuncGauge("/size", MakeStrong(this), [this] {
        return Size_.load();
    });
    WaitTimer_ = profiler.Timer("/time/wait");
    ExecTimer_ = profiler.Timer("/time/exec");
    CumulativeTimeCounter_ = profiler.TimeCounter("/time/cumulative");
    TotalTimer_ = profiler.Timer("/time/total");
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::SetThreadId(TThreadId threadId)
{
    ThreadId_ = threadId;
}

template <class TQueueImpl>
void TInvokerQueue<TQueueImpl>::Invoke(TClosure callback)
{
    YT_ASSERT(callback);

    if (!Running_.load(std::memory_order_relaxed)) {
        YT_LOG_TRACE(
            "Queue had been shut down, incoming action ignored (Callback: %v)",
            callback.GetHandle());
        return;
    }

    YT_LOG_TRACE("Callback enqueued (Callback: %v)",
        callback.GetHandle());

    auto tscp = TTscp::Get();

    Size_ += 1;
    EnqueuedCounter_.Increment();

    TEnqueuedAction action;
    action.Finished = false;
    action.EnqueuedAt = tscp.Instant;
    action.Callback = std::move(callback);

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

    DequeuedCounter_.Increment();
    WaitTimer_.Record(waitTime);

    SetCurrentInvoker(this);

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
    ExecTimer_.Record(timeFromStart);
    CumulativeTimeCounter_.Add(timeFromStart);
    TotalTimer_.Record(timeFromEnqueue);
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

////////////////////////////////////////////////////////////////////////////////

template class TInvokerQueue<TMpmcQueueImpl>;
template class TInvokerQueue<TMpscQueueImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

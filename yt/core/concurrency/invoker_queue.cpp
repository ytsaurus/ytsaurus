#include "stdafx.h"
#include "action_queue_detail.h"
#include "private.h"

#include <core/logging/log.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

///////////////////////////////////////////////////////////////////////////////

TInvokerQueue::TInvokerQueue(
    TEventCount* callbackEventCount,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : CallbackEventCount(callbackEventCount)
    , EnableLogging(enableLogging)
    , Profiler("/action_queue")
    , EnqueueCounter("/enqueue_rate", tagIds)
    , DequeueCounter("/dequeue_rate", tagIds)
    , QueueSizeCounter("/size", tagIds)
    , WaitTimeCounter("/time/wait", tagIds)
    , ExecTimeCounter("/time/exec", tagIds)
    , TotalTimeCounter("/time/total", tagIds)
{
    Profiler.SetEnabled(enableProfiling);
}

void TInvokerQueue::SetThreadId(TThreadId threadId)
{
    ThreadId = threadId;
}

void TInvokerQueue::Invoke(const TClosure& callback)
{
    YASSERT(callback);

    if (!Running.load(std::memory_order_relaxed)) {
        LOG_TRACE_IF(
            EnableLogging,
            "Queue had been shut down, incoming action ignored: %p",
            callback.GetHandle());
        return;
    }

    ++QueueSize;
    Profiler.Increment(EnqueueCounter);

    LOG_TRACE_IF(EnableLogging, "Callback enqueued: %p",
        callback.GetHandle());

    TEnqueuedAction action;
    action.Finished = false;
    action.EnqueuedAt = GetCpuInstant();
    action.Callback = callback;
    Queue.Enqueue(action);

    CallbackEventCount->NotifyOne();
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
TThreadId TInvokerQueue::GetThreadId() const
{
    return ThreadId;
}

bool TInvokerQueue::CheckAffinity(IInvokerPtr invoker) const
{
    return invoker.Get() == this;
}
#endif

void TInvokerQueue::Shutdown()
{
    Running.store(false, std::memory_order_relaxed);
}

bool TInvokerQueue::IsRunning() const
{
    return Running;
}

EBeginExecuteResult TInvokerQueue::BeginExecute(TEnqueuedAction* action)
{
    YASSERT(action->Finished);

    if (!Queue.Dequeue(action)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    CallbackEventCount->CancelWait();

    Profiler.Increment(DequeueCounter);

    action->StartedAt = GetCpuInstant();
    Profiler.Aggregate(
        WaitTimeCounter,
        CpuDurationToValue(action->StartedAt - action->EnqueuedAt));

    // Move callback to the stack frame to ensure that we hold it as long as it runs.
    auto callback = std::move(action->Callback);
    try {
        TCurrentInvokerGuard guard(this);
        callback.Run();
        return EBeginExecuteResult::Success;
    } catch (const TFiberCanceledException&) {
        return EBeginExecuteResult::Terminated;
    }
}

void TInvokerQueue::EndExecute(TEnqueuedAction* action)
{
    if (action->Finished)
        return;

    int size = --QueueSize;
    Profiler.Aggregate(QueueSizeCounter, size);

    auto endedAt = GetCpuInstant();
    Profiler.Aggregate(
        ExecTimeCounter,
        CpuDurationToValue(endedAt - action->StartedAt));
    Profiler.Aggregate(
        TotalTimeCounter,
        CpuDurationToValue(endedAt - action->EnqueuedAt));

    action->Finished = true;
}

int TInvokerQueue::GetSize() const
{
    return QueueSize.load();
}

bool TInvokerQueue::IsEmpty() const
{
    return const_cast<TLockFreeQueue<TEnqueuedAction>&>(Queue).IsEmpty();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

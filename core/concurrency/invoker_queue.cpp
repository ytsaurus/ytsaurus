#include "invoker_queue.h"
#include "private.h"

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

//! Queue interface to enqueue or dequeue actions.
struct IActionQueue
{
    virtual ~IActionQueue() = default;

    //! Inserts element into the queue.
    /*!
     * \param action Action to be enqueued.
     * \param index Index is used as a hint to place the action
     * using the most suitable implementation-specific way.
     */
    virtual void Enqueue(const TEnqueuedAction& action, int index) = 0;

    //! Extracts single element from the queue.
    /*!
     * \param action Pointer to action instance to be dequeued.
     * \param index Index is used as a hint to extract the action
     * using the most suitable implementation-specific way.
     * \return |true| on successful operation. False on empty queue.
     */
    virtual bool Dequeue(TEnqueuedAction* action, int index) = 0;

    //! Configures the queue for the specified number of threads.
    /*!
     * \param threadCount Number of threads to configure the queue.
     *
     * \note Must be invoked before any Enqueue/Dequeue invocations.
     */
    virtual void Configure(int threadCount) = 0;
};

class TLockFreeActionQueue
    : public IActionQueue
{
public:
    virtual void Enqueue(const TEnqueuedAction& action, int /*index*/) override
    {
        Queue_.Enqueue(action);
    }

    virtual bool Dequeue(TEnqueuedAction *action, int /*index*/) override
    {
        return Queue_.Dequeue(action);
    }

    virtual void Configure(int threadCount) override
    { }

private:
    TLockFreeQueue<TEnqueuedAction> Queue_;
};

template <typename T, typename TLock>
class TLockQueue
{
    using TLockGuard = TGuard<TLock>;
    using TTryLockGuard = TGuard<TLock, TTryLockOps<TLock>>;

public:
    bool Dequeue(T* val)
    {
        TLockGuard lock(Lock_);
        if (Queue_.empty()) {
            return false;
        }
        *val = std::move(Queue_.front());
        Queue_.pop_front();
        return true;
    }

    template <typename... U>
    void Enqueue(U&&... val)
    {
        TLockGuard lock(Lock_);
        Queue_.emplace_back(std::forward<U>(val)...);
    }

    bool TryDequeue(T* val)
    {
        TTryLockGuard lock(Lock_);
        if (!lock || Queue_.empty()) {
            return false;
        }
        *val = std::move(Queue_.front());
        Queue_.pop_front();
        return true;
    }

    template <typename... U>
    bool TryEnqueue(U&&... val)
    {
        TTryLockGuard lock(Lock_);
        if (!lock) {
            return false;
        }
        Queue_.emplace_back(std::forward<U>(val)...);
        return true;
    }

private:
    std::deque<T> Queue_;
    TLock Lock_;
};

template <typename T>
class TTryQueues
{
    using TQueueType = TLockQueue<T, TSpinLock>;

public:
    void Configure(int queueCount)
    {
        Queues_.resize(queueCount);
    }

    template <typename U>
    void Enqueue(U&& val, int index)
    {
        TryQueue(
            index,
            [&] (TQueueType& q) {
                return q.TryEnqueue(std::forward<U>(val));
            },
            [&] (TQueueType& q) {
                q.Enqueue(std::forward<U>(val));
                return true;
            });
    }

    bool Dequeue(T* val, int index)
    {
        Y_ASSERT(val);

        return TryQueue(
            index,
            [&] (TQueueType& q) {
                return q.TryDequeue(val);
            },
            [&] (TQueueType& q) {
                return q.Dequeue(val);
            });
    }

private:
    TQueueType& GetQueue(int index)
    {
        return Queues_[index % Queues_.size()];
    }

    template <typename FTry, typename F>
    bool TryQueue(int i, FTry&& fTry, F&& f)
    {
        for (size_t n = 0; n < Queues_.size(); ++ n) {
            if (fTry(GetQueue(i + n))) {
                return true;
            }
        }
        return f(GetQueue(i));
    }

    std::vector<TQueueType> Queues_;
};

class TMultiLockActionQueue
    : public IActionQueue
{
public:
    virtual void Enqueue(const TEnqueuedAction& action, int index) override
    {
        Queue_.Enqueue(action, index);
    }

    virtual bool Dequeue(TEnqueuedAction *action, int index) override
    {
        return Queue_.Dequeue(action, index);
    }

    virtual void Configure(int threadCount) override
    {
        Queue_.Configure(threadCount);
    }

private:
    TTryQueues<TEnqueuedAction> Queue_;
};

std::unique_ptr<IActionQueue> CreateActionQueue(EInvokerQueueType type)
{
    switch (type) {
        case EInvokerQueueType::SingleLockFreeQueue:
            return std::make_unique<TLockFreeActionQueue>();
        case EInvokerQueueType::MultiLockQueue:
            return std::make_unique<TMultiLockActionQueue>();
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

TInvokerQueue::TInvokerQueue(
    std::shared_ptr<TEventCount> callbackEventCount,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling,
    EInvokerQueueType type)
    : CallbackEventCount(std::move(callbackEventCount))
    , EnableLogging(enableLogging)
    , Queue(CreateActionQueue(type))
    , Profiler("/action_queue")
    , EnqueuedCounter("/enqueued", tagIds)
    , DequeuedCounter("/dequeued", tagIds)
    , SizeCounter("/size", tagIds)
    , WaitTimeCounter("/time/wait", tagIds)
    , ExecTimeCounter("/time/exec", tagIds)
    , CumulativeTimeCounter("/time/cumulative", tagIds)
    , TotalTimeCounter("/time/total", tagIds)
{
    Profiler.SetEnabled(enableProfiling);
}

TInvokerQueue::~TInvokerQueue() = default;

void TInvokerQueue::SetThreadId(TThreadId threadId)
{
    ThreadId = threadId;
}

void TInvokerQueue::Configure(int threadCount)
{
    Queue->Configure(threadCount);
}

void TInvokerQueue::Invoke(TClosure callback)
{
    Y_ASSERT(callback);

    if (!Running.load(std::memory_order_relaxed)) {
        LOG_TRACE_IF(
            EnableLogging,
            "Queue had been shut down, incoming action ignored: %p",
            callback.GetHandle());
        return;
    }

    QueueSize.fetch_add(1, std::memory_order_relaxed);

    auto index = Profiler.Increment(EnqueuedCounter);

    (void)EnableLogging;
    LOG_TRACE_IF(EnableLogging, "Callback enqueued: %p",
        callback.GetHandle());

    TEnqueuedAction action;
    action.Finished = false;
    action.EnqueuedAt = GetCpuInstant();
    action.Callback = std::move(callback);
    Queue->Enqueue(action, index);

    CallbackEventCount->NotifyOne();
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
TThreadId TInvokerQueue::GetThreadId() const
{
    return ThreadId;
}

bool TInvokerQueue::CheckAffinity(const IInvokerPtr& invoker) const
{
    return invoker.Get() == this;
}
#endif

void TInvokerQueue::Shutdown()
{
    Running.store(false, std::memory_order_relaxed);
}

void TInvokerQueue::Drain()
{
    YCHECK(!Running.load(std::memory_order_relaxed));

    Queue.reset();
    QueueSize = 0;
}

EBeginExecuteResult TInvokerQueue::BeginExecute(TEnqueuedAction* action, int index)
{
    Y_ASSERT(action && action->Finished);
    Y_ASSERT(Queue);

    if (!Queue->Dequeue(action, index)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    CallbackEventCount->CancelWait();

    Profiler.Increment(DequeuedCounter);

    action->StartedAt = GetCpuInstant();

    Profiler.Update(
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
    Y_ASSERT(action);

    if (action->Finished) {
        return;
    }

    int queueSize = QueueSize.fetch_sub(1, std::memory_order_relaxed) - 1;
    Profiler.Update(SizeCounter, queueSize);

    action->FinishedAt = GetCpuInstant();
    auto timeFromStart = CpuDurationToValue(action->FinishedAt - action->StartedAt);
    auto timeFromEnqueue = CpuDurationToValue(action->FinishedAt - action->EnqueuedAt);
    Profiler.Update(ExecTimeCounter, timeFromStart);
    Profiler.Increment(CumulativeTimeCounter, timeFromStart);
    Profiler.Update(TotalTimeCounter, timeFromEnqueue);

    action->Finished = true;
}

int TInvokerQueue::GetSize() const
{
    return QueueSize.load(std::memory_order_relaxed);
}

bool TInvokerQueue::IsEmpty() const
{
    return GetSize() == 0;
}

bool TInvokerQueue::IsRunning() const
{
    return Running.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

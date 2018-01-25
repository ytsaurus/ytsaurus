#include "fair_share_thread_pool.h"
#include "private.h"
#include "invoker_queue.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"

#include <yt/core/misc/heap.h>
#include <yt/core/misc/ring_queue.h>
#include <yt/core/misc/weak_ptr.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeapItem;
struct TBucket;
class TFairShareQueue;

DECLARE_REFCOUNTED_TYPE(TBucket)

struct TBucket
    : public IInvoker
{
    TBucket(TFairShareThreadPoolTag tag, TWeakPtr<TFairShareQueue> parent)
        : Tag(std::move(tag))
        , Parent(std::move(parent))
    { }

    void RunCallback(const TClosure& callback)
    {
        TCurrentInvokerGuard currentInvokerGuard(this);
        callback.Run();
    }

    virtual void Invoke(TClosure callback) override;

    void Drain()
    {
        TGuard<TSpinLock> guard(SpinLock);
        Queue.clear();
        Size = 0;
    }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual NConcurrency::TThreadId GetThreadId() const
    {
        return InvalidThreadId;
    }

    virtual bool CheckAffinity(const IInvokerPtr& invoker) const
    {
        return invoker.Get() == this;
    }
#endif

    ~TBucket();

    TFairShareThreadPoolTag Tag;
    TWeakPtr<TFairShareQueue> Parent;
    TRingQueue<TEnqueuedAction> Queue;
    TSpinLock SpinLock;
    size_t Size = 0;
    THeapItem* HeapIterator = nullptr;
};

DEFINE_REFCOUNTED_TYPE(TBucket)

struct THeapItem
{
    TCpuDuration ExcessTime;
    TBucketPtr Bucket;

    THeapItem(const THeapItem&) = delete;
    THeapItem& operator=(const THeapItem&) = delete;

    THeapItem(
        TCpuDuration excessTime,
        TBucketPtr bucket)
        : ExcessTime(excessTime)
        , Bucket(std::move(bucket))
    {
        AdjustBackReference(this);
    }

    THeapItem(THeapItem&& other) noexcept
        : ExcessTime(other.ExcessTime)
        , Bucket(std::move(other.Bucket))
    {
        AdjustBackReference(this);
    }

    THeapItem& operator=(THeapItem&& other) noexcept
    {
        ExcessTime = other.ExcessTime;
        Bucket = std::move(other.Bucket);
        AdjustBackReference(this);

        return *this;
    }

    void AdjustBackReference(THeapItem* iterator)
    {
        if (Bucket) {
            Bucket->HeapIterator = iterator;
        }
    }

    ~THeapItem()
    {
        if (Bucket) {
            Bucket->HeapIterator = nullptr;
        }
    }
};

bool operator < (const THeapItem& lhs, const THeapItem& rhs)
{
    return lhs.ExcessTime < rhs.ExcessTime;
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_TYPE(TFairShareQueue)

class TFairShareQueue
    : public TRefCounted
    , public IShutdownable
{
public:
    TFairShareQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        int threadCount,
        const TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling)
        : CallbackEventCount_(callbackEventCount)
        , CurrentlyExecutingActionsByThread_(threadCount)
        , EnableLogging_(enableLogging)
        , EnableProfiling_(enableProfiling)
        , Profiler_("/fair_share_queue")
        , BucketCounter_("/buckets", tagIds)
        , SizeCounter_("/size", tagIds)
        , WaitTimeCounter_("/time/wait", tagIds)
        , ExecTimeCounter_("/time/exec", tagIds)
        , TotalTimeCounter_("/time/total", tagIds)
    {
        Profiler_.SetEnabled(enableProfiling);
    }

    ~TFairShareQueue()
    {
        Shutdown();
    }

    IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag)
    {
        TGuard<TSpinLock> guard(TagMappingSpinLock_);

        auto inserted = TagToBucket_.emplace(tag, nullptr).first;
        auto invoker = inserted->second.Lock();

        if (!invoker) {
            invoker = New<TBucket>(tag, MakeWeak(this));
            inserted->second = invoker;
        }

        Profiler_.Update(BucketCounter_, TagToBucket_.size());

        return invoker;
    }

    void Invoke(TClosure callback, TBucket* bucket)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        QueueSize_.fetch_add(1, std::memory_order_relaxed);

        InsertBucket(bucket);
        ++bucket->Size;

        TGuard<TSpinLock> bucketGuard(bucket->SpinLock);
        guard.Release();

        Y_ASSERT(callback);

        TEnqueuedAction action;
        action.Finished = false;
        action.EnqueuedAt = GetCpuInstant();
        action.Callback = BIND(&TBucket::RunCallback, MakeStrong(bucket), std::move(callback));
        bucket->Queue.push(std::move(action));

        CallbackEventCount_->NotifyOne();
    }

    void RemoveBucket(TBucket* bucket)
    {
        TGuard<TSpinLock> guard(TagMappingSpinLock_);
        auto it = TagToBucket_.find(bucket->Tag);

        if (it != TagToBucket_.end() && it->second.IsExpired()) {
            TagToBucket_.erase(it);
        }

        Profiler_.Update(BucketCounter_, TagToBucket_.size());
    }

    virtual void Shutdown() override
    {
        Drain();
    }

    void Drain()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        for (const auto& item : Heap_) {
            item.Bucket->Drain();
        }
    }

    EBeginExecuteResult BeginExecute(TEnqueuedAction* action, int index)
    {
        auto& execution = CurrentlyExecutingActionsByThread_[index];

        Y_ASSERT(!execution.Bucket);

        auto bucket = GetStarvingBucket();

        if (!bucket) {
            return EBeginExecuteResult::QueueEmpty;
        }

        execution.Bucket = bucket;
        execution.AccountedAt = GetCpuInstant();

        Y_ASSERT(action && action->Finished);

        {
            TGuard<TSpinLock> guard(bucket->SpinLock);

            if (bucket->Queue.empty()) {
                return EBeginExecuteResult::QueueEmpty;
            }

            *action = std::move(bucket->Queue.front());
            bucket->Queue.pop();
        }

        CallbackEventCount_->CancelWait();

        action->StartedAt = GetCpuInstant();

        Profiler_.Update(
            WaitTimeCounter_,
            CpuDurationToValue(action->StartedAt - action->EnqueuedAt));

        // Move callback to the stack frame to ensure that we hold it as long as it runs.
        auto callback = std::move(action->Callback);
        try {
            TCurrentInvokerGuard guard(bucket);
            callback.Run();
            return EBeginExecuteResult::Success;
        } catch (const TFiberCanceledException&) {
            return EBeginExecuteResult::Terminated;
        }
    }

    void EndExecute(TEnqueuedAction* action, int index)
    {
        auto& execution = CurrentlyExecutingActionsByThread_[index];

        if (!execution.Bucket) {
            return;
        }

        Y_ASSERT(action);

        if (action->Finished) {
            return;
        }

        action->FinishedAt = GetCpuInstant();

        int queueSize = QueueSize_.fetch_sub(1, std::memory_order_relaxed) - 1;
        Profiler_.Update(SizeCounter_, queueSize);

        auto timeFromStart = CpuDurationToValue(action->FinishedAt - action->StartedAt);
        auto timeFromEnqueue = CpuDurationToValue(action->FinishedAt - action->EnqueuedAt);
        Profiler_.Update(ExecTimeCounter_, timeFromStart);
        Profiler_.Update(TotalTimeCounter_, timeFromEnqueue);

        action->Finished = true;

        auto duration = GetCpuInstant() - execution.AccountedAt;

        TBucketPtr bucket;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            UpdateExcessTime(execution.Bucket->HeapIterator, duration);
            bucket = std::move(execution.Bucket);
        }
    }

    void InsertBucket(TBucket* bucket)
    {
        if (bucket->HeapIterator) {
            return;
        }

        auto initialExcessTime = Heap_.empty() ? 0 : Heap_.front().ExcessTime;
        Heap_.emplace_back(initialExcessTime, bucket);
        AdjustHeapBack(Heap_.begin(), Heap_.end());
    }

private:
    struct TExecution
    {
        TCpuInstant AccountedAt = 0;
        TBucketPtr Bucket;
    };

    TSpinLock SpinLock_;

    std::vector<THeapItem> Heap_;
    std::shared_ptr<TEventCount> CallbackEventCount_;
    std::vector<TExecution> CurrentlyExecutingActionsByThread_;
    bool EnableLogging_;
    bool EnableProfiling_;

    TSpinLock TagMappingSpinLock_;
    yhash<TFairShareThreadPoolTag, TWeakPtr<TBucket>> TagToBucket_;

    std::atomic<int> QueueSize_ = {0};

    TProfiler Profiler_;
    TAggregateCounter BucketCounter_;
    TAggregateCounter SizeCounter_;
    TAggregateCounter WaitTimeCounter_;
    TAggregateCounter ExecTimeCounter_;
    TAggregateCounter TotalTimeCounter_;

    void AccountCurrentlyExecutingBuckets()
    {
        auto currentInstant = GetCpuInstant();
        for (auto& execution : CurrentlyExecutingActionsByThread_) {
            if (!execution.Bucket) {
                continue;
            }

            auto duration = currentInstant - execution.AccountedAt;
            execution.AccountedAt = currentInstant;

            UpdateExcessTime(execution.Bucket->HeapIterator, duration);
        }
    }

    void UpdateExcessTime(THeapItem* positionInHeap, TCpuDuration duration)
    {
        if (!positionInHeap) {
            return;
        }

        size_t indexInHeap = positionInHeap - Heap_.data();
        YCHECK(indexInHeap < Heap_.size());
        Heap_[indexInHeap].ExcessTime += duration;
        SiftDown(Heap_.begin(), Heap_.end(), Heap_.begin() + indexInHeap, std::less<>());
    }

    TBucketPtr GetStarvingBucket()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        // For each currently evaluating buckets recalculate excess time.
        AccountCurrentlyExecutingBuckets();

        while (!Heap_.empty()) {
            const auto& bucket = Heap_[0].Bucket;
            if (bucket->Size > 0) {
                --bucket->Size;
                return bucket;
            }
            ExtractHeap(Heap_.begin(), Heap_.end());
            Heap_.pop_back();
        }

        return nullptr;
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareQueue)

////////////////////////////////////////////////////////////////////////////////

void TBucket::Invoke(TClosure callback)
{
    if (auto parent = Parent.Lock()) {
        parent->Invoke(std::move(callback), this);
    }
}

TBucket::~TBucket()
{
    if (auto parent = Parent.Lock()) {
        parent->RemoveBucket(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TFairShareThread
    : public TSchedulerThread
{
public:
    TFairShareThread(
        TFairShareQueuePtr queue,
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        const TTagIdList& tagIds,
        bool enableLogging,
        bool enableProfiling,
        int index)
        : TSchedulerThread(
            std::move(callbackEventCount),
            threadName,
            tagIds,
            enableLogging,
            enableProfiling)
        , Queue_(std::move(queue))
        , Index_(index)
    { }

protected:
    const TFairShareQueuePtr Queue_;
    const int Index_;

    TEnqueuedAction CurrentAction;

    virtual EBeginExecuteResult BeginExecute() override
    {
        return Queue_->BeginExecute(&CurrentAction, Index_);
    }

    virtual void EndExecute() override
    {
        Queue_->EndExecute(&CurrentAction, Index_);
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareThread)

////////////////////////////////////////////////////////////////////////////////

class TFairShareThreadPool
    : public IFairShareThreadPool
{
public:
    TFairShareThreadPool(
        int threadCount,
        const TString& threadNamePrefix,
        bool enableLogging = false,
        bool enableProfiling = false)
        : Queue_(New<TFairShareQueue>(
            CallbackEventCount_,
            threadCount,
            GetThreadTagIds(enableProfiling, threadNamePrefix),
            enableLogging,
            enableProfiling))
    {
        YCHECK(threadCount > 0);

        for (int index = 0; index < threadCount; ++index) {
            auto thread = New<TFairShareThread>(
                Queue_,
                CallbackEventCount_,
                Format("%v:%v", threadNamePrefix, index),
                GetThreadTagIds(enableProfiling, threadNamePrefix),
                enableLogging,
                enableProfiling,
                index);

            Threads_.push_back(thread);
        }

        for (const auto& thread : Threads_) {
            thread->Start();
        }
    }

    IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag)
    {
        return Queue_->GetInvoker(tag);
    }

    virtual void Shutdown() override
    {
        bool expected = false;
        if (ShutdownFlag_.compare_exchange_strong(expected, true)) {
            DoShutdown();
        }
    }

    void DoShutdown()
    {
        Queue_->Shutdown();

        decltype(Threads_) threads;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            std::swap(threads, Threads_);
        }

        FinalizerInvoker_->Invoke(BIND([threads = std::move(threads), queue = Queue_] () {
            for (const auto& thread : threads) {
                thread->Shutdown();
            }
            queue->Drain();
        }));

        FinalizerInvoker_.Reset();
    }

    ~TFairShareThreadPool()
    {
        Shutdown();
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TFairShareQueuePtr Queue_;

    std::vector<TSchedulerThreadPtr> Threads_;
    std::atomic<bool> ShutdownFlag_ = {false};
    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();
    TSpinLock SpinLock_;

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IFairShareThreadPool);

IFairShareThreadPoolPtr CreateFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool enableLogging,
    bool enableProfiling)
{
    return New<TFairShareThreadPool>(
        threadCount,
        threadNamePrefix,
        enableLogging,
        enableProfiling);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


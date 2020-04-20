#include "fair_share_thread_pool.h"
#include "private.h"
#include "invoker_queue.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"

#include <yt/core/misc/heap.h>
#include <yt/core/misc/ring_queue.h>
#include <yt/core/misc/weak_ptr.h>

#include <yt/core/profiling/profiler.h>

#include <util/generic/xrange.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeapItem;
class TFairShareQueue;

DECLARE_REFCOUNTED_CLASS(TBucket)

class TBucket
    : public IInvoker
{
public:
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
        Queue.clear();
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
    THeapItem* HeapIterator = nullptr;
    i64 WaitTime = 0;

    TCpuDuration ExcessTime = 0;
    int CurrentExecutions = 0;
};

DEFINE_REFCOUNTED_TYPE(TBucket)

struct THeapItem
{
    TBucketPtr Bucket;

    THeapItem(const THeapItem&) = delete;
    THeapItem& operator=(const THeapItem&) = delete;

    explicit THeapItem(TBucketPtr bucket)
        : Bucket(std::move(bucket))
    {
        AdjustBackReference(this);
    }

    THeapItem(THeapItem&& other) noexcept
        : Bucket(std::move(other.Bucket))
    {
        AdjustBackReference(this);
    }

    THeapItem& operator=(THeapItem&& other) noexcept
    {
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
    return lhs.Bucket->ExcessTime < rhs.Bucket->ExcessTime;
}

////////////////////////////////////////////////////////////////////////////////

static constexpr auto LogDurationThreshold = TDuration::Seconds(1);

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
        bool enableProfiling)
        : CallbackEventCount_(std::move(callbackEventCount))
        , CurrentlyExecutingActionsByThread_(threadCount)
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

        if (!bucket->HeapIterator) {
            // Otherwise ExcessTime will be recalculated in AccountCurrentlyExecutingBuckets.
            if (bucket->CurrentExecutions == 0 && !Heap_.empty()) {
                bucket->ExcessTime = Heap_.front().Bucket->ExcessTime;
            }

            Heap_.emplace_back(bucket);
            AdjustHeapBack(Heap_.begin(), Heap_.end());
            YT_VERIFY(bucket->HeapIterator);
        }

        YT_ASSERT(callback);

        TEnqueuedAction action;
        action.Finished = false;
        action.EnqueuedAt = GetCpuInstant();
        action.Callback = BIND(&TBucket::RunCallback, MakeStrong(bucket), std::move(callback));
        bucket->Queue.push(std::move(action));

        guard.Release();

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

    TClosure BeginExecute(TEnqueuedAction* action, int index)
    {
        auto& execution = CurrentlyExecutingActionsByThread_[index];

        YT_ASSERT(!execution.Bucket);

        YT_ASSERT(action && action->Finished);

        TBucketPtr bucket;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            bucket = GetStarvingBucket(action);

            if (!bucket) {
                return TClosure();
            }

            ++bucket->CurrentExecutions;

            execution.Bucket = bucket;
            execution.AccountedAt = GetCpuInstant();

            action->StartedAt = GetCpuInstant();
            bucket->WaitTime = action->StartedAt - action->EnqueuedAt;
        }

        YT_ASSERT(action && !action->Finished);

        Profiler_.Update(
            WaitTimeCounter_,
            CpuDurationToValue(bucket->WaitTime));

        return std::move(action->Callback);
    }

    void EndExecute(TEnqueuedAction* action, int index)
    {
        auto& execution = CurrentlyExecutingActionsByThread_[index];

        if (!execution.Bucket) {
            return;
        }

        YT_ASSERT(action);

        if (action->Finished) {
            return;
        }

        action->FinishedAt = GetCpuInstant();

        int queueSize = QueueSize_.fetch_sub(1, std::memory_order_relaxed) - 1;
        Profiler_.Update(SizeCounter_, queueSize);

        auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
        auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);
        Profiler_.Update(ExecTimeCounter_, DurationToValue(timeFromStart));
        Profiler_.Update(TotalTimeCounter_, DurationToValue(timeFromEnqueue));

        if (timeFromStart > LogDurationThreshold) {
            YT_LOG_DEBUG("Callback execution took too long (Wait: %v, Execution: %v, Total: %v)",
                CpuDurationToDuration(action->StartedAt - action->EnqueuedAt),
                timeFromStart,
                timeFromEnqueue);
        }

        auto waitTime = CpuDurationToDuration(action->StartedAt - action->EnqueuedAt);

        if (waitTime > LogDurationThreshold) {
            YT_LOG_DEBUG("Callback wait took too long (Wait: %v, Execution: %v, Total: %v)",
                waitTime,
                timeFromStart,
                timeFromEnqueue);
        }

        action->Finished = true;

        // Remove outside lock because of lock inside RemoveBucket.
        TBucketPtr bucket;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            bucket = std::move(execution.Bucket);

            UpdateExcessTime(bucket.Get(), GetCpuInstant() - execution.AccountedAt);

            YT_VERIFY(bucket->CurrentExecutions-- > 0);
        }
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

    TSpinLock TagMappingSpinLock_;
    THashMap<TFairShareThreadPoolTag, TWeakPtr<TBucket>> TagToBucket_;

    std::atomic<int> QueueSize_ = {0};

    TProfiler Profiler_;
    TAggregateGauge BucketCounter_;
    TAggregateGauge SizeCounter_;
    TAggregateGauge WaitTimeCounter_;
    TAggregateGauge ExecTimeCounter_;
    TAggregateGauge TotalTimeCounter_;

    void AccountCurrentlyExecutingBuckets()
    {
        auto currentInstant = GetCpuInstant();
        for (auto& execution : CurrentlyExecutingActionsByThread_) {
            if (!execution.Bucket) {
                continue;
            }

            auto duration = currentInstant - execution.AccountedAt;
            execution.AccountedAt = currentInstant;

            UpdateExcessTime(execution.Bucket.Get(), duration);
        }
    }

    void UpdateExcessTime(TBucket* bucket, TCpuDuration duration)
    {
        bucket->ExcessTime += duration;

        auto positionInHeap = bucket->HeapIterator;
        if (!positionInHeap) {
            return;
        }

        size_t indexInHeap = positionInHeap - Heap_.data();
        YT_VERIFY(indexInHeap < Heap_.size());
        SiftDown(Heap_.begin(), Heap_.end(), Heap_.begin() + indexInHeap, std::less<>());
    }

    TBucketPtr GetStarvingBucket(TEnqueuedAction* action)
    {
        // For each currently evaluating buckets recalculate excess time.
        AccountCurrentlyExecutingBuckets();

        #ifdef YT_ENABLE_TRACE_LOGGING
        {
            TGuard<TSpinLock> guard(TagMappingSpinLock_);
            YT_LOG_TRACE("Buckets: [%v]",
                MakeFormattableView(
                    TagToBucket_,
                    [] (auto* builder, const auto& tagToBucket) {
                        if (auto item = tagToBucket.second.Lock()) {
                            auto excess = CpuDurationToDuration(tagToBucket.second.Lock()->ExcessTime).MilliSeconds();
                            builder->AppendFormat("(%v %v)", tagToBucket.first, excess);
                        } else {
                            builder->AppendFormat("(%v *)", tagToBucket.first);
                        }
                    }));
        }
        #endif

        if (Heap_.empty()) {
            return nullptr;
        }

        auto bucket = Heap_.front().Bucket;
        YT_VERIFY(!bucket->Queue.empty());
        *action = std::move(bucket->Queue.front());
        bucket->Queue.pop();

        if (bucket->Queue.empty()) {
            ExtractHeap(Heap_.begin(), Heap_.end());
            Heap_.pop_back();
        }

        return bucket;
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

    virtual TClosure BeginExecute() override
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
            enableProfiling))
        , ThreadCount_(threadCount)
    {
        YT_VERIFY(threadCount > 0);

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

    virtual IInvokerPtr GetInvoker(const TFairShareThreadPoolTag& tag) override
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

    virtual void Configure(int threadCount) override
    {
        if (threadCount != ThreadCount_) {
            // TODO(max42): fix me.
            YT_LOG_WARNING(
                "Fair share thread pool does not support thread count runtime configuration "
                "(CurrentThreadCount: %v, NewThreadCount: %v)",
                ThreadCount_,
                threadCount);
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
    int ThreadCount_;

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

} // namespace NYT::NConcurrency


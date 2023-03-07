#include "two_level_fair_share_thread_pool.h"
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
class TTwoLevelFairShareQueue;

DECLARE_REFCOUNTED_STRUCT(TBucket)

struct TBucket
    : public IInvoker
{
    TBucket(size_t poolId, TFairShareThreadPoolTag tag, TWeakPtr<TTwoLevelFairShareQueue> parent)
        : PoolId(poolId)
        , Tag(std::move(tag))
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

    const size_t PoolId;
    const TFairShareThreadPoolTag Tag;
    TWeakPtr<TTwoLevelFairShareQueue> Parent;
    TRingQueue<TEnqueuedAction> Queue;
    THeapItem* HeapIterator = nullptr;
    NProfiling::TCpuDuration WaitTime = 0;

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

DECLARE_REFCOUNTED_TYPE(TTwoLevelFairShareQueue)

class TTwoLevelFairShareQueue
    : public TRefCounted
    , public IShutdownable
{
public:
    TTwoLevelFairShareQueue(
        std::shared_ptr<TEventCount> callbackEventCount,
        int threadCount,
        const TString& threadNamePrefix,
        bool enableProfiling)
        : CallbackEventCount_(std::move(callbackEventCount))
        , CurrentlyExecutingActionsByThread_(threadCount)
        , Profiler_("/fair_share_queue")
        , ThreadNamePrefix_(threadNamePrefix)
    {
        Profiler_.SetEnabled(enableProfiling);
    }

    ~TTwoLevelFairShareQueue()
    {
        Shutdown();
    }

    size_t GetLowestEmptyPoolId()
    {
        size_t id = 0;
        while (id < Pools_.size() && !Pools_[id].TagToBucket.empty()) {
            ++id;
        }
        return id;
    }

    IInvokerPtr GetInvoker(const TString& poolName, double weight, const TFairShareThreadPoolTag& tag)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto it = NameToPoolId_.emplace(poolName, GetLowestEmptyPoolId());
        if (it.first->second >= Pools_.size()) {
            Pools_.emplace_back();
        }

        size_t poolId = it.first->second;
        auto& pool = Pools_[poolId];

        if (it.second) {
            pool.SetTagIds(GetBucketTagIds(Profiler_.GetEnabled(), ThreadNamePrefix_, poolName));
            pool.PoolName = poolName;
        }

        pool.Weight = weight;

        auto inserted = pool.TagToBucket.emplace(tag, nullptr).first;
        auto invoker = inserted->second.Lock();

        if (!invoker) {
            invoker = New<TBucket>(poolId, tag, MakeWeak(this));
            inserted->second = invoker;
        }

        Profiler_.Update(pool.BucketCounter, pool.TagToBucket.size());

        return invoker;
    }

    void Invoke(TClosure callback, TBucket* bucket)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto& pool = Pools_[bucket->PoolId];

        pool.QueueSize.fetch_add(1, std::memory_order_relaxed);

        if (!bucket->HeapIterator) {
            // Otherwise ExcessTime will be recalculated in AccountCurrentlyExecutingBuckets.
            if (bucket->CurrentExecutions == 0 && !pool.Heap.empty()) {
                bucket->ExcessTime = pool.Heap.front().Bucket->ExcessTime;
            }

            pool.Heap.emplace_back(bucket);
            AdjustHeapBack(pool.Heap.begin(), pool.Heap.end());
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
        TGuard<TSpinLock> guard(SpinLock_);

        auto& pool = Pools_[bucket->PoolId];
        auto it = pool.TagToBucket.find(bucket->Tag);
        if (it != pool.TagToBucket.end() && it->second.IsExpired()) {
            pool.TagToBucket.erase(it);
        }

        if (pool.TagToBucket.empty()) {
            YT_VERIFY(NameToPoolId_.erase(pool.PoolName));
        }

        Profiler_.Update(pool.BucketCounter, pool.TagToBucket.size());
    }

    virtual void Shutdown() override
    {
        Drain();
    }

    void Drain()
    {
        TGuard<TSpinLock> guard(SpinLock_);

        for (const auto& pool : Pools_) {
            for (const auto& item : pool.Heap) {
                item.Bucket->Drain();
            }
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

        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto& pool = Pools_[bucket->PoolId];

            Profiler_.Update(
                pool.WaitTimeCounter,
                CpuDurationToValue(bucket->WaitTime));
        }

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

        auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
        auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto& pool = Pools_[execution.Bucket->PoolId];

            int queueSize = pool.QueueSize.fetch_sub(1, std::memory_order_relaxed) - 1;
            Profiler_.Update(pool.SizeCounter, queueSize);
            Profiler_.Update(pool.ExecTimeCounter, DurationToValue(timeFromStart));
            Profiler_.Update(pool.TotalTimeCounter, DurationToValue(timeFromEnqueue));
        }

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
            execution.AccountedAt = GetCpuInstant();

            YT_VERIFY(bucket->CurrentExecutions-- > 0);
        }
    }

private:
    struct TExecution
    {
        TCpuInstant AccountedAt = 0;
        TBucketPtr Bucket;
    };

    struct TExecutionPool
    {
        TExecutionPool() = default;

        TExecutionPool(TExecutionPool&& other)
            : Weight(other.Weight)
            , PoolName(other.PoolName)
            , BucketCounter(other.BucketCounter)
            , SizeCounter(other.SizeCounter)
            , WaitTimeCounter(other.WaitTimeCounter)
            , ExecTimeCounter(other.ExecTimeCounter)
            , TotalTimeCounter(other.TotalTimeCounter)
            , ExcessTime(other.ExcessTime)
            , Heap(std::move(other.Heap))
            , QueueSize(other.QueueSize.load())
            , TagToBucket(std::move(other.TagToBucket))
        { }

        void SetTagIds(const TTagIdList& tagIds)
        {
            BucketCounter = TAggregateGauge("/buckets", tagIds);
            SizeCounter = TAggregateGauge("/size", tagIds);
            WaitTimeCounter = TAggregateGauge("/time/wait", tagIds);
            ExecTimeCounter = TAggregateGauge("/time/exec", tagIds);
            TotalTimeCounter = TAggregateGauge("/time/total", tagIds);
        }

        TBucketPtr GetStarvingBucket(TEnqueuedAction* action)
        {
            if (!Heap.empty()) {
                auto bucket = Heap.front().Bucket;
                YT_VERIFY(!bucket->Queue.empty());
                *action = std::move(bucket->Queue.front());
                bucket->Queue.pop();

                if (bucket->Queue.empty()) {
                    ExtractHeap(Heap.begin(), Heap.end());
                    Heap.pop_back();
                }

                return bucket;
            }

            return nullptr;
        }

        double Weight = 1.0;
        TString PoolName;

        TAggregateGauge BucketCounter;
        TAggregateGauge SizeCounter;
        TAggregateGauge WaitTimeCounter;
        TAggregateGauge ExecTimeCounter;
        TAggregateGauge TotalTimeCounter;

        TCpuDuration ExcessTime = 0;
        std::vector<THeapItem> Heap;
        std::atomic<int> QueueSize = {0};
        THashMap<TFairShareThreadPoolTag, TWeakPtr<TBucket>> TagToBucket;
    };

    TSpinLock SpinLock_;
    std::vector<TExecutionPool> Pools_;
    THashMap<TString, size_t> NameToPoolId_;

    std::shared_ptr<TEventCount> CallbackEventCount_;
    std::vector<TExecution> CurrentlyExecutingActionsByThread_;

    TProfiler Profiler_;
    TString ThreadNamePrefix_;

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
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        auto& pool = Pools_[bucket->PoolId];

        pool.ExcessTime += duration / pool.Weight;
        bucket->ExcessTime += duration;

        auto positionInHeap = bucket->HeapIterator;
        if (!positionInHeap) {
            return;
        }

        size_t indexInHeap = positionInHeap - pool.Heap.data();
        YT_VERIFY(indexInHeap < pool.Heap.size());
        SiftDown(pool.Heap.begin(), pool.Heap.end(), pool.Heap.begin() + indexInHeap, std::less<>());
    }

    TBucketPtr GetStarvingBucket(TEnqueuedAction* action)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        // For each currently evaluating buckets recalculate excess time.
        AccountCurrentlyExecutingBuckets();

        // Compute min excess over non-empty queues.
        auto minExcessTime = std::numeric_limits<NProfiling::TCpuDuration>::max();

        size_t minPoolIndex;
        for (size_t index = 0; index < Pools_.size(); ++index) {
            if (!Pools_[index].Heap.empty() && Pools_[index].ExcessTime < minExcessTime) {
                minExcessTime = Pools_[index].ExcessTime;
                minPoolIndex = index;
            }
        }

        YT_LOG_TRACE("Buckets: %v",
            MakeFormattableView(
                xrange(size_t(0), Pools_.size()),
                [&] (auto* builder, const auto index) {
                    builder->AppendFormat("[%v %v ", index, Pools_[index].ExcessTime);
                    for (const auto& tagToBucket : Pools_[index].TagToBucket) {
                        if (auto item = tagToBucket.second.Lock()) {
                            auto excess = CpuDurationToDuration(tagToBucket.second.Lock()->ExcessTime).MilliSeconds();
                            builder->AppendFormat("(%v %v) ", tagToBucket.first, excess);
                        } else {
                            builder->AppendFormat("(%v *) ", tagToBucket.first);
                        }
                    }
                    builder->AppendFormat("]");
                }));

        if (minExcessTime < std::numeric_limits<NProfiling::TCpuDuration>::max()) {
            // Reduce excesses (with truncation).
            auto delta = Pools_[minPoolIndex].ExcessTime;
            for (auto& pool : Pools_) {
                pool.ExcessTime = std::max<NProfiling::TCpuDuration>(pool.ExcessTime - delta, 0);
            }

            return Pools_[minPoolIndex].GetStarvingBucket(action);
        }

        return nullptr;
    }

};

DEFINE_REFCOUNTED_TYPE(TTwoLevelFairShareQueue)

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
        TTwoLevelFairShareQueuePtr queue,
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
    const TTwoLevelFairShareQueuePtr Queue_;
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

class TTwoLevelFairShareThreadPool
    : public ITwoLevelFairShareThreadPool
{
public:
    TTwoLevelFairShareThreadPool(
        int threadCount,
        const TString& threadNamePrefix,
        bool enableLogging = false,
        bool enableProfiling = false)
        : Queue_(New<TTwoLevelFairShareQueue>(
            CallbackEventCount_,
            threadCount,
            threadNamePrefix,
            enableProfiling))
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

    IInvokerPtr GetInvoker(
        const TString& poolName,
        double weight,
        const TFairShareThreadPoolTag& tag) override
    {
        return Queue_->GetInvoker(poolName, weight, tag);
    }

    virtual void Shutdown() override
    {
        bool expected = false;
        if (ShutdownFlag_.compare_exchange_strong(expected, true)) {
            DoShutdown();
        }
    }

    ~TTwoLevelFairShareThreadPool()
    {
        Shutdown();
    }

private:
    void DoShutdown()
    {
        Queue_->Shutdown();

        decltype(Threads_) threads;
        {
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

    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TTwoLevelFairShareQueuePtr Queue_;

    std::vector<TSchedulerThreadPtr> Threads_;
    std::atomic<bool> ShutdownFlag_ = {false};
    IInvokerPtr FinalizerInvoker_ = GetFinalizerInvoker();

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ITwoLevelFairShareThreadPoolPtr CreateTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool enableLogging,
    bool enableProfiling)
{
    return New<TTwoLevelFairShareThreadPool>(
        threadCount,
        threadNamePrefix,
        enableLogging,
        enableProfiling);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

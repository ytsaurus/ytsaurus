#include "two_level_fair_share_thread_pool.h"
#include "private.h"
#include "invoker_queue.h"
#include "profiling_helpers.h"
#include "scheduler_thread.h"

#include <yt/core/misc/heap.h>
#include <yt/core/misc/ring_queue.h>
#include <yt/core/misc/weak_ptr.h>

#include <yt/yt/library/profiling/sensor.h>

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
        , ThreadNamePrefix_(threadNamePrefix)
    {
        if (enableProfiling) {
            Profiler_ = TRegistry{"yt/fair_share_queue"};
        }
    }

    ~TTwoLevelFairShareQueue()
    {
        Shutdown();
    }

    size_t GetLowestEmptyPoolId()
    {
        size_t id = 0;
        while (id < IdToPool_.size() && IdToPool_[id]) {
            ++id;
        }
        return id;
    }

    IInvokerPtr GetInvoker(const TString& poolName, double weight, const TFairShareThreadPoolTag& tag)
    {
        TGuard<TAdaptiveLock> guard(SpinLock_);

        auto poolIt = NameToPoolId_.find(poolName);
        if (poolIt == NameToPoolId_.end()) {
            auto newPoolId = GetLowestEmptyPoolId();

            auto profiler = Profiler_.WithTags(GetBucketTags(true, ThreadNamePrefix_, poolName));
            auto newPool = std::make_unique<TExecutionPool>(poolName, profiler);
            if (newPoolId >= IdToPool_.size()) {
                IdToPool_.emplace_back();
            }
            IdToPool_[newPoolId] = std::move(newPool);
            poolIt = NameToPoolId_.emplace(poolName, newPoolId).first;
        }

        auto poolId = poolIt->second;
        const auto& pool = IdToPool_[poolId];

        pool->Weight = weight;
        auto [bucketIt, bucketInserted] = pool->TagToBucket.emplace(tag, nullptr);

        auto invoker = bucketIt->second.Lock();
        if (!invoker) {
            invoker = New<TBucket>(poolId, tag, MakeWeak(this));
            bucketIt->second = invoker;
        }

        pool->BucketCounter.Update(pool->TagToBucket.size());
        return invoker;
    }

    void Invoke(TClosure callback, TBucket* bucket)
    {
        TGuard<TAdaptiveLock> guard(SpinLock_);
        const auto& pool = IdToPool_[bucket->PoolId];

        pool->SizeCounter.Record(++pool->Size);

        if (!bucket->HeapIterator) {
            // Otherwise ExcessTime will be recalculated in AccountCurrentlyExecutingBuckets.
            if (bucket->CurrentExecutions == 0 && !pool->Heap.empty()) {
                bucket->ExcessTime = pool->Heap.front().Bucket->ExcessTime;
            }

            pool->Heap.emplace_back(bucket);
            AdjustHeapBack(pool->Heap.begin(), pool->Heap.end());
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
        TGuard<TAdaptiveLock> guard(SpinLock_);

        auto& pool = IdToPool_[bucket->PoolId];

        auto it = pool->TagToBucket.find(bucket->Tag);
        if (it != pool->TagToBucket.end() && it->second.IsExpired()) {
            pool->TagToBucket.erase(it);
        }

        pool->BucketCounter.Update(pool->TagToBucket.size());

        if (pool->TagToBucket.empty()) {
            YT_VERIFY(NameToPoolId_.erase(pool->PoolName) == 1);
            pool.reset();
        }
    }

    virtual void Shutdown() override
    {
        Drain();
    }

    void Drain()
    {
        TGuard<TAdaptiveLock> guard(SpinLock_);

        for (const auto& pool : IdToPool_) {
            if (pool) {
                for (const auto& item : pool->Heap) {
                    item.Bucket->Drain();
                }
            }
        }
    }

    TClosure BeginExecute(TEnqueuedAction* action, int index)
    {
        auto& execution = CurrentlyExecutingActionsByThread_[index];

        YT_ASSERT(!execution.Bucket);
        YT_ASSERT(action && action->Finished);

        auto tscp = NProfiling::TTscp::Get();

        TBucketPtr bucket;
        {
            TGuard<TAdaptiveLock> guard(SpinLock_);
            bucket = GetStarvingBucket(action);

            if (!bucket) {
                return TClosure();
            }

            ++bucket->CurrentExecutions;

            execution.Bucket = bucket;
            execution.AccountedAt = tscp.Instant;

            action->StartedAt = tscp.Instant;
            bucket->WaitTime = action->StartedAt - action->EnqueuedAt;
        }

        YT_ASSERT(action && !action->Finished);

        {
            TGuard<TAdaptiveLock> guard(SpinLock_);
            auto& pool = IdToPool_[bucket->PoolId];

            pool->WaitTimeCounter.Record(CpuDurationToDuration(bucket->WaitTime));
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

        auto tscp = NProfiling::TTscp::Get();

        action->FinishedAt = tscp.Instant;

        auto timeFromStart = CpuDurationToDuration(action->FinishedAt - action->StartedAt);
        auto timeFromEnqueue = CpuDurationToDuration(action->FinishedAt - action->EnqueuedAt);

        {
            TGuard<TAdaptiveLock> guard(SpinLock_);
            const auto& pool = IdToPool_[execution.Bucket->PoolId];
            pool->SizeCounter.Record(--pool->Size);
            pool->ExecTimeCounter.Record(timeFromStart);
            pool->TotalTimeCounter.Record(timeFromEnqueue);
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
            TGuard<TAdaptiveLock> guard(SpinLock_);
            bucket = std::move(execution.Bucket);

            UpdateExcessTime(bucket.Get(), tscp.Instant - execution.AccountedAt);
            execution.AccountedAt = tscp.Instant;

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
        TExecutionPool(const TString& poolName, const TRegistry& profiler)
            : PoolName(poolName)
            , BucketCounter(profiler.Gauge("/buckets"))
            , SizeCounter(profiler.Summary("/size"))
            , WaitTimeCounter(profiler.Timer("/time/wait"))
            , ExecTimeCounter(profiler.Timer("/time/exec"))
            , TotalTimeCounter(profiler.Timer("/time/total"))
        { }

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

        const TString PoolName;

        TGauge BucketCounter;
        std::atomic<i64> Size{0};
        NProfiling::TSummary SizeCounter;
        TEventTimer WaitTimeCounter;
        TEventTimer ExecTimeCounter;
        TEventTimer TotalTimeCounter;

        double Weight = 1.0;

        TCpuDuration ExcessTime = 0;
        std::vector<THeapItem> Heap;
        THashMap<TFairShareThreadPoolTag, TWeakPtr<TBucket>> TagToBucket;
    };

    TAdaptiveLock SpinLock_;
    std::vector<std::unique_ptr<TExecutionPool>> IdToPool_;
    THashMap<TString, int> NameToPoolId_;

    std::shared_ptr<TEventCount> CallbackEventCount_;
    std::vector<TExecution> CurrentlyExecutingActionsByThread_;

    TRegistry Profiler_;
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

        const auto& pool = IdToPool_[bucket->PoolId];

        pool->ExcessTime += duration / pool->Weight;
        bucket->ExcessTime += duration;

        auto positionInHeap = bucket->HeapIterator;
        if (!positionInHeap) {
            return;
        }

        size_t indexInHeap = positionInHeap - pool->Heap.data();
        YT_VERIFY(indexInHeap < pool->Heap.size());
        SiftDown(pool->Heap.begin(), pool->Heap.end(), pool->Heap.begin() + indexInHeap, std::less<>());
    }

    TBucketPtr GetStarvingBucket(TEnqueuedAction* action)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        // For each currently evaluating buckets recalculate excess time.
        AccountCurrentlyExecutingBuckets();

        // Compute min excess over non-empty queues.
        auto minExcessTime = std::numeric_limits<NProfiling::TCpuDuration>::max();

        int minPoolIndex = -1;
        for (int index = 0; index < static_cast<int>(IdToPool_.size()); ++index) {
            const auto& pool = IdToPool_[index];
            if (pool && !pool->Heap.empty() && pool->ExcessTime < minExcessTime) {
                minExcessTime = pool->ExcessTime;
                minPoolIndex = index;
            }
        }

        YT_LOG_TRACE("Buckets: %v",
            MakeFormattableView(
                xrange(size_t(0), IdToPool_.size()),
                [&] (auto* builder, auto index) {
                    const auto& pool = IdToPool_[index];
                    if (!pool) {
                        builder->AppendString("<null>");
                        return;
                    }
                    builder->AppendFormat("[%v %v ", index, pool->ExcessTime);
                    for (const auto& [tagId, weakBucket] : pool->TagToBucket) {
                        if (auto bucket = weakBucket.Lock()) {
                            auto excess = CpuDurationToDuration(bucket->ExcessTime).MilliSeconds();
                            builder->AppendFormat("(%v %v) ", tagId, excess);
                        } else {
                            builder->AppendFormat("(%v *) ", tagId);
                        }
                    }
                    builder->AppendFormat("]");
                }));

        if (minPoolIndex >= 0) {
            // Reduce excesses (with truncation).
            auto delta = IdToPool_[minPoolIndex]->ExcessTime;
            for (const auto& pool : IdToPool_) {
                if (pool) {
                    pool->ExcessTime = std::max<NProfiling::TCpuDuration>(pool->ExcessTime - delta, 0);
                }
            }
            return IdToPool_[minPoolIndex]->GetStarvingBucket(action);
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
        const TTagSet& tags,
        bool enableLogging,
        bool enableProfiling,
        int index)
        : TSchedulerThread(
            std::move(callbackEventCount),
            threadName,
            tags,
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
                GetThreadTags(enableProfiling, threadNamePrefix),
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

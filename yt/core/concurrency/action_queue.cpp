#include "stdafx.h"
#include "action_queue.h"
#include "action_queue_detail.h"

#include <core/actions/invoker_util.h>

#include <core/ypath/token.h>

#include <core/profiling/profiling_manager.h>

namespace NYT {
namespace NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

namespace {

TTagIdList GetThreadTagIds(const Stroka& threadName)
{
    TTagIdList tagIds;
    auto* profilingManager = TProfilingManager::Get();
    tagIds.push_back(profilingManager->RegisterTag("thread", threadName));
    return tagIds;
}

TTagIdList GetBucketTagIds(const Stroka& threadName, const Stroka& bucketName)
{
    TTagIdList tagIds;
    auto* profilingManager = TProfilingManager::Get();
    tagIds.push_back(profilingManager->RegisterTag("thread", threadName));
    tagIds.push_back(profilingManager->RegisterTag("bucket", bucketName));
    return tagIds;
}

} // namespace

///////////////////////////////////////////////////////////////////////////////

class TActionQueue::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(const Stroka& threadName)
        : Queue(New<TInvokerQueue>(
            &EventCount,
            nullptr,
            GetThreadTagIds(threadName),
            true,
            true))
        , Thread(New<TSingleQueueExecutorThread>(
            Queue,
            &EventCount,
            threadName,
            GetThreadTagIds(threadName),
            true,
            true))
    {
        Thread->Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        Queue->Shutdown();
        Thread->Shutdown();
    }

    IInvokerPtr GetInvoker()
    {
        return Queue;
    }

private:
    TEventCount EventCount;
    TInvokerQueuePtr Queue;
    TSingleQueueExecutorThreadPtr Thread;

};

TActionQueue::TActionQueue(const Stroka& threadName)
    : Impl(New<TImpl>(threadName))
{ }

TActionQueue::~TActionQueue()
{ }

void TActionQueue::Shutdown()
{
    return Impl->Shutdown();
}

IInvokerPtr TActionQueue::GetInvoker()
{
    return Impl->GetInvoker();
}

TCallback<TActionQueuePtr()> TActionQueue::CreateFactory(const Stroka& threadName)
{
    return BIND([=] () {
        return New<TActionQueue>(threadName);
    });
}

///////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue::TImpl
    : public TExecutorThread
{
public:
    TImpl(
        const Stroka& threadName,
        const std::vector<Stroka>& bucketNames)
        : TExecutorThread(
            &EventCount,
            threadName,
            GetThreadTagIds(threadName),
            true,
            true)
        , Buckets(bucketNames.size())
        , CurrentBucket(nullptr)
    {

        for (int index = 0; index < static_cast<int>(bucketNames.size()); ++index) {
            Buckets[index].Queue = New<TInvokerQueue>(
                &EventCount,
                nullptr,
                GetBucketTagIds(threadName, bucketNames[index]),
                true,
                true);
        }
        Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        FOREACH (auto& bucket, Buckets) {
            bucket.Queue->Shutdown();
        }
        TExecutorThread::Shutdown();
    }

    IInvokerPtr GetInvoker(int index)
    {
        YASSERT(0 <= index && index < static_cast<int>(Buckets.size()));
        return Buckets[index].Queue;
    }

private:
    TEventCount EventCount;

    struct TBucket
    {
        TBucket()
            : ExcessTime(0)
        { }

        TInvokerQueuePtr Queue;
        TCpuDuration ExcessTime;
    };

    std::vector<TBucket> Buckets;
    TCpuInstant StartInstant;

    TEnqueuedAction CurrentAction;
    TBucket* CurrentBucket;
    

    TBucket* GetStarvingBucket()
    {
        // Compute min excess over non-empty queues.
        i64 minExcess = std::numeric_limits<i64>::max();
        TBucket* minBucket = nullptr;
        FOREACH (auto& bucket, Buckets) {
            if (!bucket.Queue->IsEmpty()) {
                if (bucket.ExcessTime < minExcess) {
                    minExcess = bucket.ExcessTime;
                    minBucket = &bucket;
                }
            }
        }
        return minBucket;
    }

    virtual EBeginExecuteResult BeginExecute() override
    {
        YCHECK(!CurrentBucket);

        // Check if any action is ready at all.
        CurrentBucket = GetStarvingBucket();
        if (!CurrentBucket) {
            return EBeginExecuteResult::QueueEmpty;
        }

        // Reduce excesses (with truncation).
        FOREACH (auto& bucket, Buckets) {
            bucket.ExcessTime = std::max<i64>(0, bucket.ExcessTime - CurrentBucket->ExcessTime);
        }

        // Pump the starving queue.
        StartInstant = GetCpuInstant();
        return CurrentBucket->Queue->BeginExecute(&CurrentAction);
    }

    virtual void EndExecute() override
    {
        if (!CurrentBucket)
            return;

        CurrentBucket->Queue->EndExecute(&CurrentAction);
        CurrentBucket->ExcessTime += (GetCpuInstant() - StartInstant);
        CurrentBucket = nullptr;
    }

};

TFairShareActionQueue::TFairShareActionQueue(
    const Stroka& threadName,
    const std::vector<Stroka>& bucketNames)
    : Impl(New<TImpl>(threadName, bucketNames))
{ }

TFairShareActionQueue::~TFairShareActionQueue()
{ }

IInvokerPtr TFairShareActionQueue::GetInvoker(int index)
{
    return Impl->GetInvoker(index);
}

void TFairShareActionQueue::Shutdown()
{
    return Impl->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

class TThreadPool::TImpl
    : public TRefCounted
{
public:
    TImpl(int threadCount, const Stroka& threadNamePrefix)
        : Queue(New<TInvokerQueue>(
            &EventCount,
            nullptr,
            GetThreadTagIds(threadNamePrefix),
            true,
            true))
    {
        for (int i = 0; i < threadCount; ++i) {
            auto thread = New<TSingleQueueExecutorThread>(
                Queue,
                &EventCount,
                threadNamePrefix,
                GetThreadTagIds(threadNamePrefix),
                true,
                true);
            Threads.push_back(thread);
            thread->Start();
        }
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        Queue->Shutdown();
        FOREACH (auto thread, Threads) {
            thread->Shutdown();
        }
    }

    IInvokerPtr GetInvoker()
    {
        return Queue;
    }

private:
    TEventCount EventCount;
    TInvokerQueuePtr Queue;
    std::vector<TExecutorThreadPtr> Threads;

};

TThreadPool::TThreadPool(int threadCount, const Stroka& threadNamePrefix)
    : Impl(New<TImpl>(threadCount, threadNamePrefix))
{ }

TThreadPool::~TThreadPool()
{ }

void TThreadPool::Shutdown()
{
    return Impl->Shutdown();
}

IInvokerPtr TThreadPool::GetInvoker()
{
    return Impl->GetInvoker();
}

TCallback<TThreadPoolPtr()> TThreadPool::CreateFactory(int queueCount, const Stroka& threadName)
{
    return BIND([=] () {
        return NYT::New<NConcurrency::TThreadPool>(queueCount, threadName);
    });
}

///////////////////////////////////////////////////////////////////////////////

class TSerializedInvoker
    : public IInvoker
{
public:
    explicit TSerializedInvoker(IInvokerPtr underlyingInvoker)
        : UnderlyingInvoker(underlyingInvoker)
        , Lock(0)
    { }

    virtual bool Invoke(const TClosure& action) override
    {
        Queue.Enqueue(action);
        TrySchedule();
        return true;
    }

private:
    IInvokerPtr UnderlyingInvoker;
    TLockFreeQueue<TClosure> Queue;
    TAtomic Lock;

    void TrySchedule()
    {
        if (Queue.IsEmpty()) {
            return;
        }

        if (AtomicTryAndTryLock(&Lock)) {
            YCHECK(UnderlyingInvoker->Invoke(BIND(&TSerializedInvoker::DoInvoke, MakeStrong(this))));
        }
    }

    void DoInvoke()
    {
        // Execute as many actions as possible to minimize context switches.
        TClosure action;
        while (Queue.Dequeue(&action)) {
            TCurrentInvokerGuard guard(this);
            action.Run();
        }

        AtomicUnlock(&Lock);

        TrySchedule();
    }

};

IInvokerPtr CreateSerializedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TSerializedInvoker>(underlyingInvoker);
}

///////////////////////////////////////////////////////////////////////////////

class TPrioritizedInvoker
    : public IPrioritizedInvoker
{
public:
    explicit TPrioritizedInvoker(IInvokerPtr underlyingInvoker)
        : UnderlyingInvoker(underlyingInvoker)
    { }

    virtual bool Invoke(const TClosure& callback, i64 priority) override
    {
        {
            TGuard<TSpinLock> guard(SpinLock);
            TEntry entry;
            entry.Callback = callback;
            entry.Priority = priority;
            EntryHeap.emplace_back(std::move(entry));
            std::push_heap(EntryHeap.begin(), EntryHeap.end());
        }
        // TODO(babenko): there's no easy way to evict the entry; for now, we do not allow the
        // underlying invoker to reject the action.
        YCHECK(UnderlyingInvoker->Invoke(BIND(&TPrioritizedInvoker::DoExecute, MakeStrong(this))));
        return true;
    }

    virtual bool Invoke(const TClosure& callback) override
    {
        return UnderlyingInvoker->Invoke(callback);
    }

private:
    IInvokerPtr UnderlyingInvoker;

    struct TEntry
    {
        TClosure Callback;
        i64 Priority;

        bool operator < (const TEntry& other) const
        {
            return Priority < other.Priority;
        }
    };

    TSpinLock SpinLock;
    std::vector<TEntry> EntryHeap;

    void DoExecute()
    {
        TGuard<TSpinLock> guard(SpinLock);
        std::pop_heap(EntryHeap.begin(), EntryHeap.end());
        auto callback = std::move(EntryHeap.back().Callback);
        EntryHeap.pop_back();
        guard.Release();
        callback.Run();
    }

};

IPrioritizedInvokerPtr CreatePrioritizedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TPrioritizedInvoker>(underlyingInvoker);
}

///////////////////////////////////////////////////////////////////////////////

class TFakePrioritizedInvoker
    : public IPrioritizedInvoker
{
public:
    explicit TFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
        : UnderlyingInvoker(underlyingInvoker)
    { }

    virtual bool Invoke(const TClosure& callback, i64 /*priority*/) override
    {
        return UnderlyingInvoker->Invoke(callback);
    }

    virtual bool Invoke(const TClosure& callback) override
    {
        return UnderlyingInvoker->Invoke(callback);
    }

private:
    IInvokerPtr UnderlyingInvoker;

};

IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TFakePrioritizedInvoker>(underlyingInvoker);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

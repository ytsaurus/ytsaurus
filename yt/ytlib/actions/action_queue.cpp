#include "stdafx.h"
#include "action_queue.h"
#include "bind.h"
#include "action_queue_detail.h"

#include <ytlib/misc/foreach.h>

#include <ytlib/ypath/token.h>

namespace NYT {

using namespace NProfiling;
using namespace NYPath;

///////////////////////////////////////////////////////////////////////////////

TActionQueue::TActionQueue(const Stroka& threadName, bool enableLogging)
    : Impl(New<TExecutorThreadWithQueue>(
        nullptr,
        threadName,
        threadName,
        enableLogging))
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
    TImpl(const std::vector<Stroka>& profilingNames, const Stroka& threadName)
        : TExecutorThread(threadName, true)
        , Buckets(profilingNames.size())
        , CurrentBucket(nullptr)
    {
        for (int index = 0; index < static_cast<int>(profilingNames.size()); ++index) {
            Buckets[index].Queue = New<TInvokerQueue>(
                this,
                nullptr,
                "/" + ToYPathLiteral(threadName + ":" + profilingNames[index]),
                true);
        }

        Start();
    }

    ~TImpl()
    {
        FOREACH (auto& bucket, Buckets) {
            bucket.Queue->Shutdown();
        }

        Shutdown();
    }

    void Shutdown()
    {
        TExecutorThread::Shutdown();
    }

    IInvokerPtr GetInvoker(int queueIndex)
    {
        YASSERT(0 <= queueIndex && queueIndex < static_cast<int>(Buckets.size()));
        return Buckets[queueIndex].Queue;
    }

private:
    struct TBucket
    {
        TBucket()
            : ExcessTime(0)
        { }

        TInvokerQueuePtr Queue;
        TCpuDuration ExcessTime;
    };

    std::vector<TBucket> Buckets;
    TBucket* CurrentBucket;
    TCpuInstant StartInstant;

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
        return CurrentBucket->Queue->BeginExecute();
    }

    virtual void EndExecute() override
    {
        if (!CurrentBucket)
            return;

        CurrentBucket->Queue->EndExecute();
        auto endInstant = GetCpuInstant();
        CurrentBucket->ExcessTime += (endInstant - StartInstant);
        CurrentBucket = nullptr;
    }
};

TFairShareActionQueue::TFairShareActionQueue(
    const std::vector<Stroka>& profilingNames,
    const Stroka& threadName)
    : Impl(New<TImpl>(profilingNames, threadName))
{ }

TFairShareActionQueue::~TFairShareActionQueue()
{ }

IInvokerPtr TFairShareActionQueue::GetInvoker(int queueIndex)
{
    return Impl->GetInvoker(queueIndex);
}

void TFairShareActionQueue::Shutdown()
{
    return Impl->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

class TThreadPool::TImpl
    : public IInvoker
{
public:
    TImpl(int threadCount, const Stroka& threadNamePrefix)
    {
        for (int i = 0; i < threadCount; ++i) {
            Threads.push_back(New<TExecutorThreadWithQueue>(
                this,
                Sprintf("%s:%d", ~threadNamePrefix, i),
                threadNamePrefix,
                true));
        }
    }

    void Shutdown()
    {
        FOREACH (const auto& thread, Threads) {
            thread->Shutdown();
        }
    }

    IInvokerPtr GetInvoker()
    {
        // I am the invoker! :)
        return this;
    }

    virtual bool Invoke(const TClosure& action) override
    {
        // Pick a seemingly least-loaded thread in the pool.
        // Do not lock, just scan and choose the minimum.
        // This should be fast enough since threadCount is small.
        int minSize = std::numeric_limits<int>::max();
        TExecutorThreadWithQueuePtr minThread;
        FOREACH (const auto& thread, Threads) {
            int size = thread->GetSize();
            if (size < minSize) {
                minSize = size;
                minThread = thread;
            }
        }

        return minThread->GetInvoker()->Invoke(action);
    }

private:
    std::vector<TExecutorThreadWithQueuePtr> Threads;

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
        return NYT::New<NYT::TThreadPool>(queueCount, threadName);
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

    virtual bool Invoke(const TClosure& action, i64 priority) override
    {
        {
            TGuard<TSpinLock> guard(SpinLock);
            TEntry entry;
            entry.Action = action;
            entry.Priority = priority;
            EntryHeap.emplace_back(std::move(entry));
            std::push_heap(EntryHeap.begin(), EntryHeap.end());
        }
        // TODO(babenko): there's no easy way to evict the entry; for now, we do not allow the
        // underlying invoker to reject the action.
        YCHECK(UnderlyingInvoker->Invoke(BIND(&TPrioritizedInvoker::DoExecute, MakeStrong(this))));
        return true;
    }

    virtual bool Invoke(const TClosure& action) override
    {
        return UnderlyingInvoker->Invoke(action);
    }

private:
    IInvokerPtr UnderlyingInvoker;

    struct TEntry
    {
        TClosure Action;
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
        TClosure action;
        {
            TGuard<TSpinLock> guard(SpinLock);
            std::pop_heap(EntryHeap.begin(), EntryHeap.end());
            action = std::move(EntryHeap.back().Action);
            EntryHeap.pop_back();
        }
        action.Run();
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

    virtual bool Invoke(const TClosure& action, i64 /*priority*/) override
    {
        return UnderlyingInvoker->Invoke(action);
    }

    virtual bool Invoke(const TClosure& action) override
    {
        return UnderlyingInvoker->Invoke(action);
    }

private:
    IInvokerPtr UnderlyingInvoker;

};

IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(IInvokerPtr underlyingInvoker)
{
    return New<TFakePrioritizedInvoker>(underlyingInvoker);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

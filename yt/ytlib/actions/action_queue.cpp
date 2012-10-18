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

class TActionQueue::TImpl
    : public TActionQueueBase
{
public:
    TImpl(const Stroka& threadName, const Stroka& profilingName, bool enableLogging)
        : TActionQueueBase(threadName, enableLogging)
    {
        QueueInvoker = New<TQueueInvoker>(
            "/" + ToYPathLiteral(profilingName),
            this,
            enableLogging);
        Start();
    }

    ~TImpl()
    {
        QueueInvoker->Shutdown();
        Shutdown();
    }

    void Shutdown()
    {
        TActionQueueBase::Shutdown();
    }

    IInvokerPtr GetInvoker()
    {
        return QueueInvoker;
    }

    int GetSize() const
    {
        return QueueInvoker->GetSize();
    }

    static TCallback<TActionQueuePtr()> CreateFactory(const Stroka& threadName);

protected:
    virtual bool DequeueAndExecute() override
    {
        return QueueInvoker->DequeueAndExecute();
    }

private:
    TQueueInvokerPtr QueueInvoker;

};

TActionQueue::TActionQueue(const Stroka& threadName, bool enableLogging)
    : Impl(New<TImpl>(threadName, threadName, enableLogging))
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

int TActionQueue::GetSize() const
{
    return Impl->GetSize();
}

///////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue::TImpl
    : public TActionQueueBase
{
public:
    TImpl(const std::vector<Stroka>& profilingNames, const Stroka& threadName)
        : TActionQueueBase(threadName, true)
        , Queues(profilingNames.size())
    {
        for (int index = 0; index < static_cast<int>(profilingNames.size()); ++index) {
            Queues[index].Invoker = New<TQueueInvoker>(
                "/" + ToYPathLiteral(threadName + ":" + profilingNames[index]),
                this,
                true);
        }

        Start();
    }

    ~TImpl()
    {
        FOREACH (auto& queue, Queues) {
            queue.Invoker->Shutdown();
        }

        Shutdown();
    }

    void Shutdown()
    {
        TActionQueueBase::Shutdown();
    }

    IInvokerPtr GetInvoker(int queueIndex)
    {
        YASSERT(0 <= queueIndex && queueIndex < static_cast<int>(Queues.size()));
        return Queues[queueIndex].Invoker;
    }

private:
    struct TQueue
    {
        TQueue()
            : ExcessTime(0)
        { }

        TQueueInvokerPtr Invoker;
        TCpuDuration ExcessTime;
    };

    std::vector<TQueue> Queues;


    virtual bool DequeueAndExecute() override
    {
        // Compute min excess over non-empty queues.
        i64 minExcess = std::numeric_limits<i64>::max();
        int minQueueIndex = -1;
        for (int index = 0; index < static_cast<int>(Queues.size()); ++index) {
            const auto& queue = Queues[index];
            if (!queue.Invoker->IsEmpty()) {
                if (queue.ExcessTime < minExcess) {
                    minExcess = queue.ExcessTime;
                    minQueueIndex = index;
                }
            }
        }

        // Check if any action is ready at all.
        if (minQueueIndex < 0) {
            return false;
        }

        // Reduce excesses (with truncation).
        for (int index = 0; index < static_cast<int>(Queues.size()); ++index) {
            auto& queue = Queues[index];
            queue.ExcessTime = std::max<i64>(0, queue.ExcessTime - minExcess);
        }

        // Pump the min queue and update its excess.
        auto& minQueue = Queues[minQueueIndex];
        auto startTime = GetCpuInstant();
        YCHECK(minQueue.Invoker->DequeueAndExecute());
        auto endTime = GetCpuInstant();
        minQueue.ExcessTime += (endTime - startTime);

        return true;
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
            Threads.push_back(New<TActionQueue::TImpl>(
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
        TIntrusivePtr<TActionQueue::TImpl> minThread;
        FOREACH (const auto& thread, Threads) {
            int size = thread->GetSize();
            if (size < minSize) {
                minSize = size;
                minThread = thread;
            }
        }

        minThread->GetInvoker()->Invoke(action);
        return true;
    }

private:
    std::vector< TIntrusivePtr<TActionQueue::TImpl> > Threads;

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

class TPrioritizedActionQueue::TImpl
    : public TActionQueueBase
{
public:
    TImpl(const Stroka& threadName)
        : TActionQueueBase(threadName, true)
        , CurrentSequenceNumber(0)
    {
        Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        TActionQueueBase::Shutdown();
    }

    void Enqueue(const TClosure& action, i64 priority)
    {
        {
            TGuard<TSpinLock> guard(SpinLock);
            TItem item;
            item.Action = action;
            item.Priority = priority;
            item.SequenceNumber = CurrentSequenceNumber++;
            Items.push_back(item);
            std::push_heap(Items.begin(), Items.end());
        }

        Signal();
    }

private:
    struct TItem
    {
        TClosure Action;
        i64 Priority;
        i64 SequenceNumber;

        bool operator < (const TItem& other) const
        {
            if (this->Priority < other.Priority)
                return true;
            if (this->Priority > other.Priority)
                return false;
            return this->SequenceNumber > other.SequenceNumber;
        }
    };

    TSpinLock SpinLock;
    std::vector<TItem> Items;
    i64 CurrentSequenceNumber;

    virtual bool DequeueAndExecute() override
    {
        TClosure action;
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (Items.empty()) {
                return false;
            }

            action = MoveRV(Items.front().Action);
            std::pop_heap(Items.begin(), Items.end());
            Items.pop_back();
        }
        action.Run();
        return true;
    }
};

class TPrioritizedActionQueue::TInvoker
    : public IInvoker
{
public:
    TInvoker(TIntrusivePtr<TImpl> impl, i64 priority)
        : Impl(impl)
        , Priority(priority)
    { }

    virtual bool Invoke(const TClosure& action) override
    {
        Impl->Enqueue(action, Priority);
        return true;
    }

private:
    TIntrusivePtr<TImpl> Impl;
    i64 Priority;

};

TPrioritizedActionQueue::TPrioritizedActionQueue(const Stroka& threadName)
    : Impl(New<TImpl>(threadName))
{ }

TPrioritizedActionQueue::~TPrioritizedActionQueue()
{ }

IInvokerPtr TPrioritizedActionQueue::CreateInvoker(i64 priority)
{
    return New<TInvoker>(Impl, priority);
}

void TPrioritizedActionQueue::Shutdown()
{
    return Impl->Shutdown();
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

} // namespace NYT

#include "stdafx.h"
#include "action_queue.h"
#include "bind.h"
#include "action_queue_detail.h"

#include <ytlib/misc/foreach.h>

namespace NYT {

using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

class TActionQueue::TImpl
    : public TActionQueueBase
{
public:
    TImpl(const Stroka& threadName, bool enableLogging)
        : TActionQueueBase(threadName, enableLogging)
    {
        QueueInvoker = New<TQueueInvoker>(threadName, this, enableLogging);
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
    virtual bool DequeueAndExecute()
    {
        return QueueInvoker->DequeueAndExecute();
    }

private:
    TQueueInvokerPtr QueueInvoker;

};

TActionQueue::TActionQueue(const Stroka& threadName, bool enableLogging)
    : Impl(New<TImpl>(threadName, enableLogging))
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
    TImpl(int queueCount, const Stroka& threadName)
        : TActionQueueBase(threadName, true)
        , Queues(queueCount)
    {
        for (int index = 0; index < queueCount; ++index) {
            Queues[index].Invoker = New<TQueueInvoker>(
                threadName + "_" + ToString(index),
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


    bool DequeueAndExecute()
    {
        // Compute min excess over non-empty queues.
        i64 minExcess = std::numeric_limits<i64>::max();
        int minQueueIndex = -1;
        for (int index = 0; index < static_cast<int>(Queues.size()); ++index) {
            const auto& queue = Queues[index];
            if (queue.Invoker->GetSize() > 0) {
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
        YVERIFY(minQueue.Invoker->DequeueAndExecute());
        auto endTime = GetCpuInstant();
        minQueue.ExcessTime += (endTime - startTime);

        return true;
    }
};

TFairShareActionQueue::TFairShareActionQueue(int queueCount, const Stroka& threadName)
    : Impl(New<TImpl>(queueCount, threadName))
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

TCallback<TFairShareActionQueuePtr()> TFairShareActionQueue::CreateFactory(int queueCount, const Stroka& threadName)
{
    return BIND([=] () {
        return NYT::New<NYT::TFairShareActionQueue>(queueCount, threadName);
    });
}

///////////////////////////////////////////////////////////////////////////////

class TThreadPool::TImpl
    : public IInvoker
{
public:
    TImpl(int threadCount, const Stroka& threadName)
    {
        for (int i = 0; i < threadCount; ++i) {
            Threads.push_back(New<TActionQueue::TImpl>(
                Sprintf("%s:%d", ~threadName, i),
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

    virtual void Invoke(const TClosure& action) OVERRIDE
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
    }

private:
    std::vector< TIntrusivePtr<TActionQueue::TImpl> > Threads;

};

TThreadPool::TThreadPool(int threadCount, const Stroka& threadName)
    : Impl(New<TImpl>(threadCount, threadName))
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

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

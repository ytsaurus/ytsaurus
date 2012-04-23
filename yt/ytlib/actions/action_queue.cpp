#include "stdafx.h"
#include "action_queue.h"
#include "bind.h"
#include "callback.h"
#include "invoker.h"

#include <ytlib/misc/common.h>
#include <ytlib/logging/log.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/profiling/timing.h>

#include <util/thread/lfqueue.h>

namespace NYT {

using namespace NYTree;
using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(
    const Stroka& name,
    TActionQueueBase* owner,
    bool enableLogging)
    : Owner(owner)
    , EnableLogging(enableLogging)
    , Profiler("/action_queues/" + EscapeYPath(name))
    , EnqueueCounter("/enqueue_rate")
    , DequeueCounter("/dequeue_rate")
    , QueueSize(0)
    , QueueSizeCounter("/size")
    , WaitTimeCounter("/time/wait")
    , ExecTimeCounter("/time/exec")
    , TotalTimeCounter("/time/total")
{ }

void TQueueInvoker::Invoke(const TClosure& action)
{
    if (!Owner) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored (Action: %p)", action.GetHandle());
        return;
    }

    AtomicIncrement(QueueSize);
    Profiler.Increment(EnqueueCounter);

    TItem item;
    item.StartInstant = GetCpuInstant();
    item.Action = action;
    Queue.Enqueue(item);

    LOG_TRACE_IF(EnableLogging, "Action is enqueued (Action: %p)", action.GetHandle());

    Owner->Signal();
}

void TQueueInvoker::Shutdown()
{
    Owner = NULL;
}

bool TQueueInvoker::DequeueAndExecute()
{
    TItem item;
    if (!Queue.Dequeue(&item))
        return false;

    Profiler.Increment(DequeueCounter);

    auto startExecInstant = GetCpuInstant();
    Profiler.Aggregate(WaitTimeCounter, CpuDurationToValue(startExecInstant - item.StartInstant));

    auto size = AtomicDecrement(QueueSize);
    Profiler.Aggregate(QueueSizeCounter, size);

    auto action = item.Action;
    LOG_TRACE_IF(EnableLogging, "Action started (Action: %p)", action.GetHandle());
    action.Run();
    LOG_TRACE_IF(EnableLogging, "Action stopped (Action: %p)", action.GetHandle());

    auto endExecInstant = GetCpuInstant();
    Profiler.Aggregate(ExecTimeCounter, CpuDurationToValue(endExecInstant - startExecInstant));
    Profiler.Aggregate(TotalTimeCounter, CpuDurationToValue(endExecInstant - item.StartInstant));
        
    return true;
}

bool TQueueInvoker::IsEmpty() const
{
    return const_cast< TLockFreeQueue<TItem>& >(Queue).IsEmpty();
}

///////////////////////////////////////////////////////////////////////////////

TActionQueueBase::TActionQueueBase(const Stroka& threadName, bool enableLogging)
    : EnableLogging(enableLogging)
    , Running(true)
    , WakeupEvent(Event::rManual)
    , Thread(ThreadFunc, (void*) this)
    , ThreadName(threadName)
{ }

TActionQueueBase::~TActionQueueBase()
{
    // Derived classes must call Shutdown in dtor.
    YASSERT(!Running);
}

void TActionQueueBase::Start()
{
    Thread.Start();
}

void* TActionQueueBase::ThreadFunc(void* param)
{
    auto* queue = (TActionQueue*) param;
    queue->ThreadMain();
    return NULL;
}

void TActionQueueBase::ThreadMain()
{
    NThread::SetCurrentThreadName(~ThreadName);

    while (true) {
        if (!DequeueAndExecute()) {
            WakeupEvent.Reset();
            if (!DequeueAndExecute()) {
                if (!Running) {
                    break;
                }
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    }

    YASSERT(!DequeueAndExecute());
}

void TActionQueueBase::Shutdown()
{
    if (!Running) {
        return;
    }
    Running = false;
    WakeupEvent.Signal();
    Thread.Join();
}

void TActionQueueBase::OnIdle()
{ }

void TActionQueueBase::Signal()
{
    WakeupEvent.Signal();
}

bool TActionQueueBase::IsRunning() const
{
    return Running;
}

///////////////////////////////////////////////////////////////////////////////

TActionQueue::TActionQueue(const Stroka& threadName, bool enableLogging)
    : TActionQueueBase(threadName, enableLogging)
{
    QueueInvoker = New<TQueueInvoker>(threadName, this, enableLogging);
    Start();
}

TActionQueue::~TActionQueue()
{
    QueueInvoker->Shutdown();
    Shutdown();
}

bool TActionQueue::DequeueAndExecute()
{
    return QueueInvoker->DequeueAndExecute();
}

IInvoker* TActionQueue::GetInvoker()
{
    return ~QueueInvoker;
}

TCallback<TActionQueue::TPtr()> TActionQueue::CreateFactory(const Stroka& threadName)
{
    return BIND([=] ()
        {
            return NYT::New<NYT::TActionQueue>(threadName);
        });
}

void TActionQueue::Shutdown()
{
    TActionQueueBase::Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

class TMultiActionQueue::TImpl
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

    IInvoker* GetInvoker(int queueIndex)
    {
        YASSERT(0 <= queueIndex && queueIndex < static_cast<int>(Queues.size()));
        return ~Queues[queueIndex].Invoker;
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
        YVERIFY(minQueue.Invoker->DequeueAndExecute());
        auto endTime = GetCpuInstant();
        minQueue.ExcessTime += (endTime - startTime);

        return true;
    }
};

TMultiActionQueue::TMultiActionQueue(int queueCount, const Stroka& threadName)
    : Impl(New<TImpl>(queueCount, threadName))
{ }

TMultiActionQueue::~TMultiActionQueue()
{ }

IInvoker* TMultiActionQueue::GetInvoker(int queueIndex)
{
    return Impl->GetInvoker(queueIndex);
}


void TMultiActionQueue::Shutdown()
{
    return Impl->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

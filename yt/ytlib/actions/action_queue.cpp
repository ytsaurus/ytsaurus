#include "stdafx.h"
#include "action_queue.h"

#include <ytlib/logging/log.h>
#include <ytlib/misc/common.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/actions/action_util.h>

namespace NYT {

using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(TActionQueueBase* owner, bool enableLogging)
    : Owner(owner)
    , EnableLogging(enableLogging)
    , Profiler(CombineYPaths("action_queues", owner->ThreadName))
    , QueueSize(0)
{ }

void TQueueInvoker::Invoke(IAction::TPtr action)
{
    if (!Owner) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored (Action: %p)", ~action);
        return;
    }

    TItem item;
    item.Timer = Profiler.TimingStart("time");
    item.Action = action;
    Queue.Enqueue(item);

    LOG_TRACE_IF(EnableLogging, "Action is enqueued (Action: %p)", ~action);

    auto size = AtomicIncrement(QueueSize);
    Profiler.Enqueue("size", size);

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
    
    Profiler.TimingCheckpoint(item.Timer, "wait");

    auto size = AtomicDecrement(QueueSize);
    Profiler.Enqueue("size", size);

    auto action = item.Action;
    LOG_TRACE_IF(EnableLogging, "Action started (Action: %p)", ~action);
    action->Do();
    LOG_TRACE_IF(EnableLogging, "Action stopped (Action: %p)", ~action);
    Profiler.TimingCheckpoint(item.Timer, "exec");

    Profiler.TimingStop(item.Timer);

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
        //try {
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
        //} catch (const std::exception& ex) {
        //    LOG_FATAL("Unhandled exception in the action queue\n%s", ex.what());
        //}
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
    QueueInvoker = New<TQueueInvoker>(this, enableLogging);
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

IFunc<TActionQueue::TPtr>::TPtr TActionQueue::CreateFactory(const Stroka& threadName)
{
    return FromFunctor([=] ()
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
        FOREACH (auto& queue, Queues) {
            queue.Invoker = New<TQueueInvoker>(this, true);
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
        i64 ExcessTime;
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
        ui64 startTime = GetCycleCount();
        YVERIFY(minQueue.Invoker->DequeueAndExecute());
        ui64 endTime = GetCycleCount();
        minQueue.ExcessTime += (endTime - startTime);

        return true;
    }
};

TMultiActionQueue::TMultiActionQueue(int queueCount, const Stroka& threadName)
    : Impl(New<TImpl>(queueCount, threadName))
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

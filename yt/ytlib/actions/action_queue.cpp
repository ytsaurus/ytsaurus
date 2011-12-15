#include "stdafx.h"
#include "action_queue.h"

#include "../logging/log.h"
#include "../misc/common.h"
#include "../misc/thread.h"
#include "../monitoring/stat.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(TActionQueueBase* owner)
    : Owner(owner)
    , QueueSize(0)
{ }

void TQueueInvoker::Invoke(IAction::TPtr action)
{
    if (Owner->Finished) {
        LOG_TRACE_IF(Owner->EnableLogging, "Queue had been shut down, incoming action ignored (Action: %p)", ~action);
    } else {
        LOG_TRACE_IF(Owner->EnableLogging, "Action is enqueued (Action: %p)", ~action);
        Queue.Enqueue(MakePair(action, GetCycleCount()));
        auto size = AtomicIncrement(QueueSize);
        DATA_POINT("actionqueue.size", "smv", size);
        Owner->WakeupEvent.Signal();
    }
}

void TQueueInvoker::Shutdown()
{
    Owner.Reset();
}

bool TQueueInvoker::DequeueAndExecute()
{
    TItem item;
    if (!Queue.Dequeue(&item))
        return false;
    
    auto size = AtomicDecrement(QueueSize);
    DATA_POINT("actionqueue.size", "smv", size);

    auto startTime = item.Second();
    DATA_POINT("actionqueue.waitingtime", "tv", GetCycleCount() - startTime);

    IAction::TPtr action = item.First();
    
    LOG_TRACE_IF(Owner->EnableLogging, "Action started (Action: %p)", ~action);
    TIMEIT("actionqueue.executiontime", "tv",
        action->Do();
    )
    LOG_TRACE_IF(Owner->EnableLogging, "Action stopped (Action: %p)", ~action);

    return true;
}

///////////////////////////////////////////////////////////////////////////////

TActionQueueBase::TActionQueueBase(const Stroka& threadName, bool enableLogging)
    : EnableLogging(enableLogging)
    , Finished(false)
    , WakeupEvent(Event::rManual)
    , Thread(ThreadFunc, (void*) this)
    , ThreadName(threadName)
{ }

TActionQueueBase::~TActionQueueBase()
{
    Shutdown();
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
    try {
        NThread::SetCurrentThreadName(~ThreadName);
        while (!Finished) {
            if (!DequeueAndExecute()) {
                WakeupEvent.Reset();
                if (!DequeueAndExecute()) {
                    OnIdle();
                    WakeupEvent.Wait();
                    if (Finished) {
                        break;
                    }
                }
            }
        }
    } catch (...) {
        LOG_FATAL("Unhandled exception in the action queue\n%s", ~CurrentExceptionMessage());
    }

    OnShutdown();
}

void TActionQueueBase::Shutdown()
{
    if (Finished)
        return;

    Finished = true;
    WakeupEvent.Signal();
    Thread.Join();
}

///////////////////////////////////////////////////////////////////////////////

TActionQueue::TActionQueue(const Stroka& threadName, bool enableLogging)
    : TActionQueueBase(threadName, enableLogging)
{
    QueueInvoker = New<TQueueInvoker>(this);
    Start();
}

bool TActionQueue::DequeueAndExecute()
{
    return QueueInvoker->DequeueAndExecute();
}

void TActionQueue::OnIdle()
{ }

void TActionQueue::OnShutdown()
{
    QueueInvoker->Shutdown();
}

IInvoker::TPtr TActionQueue::GetInvoker()
{
    return QueueInvoker;
}

IFunc<TActionQueue::TPtr>::TPtr TActionQueue::CreateFactory(const Stroka& threadName)
{
    return FromFunctor([=] ()
        {
            return NYT::New<NYT::TActionQueue>(threadName);
        });
}

///////////////////////////////////////////////////////////////////////////////

TPrioritizedActionQueue::TPrioritizedActionQueue(int priorityCount, const Stroka& threadName)
    : TActionQueueBase(threadName, true)
    , QueueInvokers(priorityCount)
{
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        QueueInvokers[priority] = New<TQueueInvoker>(this);
    }
    Start();
}

TPrioritizedActionQueue::~TPrioritizedActionQueue()
{
    Shutdown();
}

IInvoker::TPtr TPrioritizedActionQueue::GetInvoker(int priority)
{
    YASSERT(0 <= priority && priority < static_cast<int>(QueueInvokers.size()));
    return QueueInvokers[priority];
}

bool TPrioritizedActionQueue::DequeueAndExecute()
{
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        if (QueueInvokers[priority]->DequeueAndExecute()) {
            return true;
        }
    }
    return false;
}

void TPrioritizedActionQueue::OnIdle()
{ }

void TPrioritizedActionQueue::OnShutdown()
{
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        QueueInvokers[priority]->Shutdown();
    }
}

///////////////////////////////////////////////////////////////////////////////

}

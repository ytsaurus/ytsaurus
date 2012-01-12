#include "stdafx.h"
#include "action_queue.h"

#include <ytlib/logging/log.h>
#include <ytlib/misc/common.h>
#include <ytlib/misc/thread.h>
#include <ytlib/monitoring/stat.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(TActionQueueBase* owner, bool enableLogging)
    : EnableLogging(enableLogging)
    , Owner(owner)
    , QueueSize(0)
{ }

void TQueueInvoker::Invoke(IAction::TPtr action)
{
    if (!Owner) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored (Action: %p)", ~action);
    } else {
        LOG_TRACE_IF(EnableLogging, "Action is enqueued (Action: %p)", ~action);
        Queue.Enqueue(MakePair(action, GetCycleCount()));
        auto size = AtomicIncrement(QueueSize);
        DATA_POINT("actionqueue.size", "smv", size);
        Owner->WakeupEvent.Signal();
    }
}

void TQueueInvoker::OnShutdown()
{
    Owner = 0;
}

bool TQueueInvoker::OnDequeueAndExecute()
{
    TItem item;
    if (!Queue.Dequeue(&item))
        return false;
    
    auto size = AtomicDecrement(QueueSize);
    DATA_POINT("actionqueue.size", "smv", size);

    auto startTime = item.Second();
    DATA_POINT("actionqueue.waitingtime", "tv", GetCycleCount() - startTime);

    IAction::TPtr action = item.First();
    
    LOG_TRACE_IF(EnableLogging, "Action started (Action: %p)", ~action);
    TIMEIT("actionqueue.executiontime", "tv",
        action->Do();
    )
    LOG_TRACE_IF(EnableLogging, "Action stopped (Action: %p)", ~action);

    return true;
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
    NThread::SetCurrentThreadName(~ThreadName);

    while (Running) {
        try {
            if (!DequeueAndExecute()) {
                WakeupEvent.Reset();
                if (!DequeueAndExecute()) {
                    TIMEIT("actionqueue.idletime", "tv",
                        OnIdle();
                    )
                    if (Running) {
                        WakeupEvent.Wait();
                    }
                }
            }
        } catch (...) {
            LOG_FATAL("Unhandled exception in the action queue\n%s", ~CurrentExceptionMessage());
        }
    }
}

void TActionQueueBase::Shutdown()
{
    if (!Running)
        return;

    Running = false;
    WakeupEvent.Signal();
    Thread.Join();
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
    QueueInvoker->OnShutdown();
}

bool TActionQueue::DequeueAndExecute()
{
    return QueueInvoker->OnDequeueAndExecute();
}

void TActionQueue::OnIdle()
{ }

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
        QueueInvokers[priority] = New<TQueueInvoker>(this, true);
    }

    Start();
}

TPrioritizedActionQueue::~TPrioritizedActionQueue()
{
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        QueueInvokers[priority]->OnShutdown();
    }
}

IInvoker::TPtr TPrioritizedActionQueue::GetInvoker(int priority)
{
    YASSERT(0 <= priority && priority < static_cast<int>(QueueInvokers.size()));
    return QueueInvokers[priority];
}

bool TPrioritizedActionQueue::DequeueAndExecute()
{
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        if (QueueInvokers[priority]->OnDequeueAndExecute()) {
            return true;
        }
    }
    return false;
}

void TPrioritizedActionQueue::OnIdle()
{ }

///////////////////////////////////////////////////////////////////////////////

}

#include "stdafx.h"
#include "action_queue.h"

#include "../logging/log.h"
#include "../misc/common.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

// TODO: make this configurable
static TDuration SleepQuantum = TDuration::MilliSeconds(100);

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(TActionQueueBase* owner)
    : Owner(owner)
{ }

void TQueueInvoker::Invoke(IAction::TPtr action)
{
    if (Owner->Finished) {
        LOG_TRACE_IF(Owner->EnableLogging, "Queue had been shut down, incoming action ignored (Action: %p)", ~action);
    } else {
        LOG_TRACE_IF(Owner->EnableLogging, "Action is enqueued (Action: %p)", ~action);
        Queue.Enqueue(action);
        Owner->WakeupEvent.Signal();
    }
}

void TQueueInvoker::Shutdown()
{
    Owner.Reset();
}

bool TQueueInvoker::DequeueAndExecute()
{
    IAction::TPtr action;
    if (!Queue.Dequeue(&action))
        return false;

    LOG_TRACE_IF(Owner->EnableLogging, "Running action (Action: %p)", ~action);
    action->Do();

    return true;
}

///////////////////////////////////////////////////////////////////////////////

TActionQueueBase::TActionQueueBase(bool enableLogging)
    : EnableLogging(enableLogging)
    , Finished(false)
    , WakeupEvent(Event::rManual)
    , Thread(ThreadFunc, (void*) this)
{
    Thread.Start();
}

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
        while (!Finished) {
            if (!DequeueAndExecute()) {
                WakeupEvent.Reset();
                if (!DequeueAndExecute()) {
                    OnIdle();
                    WakeupEvent.WaitT(SleepQuantum);
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

TActionQueue::TActionQueue(bool enableLogging)
    : TActionQueueBase(enableLogging)
{
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

///////////////////////////////////////////////////////////////////////////////

TPrioritizedActionQueue::TPrioritizedActionQueue(int priorityCount)
    : TActionQueueBase(true)
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

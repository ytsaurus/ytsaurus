#include "stdafx.h"
#include "action_queue.h"

#include "../logging/log.h"
#include "../misc/common.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

class TQueueInvoker
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TQueueInvoker> TPtr;

    typedef TLockFreeQueue<IAction::TPtr> TQueue;

    TQueueInvoker(TActionQueueBase::TPtr owner)
        : Owner(owner)
    { }

    void Invoke(IAction::TPtr action)
    {
        if (Owner->IsFinished) {
            LOG_TRACE_IF(Owner->EnableLogging, "Queue had been shut down, incoming action %p is ignored",
                ~action);
        } else {
            LOG_TRACE_IF(Owner->EnableLogging, "Enqueued action %p",
                ~action);
            Queue.Enqueue(action);
            Owner->WakeupEvent.Signal();
        }
    }

    void Shutdown()
    {
        Owner.Drop();
    }

    bool ProcessQueue()
    {
        IAction::TPtr action;
        if (!Queue.Dequeue(&action))
            return false;

        LOG_TRACE_IF(Owner->EnableLogging, "Running action %p",
            ~action);
        action->Do();

        return true;
    }

private:
    TQueue Queue;
    TActionQueueBase::TPtr Owner;

};

///////////////////////////////////////////////////////////////////////////////

TActionQueueBase::TActionQueueBase(bool enableLogging)
    : EnableLogging(enableLogging)
    , IsFinished(false)
    , WakeupEvent(Event::rAuto)
{ }

///////////////////////////////////////////////////////////////////////////////

TActionQueue::TActionQueue(bool enableLogging)
    : TActionQueueBase(enableLogging)
    , Thread(ThreadFunc, (void*) this)
    , QueueInvoker(New<TQueueInvoker>(this))
{
    Thread.Start();
}

TActionQueue::~TActionQueue()
{
    Shutdown();
}

void* TActionQueue::ThreadFunc(void* param)
{
    auto* queue = (TActionQueue*) param;
    queue->ThreadMain();
    return NULL;
}

void TActionQueue::Shutdown()
{
    if (IsFinished)
        return;

    IsFinished = true;
    WakeupEvent.Signal();
    Thread.Join();
}

void TActionQueue::ThreadMain()
{
    try {
        while (true) {
            if (!QueueInvoker->ProcessQueue()) {
                if (IsFinished)
                    break;
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    } catch (...) {
        LOG_FATAL("Unhandled exception in the action queue: %s",
            ~CurrentExceptionMessage());
    }

    QueueInvoker->Shutdown();
}

void TActionQueue::OnIdle()
{ }

IInvoker::TPtr TActionQueue::GetInvoker()
{
    return ~QueueInvoker;
}

///////////////////////////////////////////////////////////////////////////////

TPrioritizedActionQueue::TPrioritizedActionQueue(int priorityCount)
    : TActionQueueBase(true)
    , QueueInvokers(priorityCount)
    , Thread(ThreadFunc, (void*) this)
{
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        QueueInvokers[priority] = New<TQueueInvoker>(this);
    }

    Thread.Start();
}

TPrioritizedActionQueue::~TPrioritizedActionQueue()
{
    Shutdown();
}

void* TPrioritizedActionQueue::ThreadFunc(void* param)
{
    auto* queue = (TPrioritizedActionQueue*) param;
    queue->ThreadMain();
    return NULL;
}

IInvoker::TPtr TPrioritizedActionQueue::GetInvoker(int priority)
{
    YASSERT(0 <= priority && priority < static_cast<int>(QueueInvokers.size()));
    return ~QueueInvokers[priority];
}

void TPrioritizedActionQueue::Shutdown()
{
    if (IsFinished)
        return;

    IsFinished = true;
    WakeupEvent.Signal();
    Thread.Join();
}

void TPrioritizedActionQueue::ThreadMain()
{
    try {
        while (true) {
            bool anyLuck = false;
            for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
                if (QueueInvokers[priority]->ProcessQueue()) {
                    anyLuck = true;
                    break;
                }
            }

            if (!anyLuck) {
                if (IsFinished)
                    break;
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    } catch (...) {
        LOG_FATAL("Unhandled exception in the action queue: %s",
            ~CurrentExceptionMessage());
    }

    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        QueueInvokers[priority]->Shutdown();
    }
}

void TPrioritizedActionQueue::OnIdle()
{ }

///////////////////////////////////////////////////////////////////////////////


}

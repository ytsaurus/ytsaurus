#include "action_queue.h"

#include "../logging/log.h"
#include "../misc/ptr.h"

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
            LOG_DEBUG_IF(Owner->EnableLogging, "Queue has been shut down, incoming action %p is ignored", ~action);
        } else {
            LOG_DEBUG_IF(Owner->EnableLogging, "Enqueued action %p", ~action);
            Queue.Enqueue(action);
            Owner->WakeupEvent.Signal();
        }
    }

    bool ProcessQueue()
    {
        IAction::TPtr action;
        if (!Queue.Dequeue(&action))
            return false;

        LOG_DEBUG_IF(Owner->EnableLogging, "Running action %p",
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
    QueueInvoker.Drop();
    WakeupEvent.Signal();
    Thread.Join();
}

void TActionQueue::ThreadMain()
{
    try {
        while (true) {
            if (!QueueInvoker->ProcessQueue()) {
                if (IsFinished)
                    return;
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    } catch (...) {
        LOG_FATAL("Unhandled exception in the action queue: %s",
            ~CurrentExceptionMessage());
    }
}

void TActionQueue::OnIdle()
{ }

IInvoker::TPtr TActionQueue::GetInvoker()
{
    YASSERT(!IsFinished);
    auto invoker = ~QueueInvoker;
    YASSERT(invoker != NULL);
    return invoker;
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
    YASSERT(!IsFinished);
    YASSERT(0 <= priority && priority < static_cast<int>(QueueInvokers.size()));
    auto invoker = ~QueueInvokers[priority];
    YASSERT(invoker != NULL);
    return invoker;
}

void TPrioritizedActionQueue::Shutdown()
{
    if (IsFinished)
        return;

    IsFinished = true;
    for (int priority = 0; priority < static_cast<int>(QueueInvokers.size()); ++priority) {
        QueueInvokers[priority].Drop();
    }
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
                    return;
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    } catch (...) {
        LOG_FATAL("Unhandled exception in the action queue: %s",
            ~CurrentExceptionMessage());
    }
}

void TPrioritizedActionQueue::OnIdle()
{ }

///////////////////////////////////////////////////////////////////////////////


}

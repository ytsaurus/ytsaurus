#include "action_queue.h"

#include "../logging/log.h"
#include "../misc/ptr.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueue* TQueueInvoker::GetQueue() const
{
    return ~Queue;
}

TQueueInvoker::TQueueInvoker(Event* wakeupEvent, bool enableLogging /*= true*/)
    : Queue(new TQueue())
    , WakeupEvent(wakeupEvent)
    , EnableLogging(enableLogging)
    , Finished(false)
{ }

void TQueueInvoker::Invoke(IAction::TPtr action)
{
    if (Finished) {
        LOG_DEBUG_IF(EnableLogging, "Queue has been shutted down. Action %p is not enqueued", ~action);
    } else {
        LOG_DEBUG_IF(EnableLogging, "Enqueued action %p", ~action);
        Queue->Enqueue(action);
        WakeupEvent->Signal();
    }
}

void TQueueInvoker::Shutdown()
{
    if (Finished)
        return;
    LOG_DEBUG_IF(EnableLogging, "Shutting down");
    Finished = true;
}

///////////////////////////////////////////////////////////////////////////////

void* TActionQueue::ThreadFunc(void* param)
{
    TActionQueue* queue = (TActionQueue*) param;
    queue->ThreadMain();
    return NULL;
}

///////////////////////////////////////////////////////////////////////////////

TActionQueue::TActionQueue(bool enableLogging)
    : EnableLogging(enableLogging)
    , Finished(false)
    , Thread(ThreadFunc, (void*) this)
    , WakeupEvent(Event::rAuto)
    , QueueInvoker(New<TQueueInvoker>(&WakeupEvent, enableLogging))
{
    Thread.Start();
}

TActionQueue::~TActionQueue()
{
    Shutdown();
}

void TActionQueue::Shutdown()
{
    if (Finished)
        return;

    QueueInvoker->Shutdown();
    Finished = true;
    WakeupEvent.Signal();
    Thread.Join();
}

void TActionQueue::ThreadMain()
{
    try {
        auto* queue = QueueInvoker->GetQueue();
        while (true) {
            IAction::TPtr action;
            while (queue->Dequeue(&action)) {
                LOG_DEBUG_IF(EnableLogging, "Running action %p", ~action);
                action->Do();
                action.Drop();
            }

            if (IsEmpty()) {
                if (Finished)
                    return;
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    } catch (NStl::exception& ex) {
        LOG_FATAL_IF(EnableLogging, "Unhandled exception in the action queue: %s", ex.what());
    }
}

bool TActionQueue::IsEmpty() const
{
    return QueueInvoker->GetQueue()->IsEmpty();
}

void TActionQueue::OnIdle()
{ }

IInvoker::TPtr TActionQueue::GetInvoker()
{
    return ~QueueInvoker;
}

///////////////////////////////////////////////////////////////////////////////

void* TPrioritizedActionQueue::ThreadFunc(void* param)
{
    TPrioritizedActionQueue* queue = (TPrioritizedActionQueue*) param;
    queue->ThreadMain();
    return NULL;
}

TPrioritizedActionQueue::TPrioritizedActionQueue(i32 priorityCount)
    : PriorityCount(priorityCount)
    , QueueInvokers(priorityCount)
    , Thread(ThreadFunc, (void*) this)
    , Finished(false)
    , WakeupEvent(Event::rAuto)
{
    for (i32 i = 0; i < PriorityCount; ++i) {
        QueueInvokers[i] = New<TQueueInvoker>(&WakeupEvent);
    }

    Thread.Start();
}

TPrioritizedActionQueue::~TPrioritizedActionQueue()
{
    Shutdown();
}

IInvoker::TPtr TPrioritizedActionQueue::GetInvoker(i32 priority)
{
    assert(0 <= priority && priority < PriorityCount);
    return ~QueueInvokers[priority];
}

void TPrioritizedActionQueue::Shutdown()
{
    if (Finished)
        return;

    Finished = true;
    WakeupEvent.Signal();
    Thread.Join();
}

void TPrioritizedActionQueue::ThreadMain()
{
    try {
        while (true) {
            for (i32 i = 0; i < PriorityCount; ++i) {
                auto* queue = QueueInvokers[i]->GetQueue();
                IAction::TPtr action;
                if (queue->Dequeue(&action)) {
                    LOG_DEBUG("Running action %p", ~action);
                    action->Do();
                    action.Drop();
                    break;
                }
            }

            if (IsEmpty()) {
                if (Finished)
                    return;
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    } catch (NStl::exception& ex) {
        LOG_FATAL("Unhandled exception in the action queue: %s", ex.what());
    }
}

bool TPrioritizedActionQueue::IsEmpty()
{
    for (i32 i = 0; i < PriorityCount; ++i) {
        if (!QueueInvokers[i]->GetQueue()->IsEmpty())
            return false;
    }
    return true;
}

void TPrioritizedActionQueue::OnIdle()
{ }

///////////////////////////////////////////////////////////////////////////////


}

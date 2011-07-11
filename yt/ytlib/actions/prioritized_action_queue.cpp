#include "prioritized_action_queue.h"

#include "../logging/log.h"
#include "../misc/ptr.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("PrioritizedActionQueue");

//////////////////////////////////////////////////////////////////////////////

class TPrioritizedActionQueue::TQueueInvoker
    : public IInvoker
{
public:
    TQueueInvoker(TQueue* queue, Event& wakeupEvent)
        : Queue(queue)
        , WakeupEvent(wakeupEvent)
    { }

    virtual void Invoke(IAction::TPtr action) {
        LOG_DEBUG("Enqueued action %p", ~action);
        Queue->Enqueue(action);
        WakeupEvent.Signal();
    }
private:
    TQueue* Queue;
    Event& WakeupEvent;
};

//////////////////////////////////////////////////////////////////////////////

void* TPrioritizedActionQueue::ThreadFunc(void* param)
{
    TPrioritizedActionQueue* queue = (TPrioritizedActionQueue*) param;
    queue->ThreadMain();
    return NULL;
}

///////////////////////////////////////////////////////////////////////////////

TPrioritizedActionQueue::TPrioritizedActionQueue(i32 priorityCount)
    : Thread(ThreadFunc, (void*) this)
    , Finished(false)
    , PriorityCount(priorityCount)
    , Queues(priorityCount)
    , Invokers(priorityCount)
    , WakeupEvent(Event::rAuto)
{
    for (i32 i = 0; i < PriorityCount; ++i) {
        Queues[i].Reset(new TQueue());
        Invokers[i].Reset(new TQueueInvoker(~Queues[i], WakeupEvent));
    }

    Thread.Start();
}

TPrioritizedActionQueue::~TPrioritizedActionQueue()
{
    Shutdown();
}

IInvoker* TPrioritizedActionQueue::GetInvoker(i32 priority)
{
    assert(0 <= priority && priority < PriorityCount);
    return ~Invokers[priority];
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
            for(i32 i = 0; i < PriorityCount; ++i) {
                IAction::TPtr action;
                if (Queues[i]->Dequeue(&action)) {
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
        if (!Queues[i]->IsEmpty())
            return false;
    }
    return true;
}

void TPrioritizedActionQueue::OnIdle()
{ }

///////////////////////////////////////////////////////////////////////////////

}

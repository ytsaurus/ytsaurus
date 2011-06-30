#include "action_queue.h"

#include "../logging/log.h"
#include "../misc/ptr.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

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
    , Thread(ThreadFunc, (void*) this)
    , Finished(false)
    , WakeupEvent(Event::rAuto)
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

    Finished = true;
    WakeupEvent.Signal();
    Thread.Join();
}

void TActionQueue::ThreadMain()
{
    try {
        while (true) {
            IAction::TPtr action;
            while (Queue.Dequeue(&action)) {
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

bool TActionQueue::IsEmpty()
{
    return Queue.IsEmpty();
}

void TActionQueue::OnIdle()
{ }

void TActionQueue::Invoke(IAction::TPtr action)
{
    LOG_DEBUG_IF(EnableLogging, "Enqueued action %p", ~action);
    YASSERT(!Finished);
    Queue.Enqueue(action);
    WakeupEvent.Signal();
}

///////////////////////////////////////////////////////////////////////////////

}

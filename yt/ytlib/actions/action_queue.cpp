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
	item.EnqueueTime = PROFILE_TIMING_START();
	item.Action = action;
    Queue.Enqueue(item);
	LOG_TRACE_IF(EnableLogging, "Action is enqueued (Action: %p)", ~action);

    auto size = AtomicIncrement(QueueSize);
    PROFILE_VALUE("size", size);

    Owner->Signal();
}

void TQueueInvoker::OnShutdown()
{
    Owner = NULL;
}

bool TQueueInvoker::OnDequeueAndExecute()
{
    TItem item;
    if (!Queue.Dequeue(&item))
        return false;
    
    auto size = AtomicDecrement(QueueSize);
	PROFILE_VALUE("size", size);

	PROFILE_TIMING_STOP("wait_time", item.EnqueueTime);

	auto action = item.Action;
    LOG_TRACE_IF(EnableLogging, "Action started (Action: %p)", ~action);
	PROFILE_TIMING("exec_time") {
		action->Do();
	}
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
                    OnIdle();
                    if (Running) {
                        WakeupEvent.Wait();
                    }
                }
            }
        } catch (const std::exception& ex) {
            LOG_FATAL("Unhandled exception in the action queue\n%s", ex.what());
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

void TActionQueueBase::OnIdle()
{ }

void TActionQueueBase::Signal()
{
	WakeupEvent.Signal();
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

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

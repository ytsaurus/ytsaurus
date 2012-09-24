#include "stdafx.h"
#include "action_queue_detail.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/timing.h>

#include <ytlib/ytree/ypath_client.h>

#include <util/system/sigset.h>

namespace NYT {

using namespace NYTree;
using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(
    const Stroka& name,
    TActionQueueBase* owner,
    bool enableLogging,
    TNullable<Stroka> profilingName)
    : Owner(owner)
    , EnableLogging(enableLogging)
    , Profiler("/action_queues/" + (profilingName ? profilingName.Get() : EscapeYPathToken(name)))
    , EnqueueCounter("/enqueue_rate")
    , DequeueCounter("/dequeue_rate")
    , QueueSize(0)
    , QueueSizeCounter("/size")
    , WaitTimeCounter("/time/wait")
    , ExecTimeCounter("/time/exec")
    , TotalTimeCounter("/time/total")
{ }

bool TQueueInvoker::Invoke(const TClosure& action)
{
    if (!Owner) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored (Action: %p)", action.GetHandle());
        return false;
    }

    AtomicIncrement(QueueSize);
    Profiler.Increment(EnqueueCounter);

    TItem item;
    item.StartInstant = GetCpuInstant();
    item.Action = action;
    Queue.Enqueue(item);

    LOG_TRACE_IF(EnableLogging, "Action is enqueued (Action: %p)", action.GetHandle());

    Owner->Signal();
    return true;

}

void TQueueInvoker::Shutdown()
{
    Owner = NULL;
}

bool TQueueInvoker::DequeueAndExecute()
{
    TItem item;
    if (!Queue.Dequeue(&item)) {
        return false;
    }

    Profiler.Increment(DequeueCounter);

    auto startExecInstant = GetCpuInstant();
    Profiler.Aggregate(WaitTimeCounter, CpuDurationToValue(startExecInstant - item.StartInstant));

    auto action = item.Action;
    LOG_TRACE_IF(EnableLogging, "Action started (Action: %p)", action.GetHandle());
    action.Run();
    LOG_TRACE_IF(EnableLogging, "Action stopped (Action: %p)", action.GetHandle());

    auto size = AtomicDecrement(QueueSize);
    Profiler.Aggregate(QueueSizeCounter, size);

    auto endExecInstant = GetCpuInstant();
    Profiler.Aggregate(ExecTimeCounter, CpuDurationToValue(endExecInstant - startExecInstant));
    Profiler.Aggregate(TotalTimeCounter, CpuDurationToValue(endExecInstant - item.StartInstant));

    return true;
}

int TQueueInvoker::GetSize() const
{
    return static_cast<int>(QueueSize);
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
    YCHECK(!Running);
}

void TActionQueueBase::Start()
{
    Thread.Start();
}

void* TActionQueueBase::ThreadFunc(void* param)
{
    auto* this_ = (TActionQueueBase*) param;
    this_->ThreadMain();
    return NULL;
}

void TActionQueueBase::ThreadMain()
{
    OnThreadStart();
    NThread::SetCurrentThreadName(~ThreadName);
    while (true) {
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
    }
    YCHECK(!DequeueAndExecute());
    OnThreadShutdown();
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

void TActionQueueBase::OnThreadStart()
{
#ifdef _unix_
    // basically set empty sigmask to all threads
    sigset_t sigset;
    SigEmptySet(&sigset);
    SigProcMask(SIG_SETMASK, &sigset, NULL);
#endif
}

void TActionQueueBase::OnThreadShutdown()
{ }

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

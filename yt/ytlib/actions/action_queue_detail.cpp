#include "stdafx.h"
#include "action_queue_detail.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/timing.h>

#include <util/system/sigset.h>

namespace NYT {

using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TQueueInvoker::TQueueInvoker(
    const NYPath::TYPath& profilingPath,
    TActionQueueBase* owner,
    bool enableLogging)
    : Owner(owner)
    , EnableLogging(enableLogging)
    , Profiler("/action_queues" + profilingPath)
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
    // XXX(babenko): don't replace TActionQueueBase by auto here, see
    // http://connect.microsoft.com/VisualStudio/feedback/details/680927/dereferencing-of-incomplete-type-not-diagnosed-fails-to-synthesise-constructor-and-destructor
    TActionQueueBase* owner = Owner;

    if (!owner) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored: %p", action.GetHandle());
        return false;
    }

    AtomicIncrement(QueueSize);
    Profiler.Increment(EnqueueCounter);

    TItem item;
    item.StartInstant = GetCpuInstant();
    item.Action = action;
    Queue.Enqueue(item);

    LOG_TRACE_IF(EnableLogging, "Action enqueued: %p", action.GetHandle());

    owner->Signal();
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
    LOG_TRACE_IF(EnableLogging, "Action started: %p", action.GetHandle());
    action.Run();
    LOG_TRACE_IF(EnableLogging, "Action stopped: %p", action.GetHandle());

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
    , Running(false)
    , ThreadId(NThread::InvalidThreadId)
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
    LOG_DEBUG_IF(EnableLogging, "Starting thread: %s", ~ThreadName);

    AtomicSet(Running, true);
    Thread.Start();
}

void* TActionQueueBase::ThreadFunc(void* param)
{
    auto* this_ = static_cast<TActionQueueBase*>(param);
    this_->ThreadMain();
    return NULL;
}

void TActionQueueBase::ThreadMain()
{
    LOG_DEBUG_IF(EnableLogging, "Thread started: %s", ~ThreadName);

    OnThreadStart();
    
    NThread::SetCurrentThreadName(~ThreadName);
    ThreadId = NThread::GetCurrentThreadId();

    while (true) {
        if (!DequeueAndExecute()) {
            WakeupEvent.Reset();
            if (!DequeueAndExecute()) {
                if (Running) {
                    break;
                }
                OnIdle();
                WakeupEvent.Wait();
            }
        }
    }
    
    OnThreadShutdown();

    LOG_DEBUG_IF(EnableLogging, "Thread stopped: %s", ~ThreadName);
}

void TActionQueueBase::Shutdown()
{
    if (!IsRunning()) {
        return;
    }

    LOG_DEBUG_IF(EnableLogging, "Stopping thread: %s", ~ThreadName);

    AtomicSet(Running, false);
    WakeupEvent.Signal();

    // Prevent deadlock.
    if (NThread::GetCurrentThreadId() != ThreadId) {
        Thread.Join();
    }
}

void TActionQueueBase::OnIdle()
{ }

void TActionQueueBase::Signal()
{
    WakeupEvent.Signal();
}

bool TActionQueueBase::IsRunning() const
{
    return AtomicGet(Running);
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

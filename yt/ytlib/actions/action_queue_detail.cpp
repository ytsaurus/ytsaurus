#include "stdafx.h"
#include "action_queue_detail.h"
#include "invoker_util.h"

#include <ytlib/fibers/fiber.h>

#include <ytlib/ypath/token.h>

#include <ytlib/logging/log.h>

#include <ytlib/profiling/timing.h>

#include <util/system/sigset.h>

namespace NYT {

using namespace NYPath;
using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TInvokerQueue::TInvokerQueue(
    TExecutorThread* owner,
    IInvoker* currentInvoker,
    const NYPath::TYPath& profilingPath,
    bool enableLogging)
    : Owner(owner)
    , CurrentInvoker(currentInvoker ? currentInvoker : this)
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

bool TInvokerQueue::Invoke(const TClosure& action)
{
    // XXX(babenko): don't replace TActionQueueBase by auto here, see
    // http://connect.microsoft.com/VisualStudio/feedback/details/680927/dereferencing-of-incomplete-type-not-diagnosed-fails-to-synthesise-constructor-and-destructor
    TExecutorThread* owner = Owner;

    if (!owner) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored: %p", action.GetHandle());
        return false;
    }

    AtomicIncrement(QueueSize);
    Profiler.Increment(EnqueueCounter);

    TItem item;
    item.EnqueueInstant = GetCpuInstant();
    item.Action = action;
    Queue.Enqueue(item);

    LOG_TRACE_IF(EnableLogging, "Action enqueued: %p", action.GetHandle());

    owner->Signal();
    return true;

}

void TInvokerQueue::Shutdown()
{
    Owner = nullptr;
    CurrentInvoker = nullptr;
}

EBeginExecuteResult TInvokerQueue::BeginExecute()
{
    YASSERT(CurrentItem.Action.IsNull());

    if (!Queue.Dequeue(&CurrentItem)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    Profiler.Increment(DequeueCounter);

    CurrentItem.StartInstant = GetCpuInstant();
    Profiler.Aggregate(WaitTimeCounter, CpuDurationToValue(CurrentItem.StartInstant - CurrentItem.EnqueueInstant));

    TCurrentInvokerGuard guard(CurrentInvoker);

    try {
        CurrentItem.Action.Run();
    } catch (const TFiberTerminatedException&) {
        // Still consider this a success.
        // This caller is responsible for terminating the current fiber.
    }

    return EBeginExecuteResult::Success;
}

void TInvokerQueue::EndExecute()
{
    if (CurrentItem.Action.IsNull())
        return;

    auto size = AtomicDecrement(QueueSize);
    Profiler.Aggregate(QueueSizeCounter, size);

    auto endExecInstant = GetCpuInstant();
    Profiler.Aggregate(ExecTimeCounter, CpuDurationToValue(endExecInstant - CurrentItem.StartInstant));
    Profiler.Aggregate(TotalTimeCounter, CpuDurationToValue(endExecInstant - CurrentItem.EnqueueInstant));

    CurrentItem.Action.Reset();
}

int TInvokerQueue::GetSize() const
{
    return static_cast<int>(QueueSize);
}

bool TInvokerQueue::IsEmpty() const
{
    return const_cast< TLockFreeQueue<TItem>& >(Queue).IsEmpty();
}

///////////////////////////////////////////////////////////////////////////////

//! Pointer to the action queue being run by the current thread.
/*!
 *  Examining |CurrentActionQueue| could be useful for debugging purposes so we don't
 *  put it into an anonymous namespace to avoid name mangling.
 */
TLS_STATIC TExecutorThread* CurrentExecutorThread = nullptr;

TExecutorThread::TExecutorThread(
    const Stroka& threadName,
    bool enableLogging)
    : ThreadName(threadName)
    , EnableLogging(enableLogging)
    , Profiler("/action_queues/" + ToYPathLiteral(threadName))
    , Running(false)
    , FibersCreated(0)
    , FibersAlive(0)
    , ThreadId(NThread::InvalidThreadId)
    , WakeupEvent(Event::rManual)
    , Thread(ThreadMain, (void*) this)
{ }

TExecutorThread::~TExecutorThread()
{
    // Derived classes must call Shutdown in dtor.
    YCHECK(!Running);
}

void TExecutorThread::Start()
{
    LOG_DEBUG_IF(EnableLogging, "Starting thread (Name: %s)", ~ThreadName);

    Running = true;
    Thread.Start();
}

void* TExecutorThread::ThreadMain(void* opaque)
{
    static_cast<TExecutorThread*>(opaque)->ThreadMain();
    return nullptr;
}

void TExecutorThread::ThreadMain()
{
    LOG_DEBUG_IF(EnableLogging, "Thread started (Name: %s)", ~ThreadName);
    OnThreadStart();
    CurrentExecutorThread = this;

    NThread::SetCurrentThreadName(~ThreadName);
    ThreadId = NThread::GetCurrentThreadId();

    while (Running) {
        // Spawn a new fiber to run the loop.
        auto fiber = New<TFiber>(BIND(&TExecutorThread::FiberMain, MakeStrong(this)));
        fiber->Run();

        if (fiber->GetState() == EFiberState::Suspended) {
            // The callback has taken the ownership of the current fiber.
            // Finish sync part of the execution and respawn the fiber.
            // The current fiber will be owned by the callback.
            EndExecute();
        }
    }

    CurrentExecutorThread = nullptr;
    OnThreadShutdown();
    LOG_DEBUG_IF(EnableLogging, "Thread stopped (Name: %s)", ~ThreadName);
}

void TExecutorThread::FiberMain()
{
    ++FibersCreated;
    Profiler.Enqueue("/fibers_created", FibersCreated);

    ++FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersAlive);

    LOG_DEBUG_IF(EnableLogging, "Fiber started (Name: %s, Created: %d, Alive: %d)",
        ~ThreadName,
        FibersCreated,
        FibersAlive);

    while (Running) {
        {
            auto result = CheckedExecute();
            if (result == EBeginExecuteResult::Terminated)
                break;
            if (result == EBeginExecuteResult::Success)
                continue;
        }

        WakeupEvent.Reset();

        {
            auto result = CheckedExecute();
            if (result == EBeginExecuteResult::Terminated)
                break;
            if (result == EBeginExecuteResult::Success)
                continue;
            OnIdle();
            WakeupEvent.Wait();
        }
    }

    --FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersCreated);

    LOG_DEBUG_IF(EnableLogging, "Fiber finished (Name: %s, Created: %d, Alive: %d)",
        ~ThreadName,
        FibersCreated,
        FibersAlive);
}

EBeginExecuteResult TExecutorThread::CheckedExecute()
{
    auto result = BeginExecute();

    auto* fiber = TFiber::GetCurrent();
    if (!fiber->Yielded()) {
        // Make the matching call to EndExecute unless it is already done in ThreadMain.
        // NB: It is safe to call EndExecute even if no actual action was dequeued and
        // invoked in BeginExecute.
        EndExecute();
    }

    if (result == EBeginExecuteResult::Terminated || result == EBeginExecuteResult::QueueEmpty) {
        // NB: Running must be examined after calling BeginExecute since the latter
        // provides a so-much-needed barrier. Otherwise one may (and will) experience thread hangs
        // during shutdown.
        if (!Running) {
            return EBeginExecuteResult::Terminated;
        }
        return result;
    }

    if (fiber->Yielded()) {
        // If the current fiber has seen Yield calls then its ownership has been transfered to the
        // callback. In the latter case we must abandon the current fiber immediately
        // since the queue's thread had spawned (or will soon spawn)
        // a brand new fiber to continue serving the queue.
        return EBeginExecuteResult::Terminated;
    }

    if (fiber->IsCanceled()) {
        // All TFiberTerminatedException-s are being caught in BeginExecute.
        // A fiber that is currently being terminated cannot be reused and must be abandoned.
        return EBeginExecuteResult::Terminated;
    }

    return EBeginExecuteResult::Success;
}

void TExecutorThread::Shutdown()
{
    if (!IsRunning()) {
        return;
    }

    LOG_DEBUG_IF(EnableLogging, "Stopping thread (Name: %s)", ~ThreadName);

    Running = false;
    WakeupEvent.Signal();

    // Prevent deadlock.
    if (NThread::GetCurrentThreadId() != ThreadId) {
        Thread.Join();
    }
}

void TExecutorThread::OnIdle()
{ }

void TExecutorThread::Signal()
{
    WakeupEvent.Signal();
}

bool TExecutorThread::IsRunning() const
{
    return Running;
}

void TExecutorThread::OnThreadStart()
{
#ifdef _unix_
    // Set empty sigmask for all threads.
    sigset_t sigset;
    SigEmptySet(&sigset);
    SigProcMask(SIG_SETMASK, &sigset, nullptr);
#endif
}

void TExecutorThread::OnThreadShutdown()
{
    // TODO(babenko): consider killing the root fiber here
}

///////////////////////////////////////////////////////////////////////////////

TExecutorThreadWithQueue::TExecutorThreadWithQueue(
    IInvoker* currentInvoker,
    const Stroka& threadName,
    const Stroka& profilingName,
    bool enableLogging)
    : TExecutorThread(threadName, enableLogging)
{
    Queue = New<TInvokerQueue>(
        this,
        currentInvoker,
        "/" + ToYPathLiteral(profilingName),
        enableLogging);
    Start();
}

TExecutorThreadWithQueue::~TExecutorThreadWithQueue()
{
    Queue->Shutdown();
    Shutdown();
}

void TExecutorThreadWithQueue::Shutdown()
{
    TExecutorThread::Shutdown();
}

NYT::IInvokerPtr TExecutorThreadWithQueue::GetInvoker()
{
    return Queue;
}

int TExecutorThreadWithQueue::GetSize()
{
    return Queue->GetSize();
}

EBeginExecuteResult TExecutorThreadWithQueue::BeginExecute()
{
    return Queue->BeginExecute();
}

void TExecutorThreadWithQueue::EndExecute()
{
    Queue->EndExecute();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

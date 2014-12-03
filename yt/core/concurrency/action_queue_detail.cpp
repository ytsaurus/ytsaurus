#include "stdafx.h"
#include "action_queue_detail.h"

#include <core/actions/invoker_util.h>

#include <core/concurrency/fiber.h>

#include <core/ypath/token.h>

#include <core/logging/log.h>

#include <core/profiling/timing.h>

#include <util/system/sigset.h>

namespace NYT {
namespace NConcurrency {

using namespace NYPath;
using namespace NProfiling;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ActionQueue");

///////////////////////////////////////////////////////////////////////////////

TInvokerQueue::TInvokerQueue(
    TEventCount* eventCount,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : EventCount(eventCount)
    , ThreadId(InvalidThreadId)
    , EnableLogging(enableLogging)
    , Running(true)
    , Profiler("/action_queue")
    , EnqueueCounter("/enqueue_rate", tagIds)
    , DequeueCounter("/dequeue_rate", tagIds)
    , QueueSize(0)
    , QueueSizeCounter("/size", tagIds)
    , WaitTimeCounter("/time/wait", tagIds)
    , ExecTimeCounter("/time/exec", tagIds)
    , TotalTimeCounter("/time/total", tagIds)
{
    Profiler.SetEnabled(enableProfiling);
}

void TInvokerQueue::SetThreadId(TThreadId threadId)
{
    ThreadId = threadId;
}

bool TInvokerQueue::Invoke(const TClosure& callback)
{
    if (!Running) {
        LOG_TRACE_IF(EnableLogging, "Queue had been shut down, incoming action ignored: %p",
            callback.GetHandle());
        return false;
    }

    AtomicIncrement(QueueSize);
    Profiler.Increment(EnqueueCounter);

    TEnqueuedAction action;
    action.Finished = false;
    action.EnqueueInstant = GetCpuInstant();
    action.Callback = callback;
    Queue.Enqueue(action);

    LOG_TRACE_IF(EnableLogging, "Callback enqueued: %p",
        callback.GetHandle());

    EventCount->Notify();

    return true;
}

TThreadId TInvokerQueue::GetThreadId() const
{
    return ThreadId;
}

void TInvokerQueue::Shutdown()
{
    Running = false;
}

bool TInvokerQueue::IsRunning() const
{
    return Running;
}

EBeginExecuteResult TInvokerQueue::BeginExecute(TEnqueuedAction* action)
{
    YASSERT(action->Finished);

    if (!Queue.Dequeue(action)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    EventCount->CancelWait();

    Profiler.Increment(DequeueCounter);

    action->StartInstant = GetCpuInstant();
    Profiler.Aggregate(WaitTimeCounter, CpuDurationToValue(action->StartInstant - action->EnqueueInstant));

    TCurrentInvokerGuard guard(this);

    // Move callback to the stack frame to ensure that we hold it as hold as it runs.
    auto callback = std::move(action->Callback);
    try {
        callback.Run();
    } catch (const TFiberTerminatedException&) {
        // Still consider this a success.
        // This caller is responsible for terminating the current fiber.
    }

    return EBeginExecuteResult::Success;
}

void TInvokerQueue::EndExecute(TEnqueuedAction* action)
{
    if (action->Finished)
        return;

    auto size = AtomicDecrement(QueueSize);
    Profiler.Aggregate(QueueSizeCounter, size);

    auto endExecInstant = GetCpuInstant();
    Profiler.Aggregate(ExecTimeCounter, CpuDurationToValue(endExecInstant - action->StartInstant));
    Profiler.Aggregate(TotalTimeCounter, CpuDurationToValue(endExecInstant - action->EnqueueInstant));

    action->Finished = true;
}

int TInvokerQueue::GetSize() const
{
    return static_cast<int>(QueueSize);
}

bool TInvokerQueue::IsEmpty() const
{
    return const_cast< TLockFreeQueue<TEnqueuedAction>& >(Queue).IsEmpty();
}

///////////////////////////////////////////////////////////////////////////////

//! Pointer to the action queue being run by the current thread.
/*!
 *  Examining |CurrentActionQueue| could be useful for debugging purposes so we don't
 *  put it into an anonymous namespace to avoid name mangling.
 */
TLS_STATIC TExecutorThread* CurrentExecutorThread = nullptr;

TExecutorThread::TExecutorThread(
    TEventCount* eventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : EventCount(eventCount)
    , ThreadName(threadName)
    , EnableLogging(enableLogging)
    , Profiler("/action_queue", tagIds)
    , Running(false)
    , Started(NewPromise())
    , FibersCreated(0)
    , FibersAlive(0)
    , ThreadId(InvalidThreadId)
    , Thread(ThreadMain, (void*) this)
{
    Profiler.SetEnabled(enableProfiling);
}

void TExecutorThread::Start()
{
    Running = true;    
    
    LOG_DEBUG_IF(EnableLogging, "Starting thread (Name: %s)",
        ~ThreadName);
    
    Thread.Start();
    Started.Get();
}

TExecutorThread::~TExecutorThread()
{
    YCHECK(!Running);
    Thread.Detach();
}

void* TExecutorThread::ThreadMain(void* opaque)
{
    static_cast<TExecutorThread*>(opaque)->ThreadMain();
    return nullptr;
}

void TExecutorThread::ThreadMain()
{
    // NB: This way we also hold this strongly for the duration of this method.
    auto fiberMainCallback = BIND(&TExecutorThread::FiberMain, MakeStrong(this));

    try {
        OnThreadStart();
        CurrentExecutorThread = this;
        SetCurrentThreadName(~ThreadName);
        ThreadId = GetCurrentThreadId();
        Started.Set();

        LOG_DEBUG_IF(EnableLogging, "Thread started (Name: %s)",
            ~ThreadName);

        while (Running) {
            // Spawn a new fiber to run the loop.
            auto fiber = New<TFiber>(fiberMainCallback);
            auto state = fiber->Run();

            YCHECK(state == EFiberState::Suspended || state == EFiberState::Terminated);

            // Check for fiber termination.
            if (state == EFiberState::Terminated)
                break;

            // The callback has taken the ownership of the current fiber.
            // Finish sync part of the execution and respawn the fiber.
            // The current fiber will be owned by the callback.
            if (state == EFiberState::Suspended) {
                EndExecute();
            }
        }

        CurrentExecutorThread = nullptr;
        OnThreadShutdown();
        
        LOG_DEBUG_IF(EnableLogging, "Thread stopped (Name: %s)",
            ~ThreadName);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %s)",
            ~ThreadName);
    }
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

    bool stop = false;
    while (!stop) {
        auto cookie = EventCount->PrepareWait();
        auto result = Execute();
        switch (result) {
            case EBeginExecuteResult::Success:
                // CancelWait was called inside Execute.
                break;

            case EBeginExecuteResult::Terminated:
                // CancelWait was called inside Execute.
                stop = true;
                break;

            case EBeginExecuteResult::QueueEmpty:
                EventCount->Wait(cookie);
                break;

            default:
                YUNREACHABLE();
        }
    }

    --FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersAlive);

    LOG_DEBUG_IF(EnableLogging, "Fiber finished (Name: %s, Created: %d, Alive: %d)",
        ~ThreadName,
        FibersCreated,
        FibersAlive);
}

EBeginExecuteResult TExecutorThread::Execute()
{
    if (!Running) {
        EventCount->CancelWait();
        return EBeginExecuteResult::Terminated;
    }

    auto result = BeginExecute();

    auto* fiber = TFiber::GetCurrent();
    if (!fiber->Yielded()) {
        // Make the matching call to EndExecute unless it is already done in ThreadMain.
        // NB: It is safe to call EndExecute even if no actual action was dequeued and
        // invoked in BeginExecute.
        EndExecute();
    }

    if (result == EBeginExecuteResult::Terminated || result == EBeginExecuteResult::QueueEmpty) {
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
    if (!Running)
        return;

    LOG_DEBUG_IF(EnableLogging, "Stopping thread (Name: %s)",
        ~ThreadName);

    Running = false;
    EventCount->NotifyAll();

    // Prevent deadlock.
    if (GetCurrentThreadId() != ThreadId) {
        Thread.Join();
    }
}

TThreadId TExecutorThread::GetId() const
{
    return TThreadId(Thread.SystemId());
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

TSingleQueueExecutorThread::TSingleQueueExecutorThread(
    TInvokerQueuePtr queue,
    TEventCount* eventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : TExecutorThread(eventCount, threadName, tagIds, enableLogging, enableProfiling)
    , Queue(queue)
{ }

TSingleQueueExecutorThread::~TSingleQueueExecutorThread()
{ }

IInvokerPtr TSingleQueueExecutorThread::GetInvoker()
{
    return Queue;
}

EBeginExecuteResult TSingleQueueExecutorThread::BeginExecute()
{
    return Queue->BeginExecute(&CurrentAction);
}

void TSingleQueueExecutorThread::EndExecute()
{
    Queue->EndExecute(&CurrentAction);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

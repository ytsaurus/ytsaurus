#include "stdafx.h"
#include "action_queue_detail.h"
#include "fiber.h"
#include "scheduler.h"

#include <core/actions/invoker_util.h>

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
    // XXX(babenko): VS2013 Nov CTP does not have a proper ctor :(
    // , Running(true)
    , Profiler("/action_queue")
    , EnqueueCounter("/enqueue_rate", tagIds)
    , DequeueCounter("/dequeue_rate", tagIds)
    , QueueSize(0)
    , QueueSizeCounter("/size", tagIds)
    , WaitTimeCounter("/time/wait", tagIds)
    , ExecTimeCounter("/time/exec", tagIds)
    , TotalTimeCounter("/time/total", tagIds)
{
    Running.store(true, std::memory_order_relaxed);
    Profiler.SetEnabled(enableProfiling);
}

void TInvokerQueue::SetThreadId(TThreadId threadId)
{
    ThreadId = threadId;
}

void TInvokerQueue::Invoke(const TClosure& callback)
{
    if (!Running.load(std::memory_order_relaxed)) {
        LOG_TRACE_IF(
            EnableLogging,
            "Queue had been shut down, incoming action ignored: %p",
            callback.GetHandle());
        return;
    }

    AtomicIncrement(QueueSize);
    Profiler.Increment(EnqueueCounter);

    LOG_TRACE_IF(EnableLogging, "Callback enqueued: %p",
        callback.GetHandle());

    TEnqueuedAction action;
    action.Finished = false;
    action.EnqueuedAt = GetCpuInstant();
    action.Callback = std::move(callback);
    Queue.Enqueue(action);

    EventCount->Notify();
}

TThreadId TInvokerQueue::GetThreadId() const
{
    return ThreadId;
}

void TInvokerQueue::Shutdown()
{
    Running.store(false, std::memory_order_relaxed);
}

EBeginExecuteResult TInvokerQueue::BeginExecute(TEnqueuedAction* action)
{
    YASSERT(action->Finished);

    if (!Queue.Dequeue(action)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    EventCount->CancelWait();

    Profiler.Increment(DequeueCounter);

    action->StartedAt = GetCpuInstant();
    Profiler.Aggregate(
        WaitTimeCounter,
        CpuDurationToValue(action->StartedAt - action->EnqueuedAt));

    // Move callback to the stack frame to ensure that we hold it as hold as it runs.
    auto callback = std::move(action->Callback);

    try {
        TCurrentInvokerGuard guard(this);
        callback.Run();
    } catch (const TFiberCanceledException&) {
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

    auto endedAt = GetCpuInstant();
    Profiler.Aggregate(
        ExecTimeCounter,
        CpuDurationToValue(endedAt - action->StartedAt));
    Profiler.Aggregate(
        TotalTimeCounter,
        CpuDurationToValue(endedAt - action->EnqueuedAt));

    action->Finished = true;
}

int TInvokerQueue::GetSize() const
{
    return static_cast<int>(QueueSize);
}

bool TInvokerQueue::IsEmpty() const
{
    return const_cast<TLockFreeQueue<TEnqueuedAction>&>(Queue).IsEmpty();
}

///////////////////////////////////////////////////////////////////////////////

//! Pointer to the action queue being run by the current thread.
/*!
 *  Examining |CurrentExecutorThread| could be useful for debugging purposes so we don't
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
    // XXX(babenko): VS2013 Nov CTP does not have a proper ctor :(
    // , Running(false)
    , Started(NewPromise())
    , ThreadId(InvalidThreadId)
    , Thread(ThreadMain, (void*) this)
    , FibersCreated(0)
    , FibersAlive(0)
{
    Epoch.store(0, std::memory_order_relaxed);
    Profiler.SetEnabled(enableProfiling);
}

TExecutorThread::~TExecutorThread()
{
    YCHECK(!IsRunning());
}

void* TExecutorThread::ThreadMain(void* opaque)
{
    static_cast<TExecutorThread*>(opaque)->ThreadMain();
    return nullptr;
}

void TExecutorThread::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    ThreadId = GetCurrentThreadId();

    try {
        OnThreadStart();
        CurrentExecutorThread = this;

        Started.Set();

        ThreadMainLoop();

        CurrentExecutorThread = nullptr;
        OnThreadShutdown();

        LOG_DEBUG_IF(EnableLogging, "Thread stopped (Name: %s)",
            ~ThreadName);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %s)",
            ~ThreadName);
    }
}

void TExecutorThread::ThreadMainLoop()
{
    TCurrentSchedulerGuard guard(this);

    while (IsRunning()) {
        YASSERT(!CurrentFiber);

        if (RunQueue.empty()) {
            // Spawn a new idle fiber to run the loop.
            YASSERT(!IdleFiber);
            IdleFiber= New<TFiber>(BIND(
                &TExecutorThread::FiberMain,
                MakeStrong(this),
                (Epoch.load(std::memory_order_relaxed) & ~0x1)));

            RunQueue.push_back(IdleFiber);
        }

        YASSERT(!RunQueue.empty());

        CurrentFiber = std::move(RunQueue.front());
        RunQueue.pop_front();

        YCHECK(CurrentFiber->GetState() == EFiberState::Suspended);

        CurrentFiber->SetState(EFiberState::Running);
        SwitchExecutionContext(
            &SchedulerContext,
            &CurrentFiber->GetContext(),
            /* as per FiberTrampoline */ CurrentFiber.Get());

        switch (CurrentFiber->GetState()) {
            case EFiberState::Sleeping:
                // Advance epoch as this (idle) fiber might be rescheduled elsewhere.
                if (CurrentFiber == IdleFiber) {
                    Epoch.fetch_add(0x2, std::memory_order_relaxed);
                    IdleFiber.Reset();
                }
                // Reschedule this fiber.
                Reschedule(
                    std::move(CurrentFiber),
                    std::move(WaitForFuture),
                    std::move(SwitchToInvoker));
                break;

            case EFiberState::Suspended:
                // Reschedule this fiber to be executed later.
                RunQueue.emplace_back(std::move(CurrentFiber));
                break;

            case EFiberState::Running:
                // We cannot reach here.
                YUNREACHABLE();
                break;

            case EFiberState::Terminated:
            case EFiberState::Canceled:
                // Advance epoch as this (idle) fiber just died.
                if (CurrentFiber == IdleFiber) {
                    Epoch.fetch_add(0x2, std::memory_order_relaxed);
                    IdleFiber.Reset();
                }
                // We do not own this fiber any more, so forget about it.
                CurrentFiber.Reset();
                break;

            case EFiberState::Crashed:
                // Notify about an unhandled exception.
                Crash(CurrentFiber->GetException());
                break;
        }

        // Finish sync part of the execution.
        if (ShouldEndExecute) {
            EndExecute();
            ShouldEndExecute = false;
        }

        // Check for a clear scheduling state.
        YASSERT(!CurrentFiber);
        YASSERT(!WaitForFuture);
        YASSERT(!SwitchToInvoker);
    }
}

void TExecutorThread::FiberMain(unsigned int epoch)
{
    ++FibersCreated;
    Profiler.Enqueue("/fibers_created", FibersCreated);

    ++FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersAlive);

    LOG_DEBUG_IF(EnableLogging, "Fiber started (Name: %s, Created: %d, Alive: %d)",
        ~ThreadName,
        FibersCreated,
        FibersAlive);

    bool running = true;
    while (running) {
        auto cookie = EventCount->PrepareWait();
        auto result = Execute(epoch);
        switch (result) {
            case EBeginExecuteResult::Success:
                // EventCount->CancelWait was called inside Execute.
                break;

            case EBeginExecuteResult::Terminated:
                // EventCount->CancelWait was called inside Execute.
                running = false;
                break;

            case EBeginExecuteResult::QueueEmpty:
                EventCount->Wait(cookie);
                break;

            default:
                YUNREACHABLE();
        }
    }

    --FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersCreated);

    LOG_DEBUG_IF(EnableLogging, "Fiber finished (Name: %s, Created: %d, Alive: %d)",
        ~ThreadName,
        FibersCreated,
        FibersAlive);
}

EBeginExecuteResult TExecutorThread::Execute(unsigned int spawnedEpoch)
{
    if (!IsRunning()) {
        EventCount->CancelWait();
        return EBeginExecuteResult::Terminated;
    }

    // EventCount->CancelWait must be called within BeginExecute.
    ShouldEndExecute = true;
    auto result = BeginExecute();

    auto currentEpoch = Epoch.load(std::memory_order_relaxed);

    if (spawnedEpoch == currentEpoch) {
        // Make the matching call to EndExecute unless it is already done in ThreadMain.
        // NB: It is safe to call EndExecute even if no actual action was dequeued and
        // invoked in BeginExecute.
        if (ShouldEndExecute) {
            EndExecute();
            ShouldEndExecute = false;
        }
    }

    if (
        result == EBeginExecuteResult::QueueEmpty ||
        result == EBeginExecuteResult::Terminated) {
        return result;
    }

    if (spawnedEpoch != currentEpoch) {
        // If the current fiber has seen WaitFor/SwitchTo calls then
        // its ownership has been transfered to the callback. In the latter case
        // we must abandon the current fiber immediately since the queue's thread
        // had spawned (or will soon spawn) a brand new fiber to continue
        // serving the queue.
        return EBeginExecuteResult::Terminated;
    }

    // TODO(sandello): Does this check makes sense in the new setting?
    /*
    if (fiber->IsCanceled()) {
        // All TFiberCanceledException-s are being caught in BeginExecute.
        // A fiber that is currently being terminated cannot be reused and must be abandoned.
        return EBeginExecuteResult::Terminated;
    }
    */

    return EBeginExecuteResult::Success;
}

void TExecutorThread::Reschedule(TFiberPtr fiber, TFuture<void> future, IInvokerPtr invoker)
{
    SetCurrentInvoker(invoker, fiber.Get());

    auto precontinuation = BIND([] (TFiberPtr fiber, bool cancel) {
        if (cancel) {
            fiber->Cancel();
        }

        YCHECK(fiber->GetState() == EFiberState::Sleeping);
        fiber->SetState(EFiberState::Suspended);

        GetCurrentScheduler()->YieldTo(std::move(fiber));
    }, Passed(std::move(fiber)));

    auto continuation = BIND(&GuardedInvoke,
        Passed(std::move(invoker)),
        Passed(BIND(precontinuation, false)),
        Passed(BIND(precontinuation, true)));

    if (future) {
        future.Subscribe(std::move(continuation));
    } else {
        RunQueue.emplace_front(New<TFiber>(std::move(continuation)));
    }
}

void TExecutorThread::Crash(std::exception_ptr exception)
{
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Fiber has crashed in executor thread (Name: %s)",
            ~ThreadName);
    } catch (...) {
        YCHECK(false);
    }
}

void TExecutorThread::Start()
{
    Epoch.fetch_or(0x1, std::memory_order_relaxed);

    LOG_DEBUG_IF(EnableLogging, "Starting thread (Name: %s)",
        ~ThreadName);

    Thread.Start();
    Started.Get();
}

void TExecutorThread::Shutdown()
{
    if (!IsRunning()) {
        return;
    }

    LOG_DEBUG_IF(EnableLogging, "Stopping thread (Name: %s)",
        ~ThreadName);

    Epoch.fetch_and(~((unsigned int)0x1), std::memory_order_relaxed);

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
    return (Epoch.load(std::memory_order_relaxed) & 0x1) == 0x1;
}

TFiber* TExecutorThread::GetCurrentFiber()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    return CurrentFiber.Get();
}

void TExecutorThread::Return()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);
    YASSERT(CurrentFiber->CanReturn());

    SwitchExecutionContext(
        &CurrentFiber->GetContext(),
        &SchedulerContext,
        nullptr);

    YUNREACHABLE();
}

void TExecutorThread::Yield()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);
    auto fiber = CurrentFiber.Get();

    if (fiber->IsCanceled()) {
        throw TFiberCanceledException();
    }

    fiber->SetState(EFiberState::Suspended);
    SwitchExecutionContext(
        &fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    if (fiber->IsCanceled()) {
        throw TFiberCanceledException();
    }
}

void TExecutorThread::YieldTo(TFiberPtr&& other)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);

    if (CurrentFiber->IsCanceled()) {
        throw TFiberCanceledException();
    }

    // Memoize raw pointers.
    auto caller = CurrentFiber.Get();
    auto target = other.Get();

    RunQueue.emplace_front(std::move(CurrentFiber));
    CurrentFiber = std::move(other);

    caller->SetState(EFiberState::Suspended);
    target->SetState(EFiberState::Running);

    SwitchExecutionContext(
        &caller->GetContext(),
        &target->GetContext(),
        /* as per FiberTrampoline */ target);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    if (caller->IsCanceled()) {
        throw TFiberCanceledException();
    }
}

void TExecutorThread::SwitchTo(IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);
    auto fiber = CurrentFiber.Get();

    if (fiber->IsCanceled()) {
        throw TFiberCanceledException();
    }

    // Update scheduling state.
    YASSERT(!SwitchToInvoker);
    SwitchToInvoker = std::move(invoker);

    fiber->SetState(EFiberState::Sleeping);
    SwitchExecutionContext(
        &fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    if (fiber->IsCanceled()) {
        throw TFiberCanceledException();
    }
}

void TExecutorThread::WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);
    auto fiber = CurrentFiber.Get();

    if (fiber->IsCanceled()) {
        throw TFiberCanceledException();
    }

    // Update scheduling state.
    YASSERT(!WaitForFuture);
    WaitForFuture = std::move(future);
    YASSERT(!SwitchToInvoker);
    SwitchToInvoker = std::move(invoker);

    fiber->SetState(EFiberState::Sleeping);
    SwitchExecutionContext(
        &fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    if (fiber->IsCanceled()) {
        throw TFiberCanceledException();
    }
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
{ }

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

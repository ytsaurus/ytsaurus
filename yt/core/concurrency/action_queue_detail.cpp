#include "stdafx.h"
#include "action_queue_detail.h"
#include "fiber.h"
#include "scheduler.h"
#include "thread.h"
#include "private.h"

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

static const auto& Logger = ConcurrencyLogger;

///////////////////////////////////////////////////////////////////////////////

namespace {

void ResumeFiber(TFiberPtr fiber)
{
    YCHECK(fiber->GetState() == EFiberState::Sleeping);
    fiber->SetSuspended();

    GetCurrentScheduler()->YieldTo(std::move(fiber));
}

void UnwindFiber(TFiberPtr fiber)
{
    fiber->GetCanceler().Run();

    BIND(&ResumeFiber, Passed(std::move(fiber)))
        .Via(GetFinalizerInvoker())
        .Run();
}

void CheckForCanceledFiber(TFiber* fiber)
{
    if (fiber->IsCanceled()) {
        LOG_DEBUG("Throwing fiber cancelation exception");
        throw TFiberCanceledException();
    }
}

} // namespace

///////////////////////////////////////////////////////////////////////////////

TSchedulerThread::TSchedulerThread(
    TEventCount* callbackEventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : CallbackEventCount(callbackEventCount)
    , ThreadName(threadName)
    , EnableLogging(enableLogging)
    , Profiler("/action_queue", tagIds)
    , Epoch(0)
    , Thread(ThreadMain, (void*) this)
{
    Profiler.SetEnabled(enableProfiling);
}

TSchedulerThread::~TSchedulerThread()
{
    YCHECK(!IsRunning());
    Thread.Detach();
    // TODO(sandello): Why not join here?
}

void TSchedulerThread::Start()
{
    LOG_DEBUG_IF(EnableLogging, "Starting thread (Name: %v)",
        ThreadName);

    Thread.Start();
    ThreadId = TThreadId(Thread.SystemId());

    OnStart();

    ThreadStartedEvent.Wait();
}

void TSchedulerThread::Shutdown()
{
    if (!IsRunning()) {
        return;
    }

    LOG_DEBUG_IF(EnableLogging, "Stopping thread (Name: %v)",
        ThreadName);

    Epoch.fetch_add(0x1, std::memory_order_relaxed);

    CallbackEventCount->NotifyAll();

    OnShutdown();

    // Prevent deadlock.
    if (GetCurrentThreadId() != ThreadId) {
        Thread.Join();
    }
}

void* TSchedulerThread::ThreadMain(void* opaque)
{
    static_cast<TSchedulerThread*>(opaque)->ThreadMain();
    return nullptr;
}

void TSchedulerThread::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    TCurrentSchedulerGuard guard(this);
    SetCurrentThreadName(ThreadName.c_str());

    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        OnThreadStart();
        ThreadStartedEvent.NotifyAll();
        LOG_DEBUG_IF(EnableLogging, "Thread started (Name: %v)",
            ThreadName);

        while (IsRunning()) {
            ThreadMainStep();
        }

        OnThreadShutdown();
        LOG_DEBUG_IF(EnableLogging, "Thread stopped (Name: %v)",
            ThreadName);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)",
            ThreadName);
    }
}

void TSchedulerThread::ThreadMainStep()
{
    YASSERT(!CurrentFiber);

    if (RunQueue.empty()) {
        // Spawn a new idle fiber to run the loop.
        YASSERT(!IdleFiber);
        IdleFiber = New<TFiber>(BIND(
            &TSchedulerThread::FiberMain,
            MakeStrong(this),
            Epoch.load(std::memory_order_relaxed)));
        RunQueue.push_back(IdleFiber);
    }

    YASSERT(!RunQueue.empty());

    CurrentFiber = std::move(RunQueue.front());
    RunQueue.pop_front();

    YCHECK(CurrentFiber->GetState() == EFiberState::Suspended);
    CurrentFiber->SetRunning();

    SwitchExecutionContext(
        &SchedulerContext,
        CurrentFiber->GetContext(),
        /* as per TFiber::Trampoline */ CurrentFiber.Get());

    // Notify context switch subscribers.
    OnContextSwitch();

    auto maybeReleaseIdleFiber = [&] () {
        if (CurrentFiber == IdleFiber) {
            // Advance epoch as this (idle) fiber might be rescheduled elsewhere.
            Epoch.fetch_add(0x2, std::memory_order_relaxed);
            IdleFiber.Reset();
        }
    };

    switch (CurrentFiber->GetState()) {
        case EFiberState::Sleeping:
            maybeReleaseIdleFiber();
            // Reschedule this fiber to wake up later.
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
            maybeReleaseIdleFiber();
            // We do not own this fiber anymore, so forget about it.
            CurrentFiber.Reset();
            break;

        default:
            YUNREACHABLE();
    }

    // Finish sync part of the execution.
    EndExecute();

    // Check for a clear scheduling state.
    YASSERT(!CurrentFiber);
    YASSERT(!WaitForFuture);
    YASSERT(!SwitchToInvoker);
}

void TSchedulerThread::FiberMain(unsigned int spawnedEpoch)
{
    ++FibersCreated;
    Profiler.Enqueue("/fibers_created", FibersCreated);

    ++FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersAlive);

    LOG_DEBUG_IF(EnableLogging, "Fiber started (Name: %v, Created: %v, Alive: %v)",
        ThreadName,
        FibersCreated,
        FibersAlive);

    while (FiberMainStep(spawnedEpoch));

    --FibersAlive;
    Profiler.Enqueue("/fibers_alive", FibersAlive);

    LOG_DEBUG_IF(EnableLogging, "Fiber finished (Name: %v, Created: %v, Alive: %v)",
        ThreadName,
        FibersCreated,
        FibersAlive);
}

bool TSchedulerThread::FiberMainStep(unsigned int spawnedEpoch)
{
    auto cookie = CallbackEventCount->PrepareWait();

    if (!IsRunning()) {
        return false;
    }

    // CancelWait must be called within BeginExecute, if needed.
    auto result = BeginExecute();

    // NB: We might get to this point after a long sleep, and scheduler might spawn
    // another event loop. So we carefully examine scheduler state.
    auto currentEpoch = Epoch.load(std::memory_order_relaxed);

    if (spawnedEpoch == currentEpoch) {
        // Make the matching call to EndExecute unless it is already done in ThreadMainStep.
        // NB: It is safe to call EndExecute even if no actual action was dequeued and
        // invoked in BeginExecute.
        EndExecute();
    }

    if (result == EBeginExecuteResult::QueueEmpty) {
        CallbackEventCount->Wait(cookie);
        return true;
    }

    if (result == EBeginExecuteResult::Terminated) {
        return false;
    }

    if (spawnedEpoch != currentEpoch) {
        // If the current fiber has seen WaitFor/SwitchTo calls then
        // its ownership has been transferred to the callback. In the latter case
        // we must abandon the current fiber immediately since the queue's thread
        // had spawned (or will soon spawn) a brand new fiber to continue
        // serving the queue.
        return false;
    }

    return !CurrentFiber->IsCancelable();
}

void TSchedulerThread::Reschedule(TFiberPtr fiber, TFuture<void> future, IInvokerPtr invoker)
{
    SetCurrentInvoker(invoker, fiber.Get());

    fiber->GetCanceler(); // Initialize canceler; who knows what might happen to this fiber?

    auto resume = BIND(&ResumeFiber, fiber);
    auto unwind = BIND(&UnwindFiber, fiber);

    if (future) {
        future.Subscribe(BIND([=] (const TError&) mutable {
            GuardedInvoke(std::move(invoker), std::move(resume), std::move(unwind));
        }));
    } else {
        GuardedInvoke(std::move(invoker), std::move(resume), std::move(unwind));
    }
}

void TSchedulerThread::OnContextSwitch()
{
    ContextSwitchCallbacks.Fire();
    ContextSwitchCallbacks.Clear();
}

TThreadId TSchedulerThread::GetId() const
{
    return ThreadId;
}

bool TSchedulerThread::IsRunning() const
{
    return (Epoch.load(std::memory_order_relaxed) & 0x1) == 0x0;
}

TFiber* TSchedulerThread::GetCurrentFiber()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    return CurrentFiber.Get();
}

void TSchedulerThread::Return()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);
    YASSERT(CurrentFiber->IsTerminated());

    SwitchExecutionContext(
        CurrentFiber->GetContext(),
        &SchedulerContext,
        nullptr);

    YUNREACHABLE();
}

void TSchedulerThread::Yield()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto fiber = CurrentFiber.Get();
    YASSERT(fiber);

    CheckForCanceledFiber(fiber);

    YCHECK(CurrentFiber->GetState() == EFiberState::Running);
    fiber->SetSuspended();

    SwitchExecutionContext(
        fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    CheckForCanceledFiber(fiber);
}

void TSchedulerThread::SubscribeContextSwitched(TClosure callback)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    ContextSwitchCallbacks.Subscribe(std::move(callback));
}

void TSchedulerThread::UnsubscribeContextSwitched(TClosure callback)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    ContextSwitchCallbacks.Unsubscribe(std::move(callback));
}

void TSchedulerThread::YieldTo(TFiberPtr&& other)
{
    VERIFY_THREAD_AFFINITY(HomeThread);
    YASSERT(CurrentFiber);

    // Memoize raw pointers.
    auto caller = CurrentFiber.Get();
    auto target = other.Get();

    // TODO(babenko): handle canceled caller

    RunQueue.emplace_front(std::move(CurrentFiber));
    CurrentFiber = std::move(other);

    caller->SetSuspended();
    target->SetRunning();

    SwitchExecutionContext(
        caller->GetContext(),
        target->GetContext(),
        /* as per FiberTrampoline */ target);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    CheckForCanceledFiber(caller);
}

void TSchedulerThread::SwitchTo(IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YASSERT(CurrentFiber);
    auto fiber = CurrentFiber.Get();

    CheckForCanceledFiber(fiber);

    // Update scheduling state.
    YASSERT(!SwitchToInvoker);
    SwitchToInvoker = std::move(invoker);

    fiber->SetSleeping();

    SwitchExecutionContext(
        fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    CheckForCanceledFiber(fiber);
}

void TSchedulerThread::WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto fiber = CurrentFiber.Get();
    YASSERT(fiber);

    CheckForCanceledFiber(fiber);

    UninterruptableWaitFor(std::move(future), std::move(invoker));

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    CheckForCanceledFiber(fiber);
}

void TSchedulerThread::UninterruptableWaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto fiber = CurrentFiber.Get();
    YASSERT(fiber);

    // Update scheduling state.
    YASSERT(!WaitForFuture);
    WaitForFuture = std::move(future);
    YASSERT(!SwitchToInvoker);
    SwitchToInvoker = std::move(invoker);

    fiber->SetSleeping(WaitForFuture);

    SwitchExecutionContext(
        fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.
}

void TSchedulerThread::OnStart()
{ }

void TSchedulerThread::OnShutdown()
{ }

void TSchedulerThread::OnThreadStart()
{
#ifdef _unix_
    // Set empty sigmask for all threads.
    sigset_t sigset;
    SigEmptySet(&sigset);
    SigProcMask(SIG_SETMASK, &sigset, nullptr);
#endif
}

void TSchedulerThread::OnThreadShutdown()
{ }

///////////////////////////////////////////////////////////////////////////////

TSingleQueueSchedulerThread::TSingleQueueSchedulerThread(
    TInvokerQueuePtr queue,
    TEventCount* callbackEventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : TSchedulerThread(
        callbackEventCount,
        threadName,
        tagIds,
        enableLogging,
        enableProfiling)
    , Queue(queue)
{ }

TSingleQueueSchedulerThread::~TSingleQueueSchedulerThread()
{ }

IInvokerPtr TSingleQueueSchedulerThread::GetInvoker()
{
    return Queue;
}

EBeginExecuteResult TSingleQueueSchedulerThread::BeginExecute()
{
    return Queue->BeginExecute(&CurrentAction);
}

void TSingleQueueSchedulerThread::EndExecute()
{
    Queue->EndExecute(&CurrentAction);
}

///////////////////////////////////////////////////////////////////////////////

TEVSchedulerThread::TInvoker::TInvoker(TEVSchedulerThread* owner)
    : Owner(owner)
{ }

void TEVSchedulerThread::TInvoker::Invoke(const TClosure& callback)
{
    YASSERT(callback);

    if (!Owner->IsRunning())
        return;

    Owner->Queue.Enqueue(callback);
    Owner->CallbackWatcher.send();
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
TThreadId TEVSchedulerThread::TInvoker::GetThreadId() const
{
    return Owner->ThreadId;
}

bool TEVSchedulerThread::TInvoker::CheckAffinity(IInvokerPtr invoker) const
{
    return invoker.Get() == this;
}
#endif

///////////////////////////////////////////////////////////////////////////////

TEVSchedulerThread::TEVSchedulerThread(
    const Stroka& threadName,
    bool enableLogging)
    : TSchedulerThread(
        &CallbackEventCount,
        threadName,
        NProfiling::EmptyTagIds,
        enableLogging,
        false)
    , CallbackWatcher(EventLoop)
    , Invoker(New<TInvoker>(this))
{
    CallbackWatcher.set<TEVSchedulerThread, &TEVSchedulerThread::OnCallback>(this);
    CallbackWatcher.start();
}

IInvokerPtr TEVSchedulerThread::GetInvoker()
{
    return Invoker;
}

void TEVSchedulerThread::OnShutdown()
{
    CallbackWatcher.send();
}

EBeginExecuteResult TEVSchedulerThread::BeginExecute()
{
    {
        auto result = BeginExecuteCallbacks();
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }
    }

    EventLoop.run(0);

    {
        auto result = BeginExecuteCallbacks();
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }
    }

    // NB: Never return QueueEmpty to prevent waiting on CallbackEventCount.
    return EBeginExecuteResult::Success;
}

EBeginExecuteResult TEVSchedulerThread::BeginExecuteCallbacks()
{
    if (!IsRunning()) {
        return EBeginExecuteResult::Terminated;
    }

    TClosure callback;
    if (!Queue.Dequeue(&callback)) {
        return EBeginExecuteResult::QueueEmpty;
    }

    try {
        TCurrentInvokerGuard guard(Invoker);
        callback.Run();
        return EBeginExecuteResult::Success;
    } catch (const TFiberCanceledException&) {
        return EBeginExecuteResult::Terminated;
    }
}

void TEVSchedulerThread::EndExecute()
{ }

void TEVSchedulerThread::OnCallback(ev::async&, int)
{
    EventLoop.break_loop();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

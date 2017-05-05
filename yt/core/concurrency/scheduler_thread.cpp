#include "scheduler_thread.h"
#include "private.h"
#include "fiber.h"

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

    GetFinalizerInvoker()->Invoke(BIND(&ResumeFiber, Passed(std::move(fiber))));
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
    std::shared_ptr<TEventCount> callbackEventCount,
    const Stroka& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : CallbackEventCount(std::move(callbackEventCount))
    , ThreadName(threadName)
    , EnableLogging(enableLogging)
    , Profiler("/action_queue", tagIds)
    , Thread(ThreadMain, (void*) this)
    , CreatedFibersCounter("/created_fibers")
    , AliveFibersCounter("/alive_fibers")
{
    Profiler.SetEnabled(enableProfiling);
}

TSchedulerThread::~TSchedulerThread()
{
    Shutdown();
}

void TSchedulerThread::Start()
{
    ui64 epoch = Epoch.load(std::memory_order_acquire);
    while (true) {
        if ((epoch & StartedEpochMask) != 0x0) {
            // Startup already in progress.
            goto await;
        }
        // Acquire startup lock.
        if (Epoch.compare_exchange_strong(epoch, epoch | StartedEpochMask, std::memory_order_release)) {
            break;
        }
    }

    if ((epoch & ShutdownEpochMask) == 0x0) {
        LOG_DEBUG_IF(EnableLogging, "Starting thread (Name: %v)", ThreadName);

        Thread.Start();
        ThreadId = TThreadId(Thread.Id());

        OnStart();
    } else {
        // Pretend that thread was started and (immediately) stopped.
        ThreadStartedEvent.NotifyAll();
    }

await:
    ThreadStartedEvent.Wait();
}

void TSchedulerThread::Shutdown()
{
    ui64 epoch = Epoch.load(std::memory_order_acquire);
    while (true) {
        if ((epoch & ShutdownEpochMask) != 0x0) {
            // Shutdown requested; await.
            goto await;
        }
        if (Epoch.compare_exchange_strong(epoch, epoch | ShutdownEpochMask, std::memory_order_release)) {
            break;
        }
    }

    if ((epoch & StartedEpochMask) != 0x0) {
        // There is a tiny chance that thread is not started yet, and call to TThread::Join may fail
        // in this case. Ensure proper event sequencing by synchronizing with thread startup.
        ThreadStartedEvent.Wait();

        LOG_DEBUG_IF(EnableLogging, "Stopping thread (Name: %v)", ThreadName);

        CallbackEventCount->NotifyAll();

        BeforeShutdown();

        // Avoid deadlock.
        YCHECK(TThread::CurrentThreadId() != ThreadId);
        Thread.Join();

        AfterShutdown();
    } else {
        // Thread was not started at all.
    }

    ThreadShutdownEvent.NotifyAll();

await:
    ThreadShutdownEvent.Wait();
}

void* TSchedulerThread::ThreadMain(void* opaque)
{
    static_cast<TSchedulerThread*>(opaque)->ThreadMain();
    return nullptr;
}

void TSchedulerThread::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    SetCurrentScheduler(this);
    TThread::CurrentThreadSetName(ThreadName.c_str());

    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        OnThreadStart();
        LOG_DEBUG_IF(EnableLogging, "Thread started (Name: %v)", ThreadName);

        ThreadStartedEvent.NotifyAll();

        while ((Epoch.load(std::memory_order_relaxed) & ShutdownEpochMask) == 0x0) {
            ThreadMainStep();
        }

        OnThreadShutdown();
        LOG_DEBUG_IF(EnableLogging, "Thread stopped (Name: %v)", ThreadName);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)", ThreadName);
    }
}

void TSchedulerThread::ThreadMainStep()
{
    Y_ASSERT(!CurrentFiber);

    if (RunQueue.empty()) {
        // Spawn a new idle fiber to run the loop.
        YCHECK(!IdleFiber);
        IdleFiber = New<TFiber>(BIND(
            &TSchedulerThread::FiberMain,
            MakeStrong(this),
            Epoch.load(std::memory_order_relaxed)));
        RunQueue.push_back(IdleFiber);
    }

    Y_ASSERT(!RunQueue.empty());
    CurrentFiber = std::move(RunQueue.front());
    SetCurrentFiberId(CurrentFiber->GetId());
    RunQueue.pop_front();

    YCHECK(CurrentFiber->GetState() == EFiberState::Suspended);
    CurrentFiber->SetRunning();

    SwitchExecutionContext(
        &SchedulerContext,
        CurrentFiber->GetContext(),
        /* as per TFiber::Trampoline */ CurrentFiber.Get());

    SetCurrentFiberId(InvalidFiberId);

    // Notify context switch subscribers.
    OnContextSwitch();

    auto maybeReleaseIdleFiber = [&] () {
        if (CurrentFiber == IdleFiber) {
            // Advance epoch as this (idle) fiber might be rescheduled elsewhere.
            Epoch.fetch_add(TurnDelta, std::memory_order_relaxed);
            IdleFiber.Reset();
        }
    };

    YCHECK(CurrentFiber);

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

        case EFiberState::Terminated:
            maybeReleaseIdleFiber();
            // We do not own this fiber anymore, so forget about it.
            CurrentFiber.Reset();
            break;

        default:
            Y_UNREACHABLE();
    }

    // Finish sync part of the execution.
    EndExecute();

    // Check for a clear scheduling state.
    Y_ASSERT(!CurrentFiber);
    Y_ASSERT(!WaitForFuture);
    Y_ASSERT(!SwitchToInvoker);
}

void TSchedulerThread::FiberMain(ui64 spawnedEpoch)
{
    {
        auto createdFibers = Profiler.Increment(CreatedFibersCounter);
        auto aliveFibers = Profiler.Increment(AliveFibersCounter, +1);
        LOG_TRACE_IF(EnableLogging, "Fiber started (Name: %v, Created: %v, Alive: %v)",
            ThreadName,
            createdFibers,
            aliveFibers);
    }

    while (FiberMainStep(spawnedEpoch)) {
        // Empty body.
    }

    {
        auto createdFibers = CreatedFibersCounter.Current.load();
        auto aliveFibers = Profiler.Increment(AliveFibersCounter, -1);
        LOG_TRACE_IF(EnableLogging, "Fiber finished (Name: %v, Created: %v, Alive: %v)",
            ThreadName,
            createdFibers,
            aliveFibers);
    }
}

bool TSchedulerThread::FiberMainStep(ui64 spawnedEpoch)
{
    // Call PrepareWait before checking Epoch, which may be modified by
    // a concurrently running Shutdown(), which updates Epoch and then notifies
    // all waiters.
    auto cookie = CallbackEventCount->PrepareWait();

    auto currentEpoch = Epoch.load(std::memory_order_relaxed);
    if ((currentEpoch & ShutdownEpochMask) != 0x0) {
        CallbackEventCount->CancelWait();
        return false;
    }

    // The protocol is that BeginExecute() returns `Success` or `Terminated`
    // if CancelWait was called. Otherwise, it returns `QueueEmpty` requesting
    // to block until a notification.
    auto result = BeginExecute();

    // NB: We might get to this point after a long sleep, and scheduler might spawn
    // another event loop. So we carefully examine scheduler state.
    currentEpoch = Epoch.load(std::memory_order_relaxed);

    // Make the matching call to EndExecute unless it is already done in ThreadMainStep.
    // NB: It is safe to call EndExecute even if no actual action was dequeued and
    // invoked in BeginExecute.
    if (spawnedEpoch == currentEpoch) {
        EndExecute();
    }

    switch (result) {
        case EBeginExecuteResult::QueueEmpty:
            // If the fiber has yielded, we just return control to the scheduler.
            if (spawnedEpoch != currentEpoch || !RunQueue.empty()) {
                CallbackEventCount->CancelWait();
                return false;
            }
            // Actually, await for further notifications.
            CallbackEventCount->Wait(cookie);
            break;
        case EBeginExecuteResult::Success:
            // Note that if someone has called TFiber::GetCanceler and
            // thus has got an ability to cancel the current fiber at any moment,
            // we cannot reuse it.
            // Also, if the fiber has yielded at some point in time,
            // we cannot reuse it as well.
            if (spawnedEpoch != currentEpoch || CurrentFiber->IsCancelable()) {
                return false;
            }
            break;
        case EBeginExecuteResult::Terminated:
            return false;
            break;
    }

    // Reuse the fiber but regenerate its id.
    CurrentFiber->RegenerateId();
    return true;
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
    for (auto it = ContextSwitchCallbacks.rbegin(); it != ContextSwitchCallbacks.rend(); ++it) {
        (*it)();
    }
    ContextSwitchCallbacks.clear();
}

TThreadId TSchedulerThread::GetId() const
{
    return ThreadId;
}

bool TSchedulerThread::IsStarted() const
{
    return (Epoch.load(std::memory_order_relaxed) & StartedEpochMask) != 0x0;
}

bool TSchedulerThread::IsShutdown() const
{
    return (Epoch.load(std::memory_order_relaxed) & ShutdownEpochMask) != 0x0;
}

TFiber* TSchedulerThread::GetCurrentFiber()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    return CurrentFiber.Get();
}

void TSchedulerThread::Return()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YCHECK(CurrentFiber);
    YCHECK(CurrentFiber->IsTerminated());

    SwitchExecutionContext(
        CurrentFiber->GetContext(),
        &SchedulerContext,
        nullptr);

    Y_UNREACHABLE();
}

void TSchedulerThread::PushContextSwitchHandler(std::function<void()> callback)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    ContextSwitchCallbacks.emplace_back(std::move(callback));
}

void TSchedulerThread::PopContextSwitchHandler()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    ContextSwitchCallbacks.pop_back();
}

void TSchedulerThread::YieldTo(TFiberPtr&& other)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    if (!CurrentFiber) {
        YCHECK(other->GetState() == EFiberState::Suspended);
        RunQueue.emplace_back(std::move(other));
        return;
    }

    // Memoize raw pointers.
    auto caller = CurrentFiber.Get();
    auto target = other.Get();
    YCHECK(caller);
    YCHECK(target);

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

    auto fiber = CurrentFiber.Get();
    YCHECK(fiber);

    CheckForCanceledFiber(fiber);

    // Update scheduling state.
    YCHECK(!SwitchToInvoker);
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
    YCHECK(fiber);

    CheckForCanceledFiber(fiber);

    // Update scheduling state.
    YCHECK(!WaitForFuture);
    WaitForFuture = std::move(future);
    YCHECK(!SwitchToInvoker);
    SwitchToInvoker = std::move(invoker);

    fiber->SetSleeping(WaitForFuture);

    SwitchExecutionContext(
        fiber->GetContext(),
        &SchedulerContext,
        nullptr);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    CheckForCanceledFiber(fiber);
}

void TSchedulerThread::OnStart()
{ }

void TSchedulerThread::BeforeShutdown()
{ }

void TSchedulerThread::AfterShutdown()
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

} // namespace NConcurrency
} // namespace NYT

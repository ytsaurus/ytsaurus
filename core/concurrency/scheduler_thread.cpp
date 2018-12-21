#include "scheduler_thread.h"
#include "private.h"
#include "fiber.h"

#include <util/system/sigset.h>

namespace NYT::NConcurrency {

using namespace NYPath;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

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
        YT_LOG_DEBUG("Throwing fiber cancelation exception");
        throw TFiberCanceledException();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSchedulerThread::TSchedulerThread(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName,
    const NProfiling::TTagIdList& tagIds,
    bool enableLogging,
    bool enableProfiling)
    : CallbackEventCount_(std::move(callbackEventCount))
    , ThreadName_(threadName)
    , EnableLogging_(enableLogging)
    , Profiler("/action_queue", tagIds)
    , Thread_(ThreadMain, (void*) this)
    , CreatedFibersCounter_("/created_fibers")
    , AliveFibersCounter_("/alive_fibers")
{
    Profiler.SetEnabled(enableProfiling);
}

TSchedulerThread::~TSchedulerThread()
{
    Shutdown();
}

void TSchedulerThread::Start()
{
    bool alreadyDone = false;
    ui64 epoch;
    while (true) {
        epoch = Epoch_.load(std::memory_order_acquire);
        if ((epoch & StartedEpochMask) != 0x0) {
            // Startup already in progress.
            alreadyDone = true;
            break;
        }
        // Acquire startup lock.
        if (Epoch_.compare_exchange_strong(epoch, epoch | StartedEpochMask, std::memory_order_release)) {
            break;
        }
    }

    if (!alreadyDone) {
        if ((epoch & ShutdownEpochMask) == 0x0) {
            YT_LOG_DEBUG_IF(EnableLogging_, "Starting thread (Name: %v)", ThreadName_);

            try {
                Thread_.Start();
            } catch (const std::exception& ex) {
                // NB: Cannot use logging here since it relies on us.
                auto error = TError("Error starting %v thread", ThreadName_)
                    << ex;
                fprintf(stderr, "%s\n", ToString(error).c_str());
                _exit(100);
            }

            ThreadId_ = TThreadId(Thread_.Id());

            OnStart();
        } else {
            // Pretend that thread was started and (immediately) stopped.
            ThreadStartedEvent_.NotifyAll();
        }
    }

    ThreadStartedEvent_.Wait();
}

void TSchedulerThread::Shutdown()
{
    bool alreadyDone = false;
    ui64 epoch;
    while (true) {
        epoch = Epoch_.load(std::memory_order_acquire);
        if ((epoch & ShutdownEpochMask) != 0x0) {
            // Shutdown requested; await.
            alreadyDone = true;
            break;
        }
        if (Epoch_.compare_exchange_strong(epoch, epoch | ShutdownEpochMask, std::memory_order_release)) {
            break;
        }
    }

    if (!alreadyDone) {
        if ((epoch & StartedEpochMask) != 0x0) {
            // There is a tiny chance that thread is not started yet, and call to TThread::Join may fail
            // in this case. Ensure proper event sequencing by synchronizing with thread startup.
            ThreadStartedEvent_.Wait();

            YT_LOG_DEBUG_IF(EnableLogging_, "Stopping thread (Name: %v)", ThreadName_);

            CallbackEventCount_->NotifyAll();

            BeforeShutdown();

            // Avoid deadlock.
            if (TThread::CurrentThreadId() == ThreadId_) {
                Thread_.Detach();
            } else {
                Thread_.Join();
            }


            AfterShutdown();
        } else {
            // Thread was not started at all.
        }

        ThreadShutdownEvent_.NotifyAll();
    }

    ThreadShutdownEvent_.Wait();
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
    TThread::CurrentThreadSetName(ThreadName_.c_str());

    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        OnThreadStart();
        YT_LOG_DEBUG_IF(EnableLogging_, "Thread started (Name: %v)", ThreadName_);

        ThreadStartedEvent_.NotifyAll();

        while ((Epoch_.load(std::memory_order_relaxed) & ShutdownEpochMask) == 0x0) {
            ThreadMainStep();
        }

        OnThreadShutdown();
        YT_LOG_DEBUG_IF(EnableLogging_, "Thread stopped (Name: %v)", ThreadName_);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)", ThreadName_);
    }
}

void TSchedulerThread::ThreadMainStep()
{
    Y_ASSERT(!CurrentFiber_);

    if (RunQueue_.empty()) {
        // Spawn a new idle fiber to run the loop.
        YCHECK(!IdleFiber_);
        IdleFiber_ = New<TFiber>(BIND(
            &TSchedulerThread::FiberMain,
            MakeStrong(this),
            Epoch_.load(std::memory_order_relaxed)));
        RunQueue_.push_back(IdleFiber_);
    }

    Y_ASSERT(!RunQueue_.empty());
    SetCurrentFiber(std::move(RunQueue_.front()));
    SetCurrentFiberId(CurrentFiber_->GetId());
    RunQueue_.pop_front();

    YCHECK(CurrentFiber_->GetState() == EFiberState::Suspended);
    CurrentFiber_->SetRunning();

    SchedulerContext_.SwitchTo(CurrentFiber_->GetContext());

    SetCurrentFiberId(InvalidFiberId);

    auto optionalReleaseIdleFiber = [&] () {
        if (CurrentFiber_ == IdleFiber_) {
            // Advance epoch as this (idle) fiber might be rescheduled elsewhere.
            Epoch_.fetch_add(TurnDelta, std::memory_order_relaxed);
            IdleFiber_.Reset();
        }
    };

    YCHECK(CurrentFiber_);

    auto savedFiberId = CurrentFiber_->GetId();

    switch (CurrentFiber_->GetState()) {
        case EFiberState::Sleeping:
            optionalReleaseIdleFiber();
            // Reschedule this fiber to wake up later.
            Reschedule(
                std::move(CurrentFiber_),
                std::move(WaitForFuture_),
                std::move(SwitchToInvoker_));
            break;

        case EFiberState::Suspended:
            // Reschedule this fiber to be executed later.
            RunQueue_.emplace_back(std::move(CurrentFiber_));
            break;

        case EFiberState::Terminated:
            optionalReleaseIdleFiber();
            // We do not own this fiber anymore, so forget about it.
            CurrentFiber_.Reset();
            break;

        default:
            Y_UNREACHABLE();
    }

    // Finish sync part of the execution.
    SetCurrentFiberId(savedFiberId);
    EndExecute();
    SetCurrentFiberId(InvalidFiberId);

    // Check for a clear scheduling state.
    Y_ASSERT(!CurrentFiber_);
    Y_ASSERT(!WaitForFuture_);
    Y_ASSERT(!SwitchToInvoker_);
}

void TSchedulerThread::FiberMain(ui64 spawnedEpoch)
{
    {
        auto createdFibers = Profiler.Increment(CreatedFibersCounter_);
        auto aliveFibers = Profiler.Increment(AliveFibersCounter_, 1);
        YT_LOG_TRACE_IF(EnableLogging_, "Fiber started (Name: %v, Created: %v, Alive: %v)",
            ThreadName_,
            createdFibers,
            aliveFibers);
    }

    while (FiberMainStep(spawnedEpoch)) {
        // Empty body.
    }

    {
        auto createdFibers = CreatedFibersCounter_.GetCurrent();
        auto aliveFibers = Profiler.Increment(AliveFibersCounter_, -1);
        YT_LOG_TRACE_IF(EnableLogging_, "Fiber finished (Name: %v, Created: %v, Alive: %v)",
            ThreadName_,
            createdFibers,
            aliveFibers);
    }
}

bool TSchedulerThread::FiberMainStep(ui64 spawnedEpoch)
{
    // Call PrepareWait before checking Epoch, which may be modified by
    // a concurrently running Shutdown(), which updates Epoch and then notifies
    // all waiters.
    auto cookie = CallbackEventCount_->PrepareWait();

    auto currentEpoch = Epoch_.load(std::memory_order_relaxed);
    if ((currentEpoch & ShutdownEpochMask) != 0x0) {
        CallbackEventCount_->CancelWait();
        return false;
    }

    // The protocol is that BeginExecute() returns `Success` or `Terminated`
    // if CancelWait was called. Otherwise, it returns `QueueEmpty` requesting
    // to block until a notification.
    auto result = BeginExecute();

    // NB: We might get to this point after a long sleep, and scheduler might spawn
    // another event loop. So we carefully examine scheduler state.
    currentEpoch = Epoch_.load(std::memory_order_relaxed);

    // Make the matching call to EndExecute unless it is already done in ThreadMainStep.
    // NB: It is safe to call EndExecute even if no actual action was dequeued and
    // invoked in BeginExecute.
    if (spawnedEpoch == currentEpoch) {
        EndExecute();
    }

    switch (result) {
        case EBeginExecuteResult::QueueEmpty:
            // If the fiber has yielded, we just return control to the scheduler.
            if (spawnedEpoch != currentEpoch || !RunQueue_.empty()) {
                CallbackEventCount_->CancelWait();
                return false;
            }
            // Actually, await for further notifications.
            CallbackEventCount_->Wait(cookie);
            break;
        case EBeginExecuteResult::Success:
            // Note that if someone has called TFiber::GetCanceler and
            // thus has got an ability to cancel the current fiber at any moment,
            // we cannot reuse it.
            // Also, if the fiber has yielded at some point in time,
            // we cannot reuse it as well.
            if (spawnedEpoch != currentEpoch || CurrentFiber_->IsCancelable()) {
                return false;
            }
            break;
        case EBeginExecuteResult::Terminated:
            return false;
        default:
            Y_UNREACHABLE();
    }

    // Reuse the fiber but regenerate its id.
    SetCurrentFiberId(CurrentFiber_->RegenerateId());
    return true;
}

void TSchedulerThread::Reschedule(TFiberPtr fiber, TFuture<void> future, IInvokerPtr invoker)
{
    SetCurrentInvoker(invoker, fiber.Get());

    fiber->GetCanceler(); // Initialize canceler; who knows what might happen to this fiber?

    auto resumer = BIND(&ResumeFiber, fiber);
    auto unwinder = BIND(&UnwindFiber, fiber);

    if (future) {
        future.Subscribe(BIND([
            invoker = std::move(invoker),
            fiber = std::move(fiber),
            resumer = std::move(resumer),
            unwinder = std::move(unwinder)
        ] (const TError&) mutable {
            YT_LOG_DEBUG("Waking up fiber %llx", fiber->GetId());
            GuardedInvoke(std::move(invoker), std::move(resumer), std::move(unwinder));
        }));
    } else {
        GuardedInvoke(std::move(invoker), std::move(resumer), std::move(unwinder));
    }
}

TThreadId TSchedulerThread::GetId() const
{
    return ThreadId_;
}

bool TSchedulerThread::IsStarted() const
{
    return (Epoch_.load(std::memory_order_relaxed) & StartedEpochMask) != 0x0;
}

bool TSchedulerThread::IsShutdown() const
{
    return (Epoch_.load(std::memory_order_relaxed) & ShutdownEpochMask) != 0x0;
}

TFiber* TSchedulerThread::GetCurrentFiber()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    return CurrentFiber_.Get();
}

void TSchedulerThread::Return()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    YCHECK(CurrentFiber_);
    YCHECK(CurrentFiber_->IsTerminated());

    CurrentFiber_->GetContext()->SwitchTo(&SchedulerContext_);
    Y_UNREACHABLE();
}

void TSchedulerThread::PushContextSwitchHandler(std::function<void()> out, std::function<void()> in)
{
    CurrentFiber_->PushContextHandler(std::move(out), std::move(in));
}

void TSchedulerThread::PopContextSwitchHandler()
{
    CurrentFiber_->PopContextHandler();
}

void TSchedulerThread::YieldTo(TFiberPtr&& other)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    if (!CurrentFiber_) {
        YCHECK(other->GetState() == EFiberState::Suspended);
        RunQueue_.emplace_back(std::move(other));
        return;
    }

    // Memoize raw pointers.
    auto caller = CurrentFiber_.Get();
    auto target = other.Get();
    YCHECK(caller);
    YCHECK(target);

    // TODO(babenko): handle canceled caller

    RunQueue_.emplace_front(std::move(CurrentFiber_));
    SetCurrentFiber(std::move(other));
    SetCurrentFiberId(target->GetId());

    caller->SetSuspended();
    target->SetRunning();

    caller->GetContext()->SwitchTo(target->GetContext());

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.

    CheckForCanceledFiber(caller);
}

void TSchedulerThread::SwitchTo(IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto fiber = CurrentFiber_.Get();
    YCHECK(fiber);

    CheckForCanceledFiber(fiber);

    // Update scheduling state.
    YCHECK(!SwitchToInvoker_);
    SwitchToInvoker_ = std::move(invoker);

    fiber->SetSleeping();

    SwitchContextFrom(fiber);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.
}

void TSchedulerThread::WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto fiber = CurrentFiber_.Get();
    YCHECK(fiber);

// Check cancellation after wakeup to reduce sync execution time.
#if 0
    CheckForCanceledFiber(fiber);
#endif

    // Update scheduling state.
    YCHECK(!WaitForFuture_);
    WaitForFuture_ = std::move(future);
    YCHECK(!SwitchToInvoker_);
    SwitchToInvoker_ = std::move(invoker);

    fiber->SetSleeping(WaitForFuture_);

    SwitchContextFrom(fiber);

    // Cannot access |this| from this point as the fiber might be resumed
    // in other scheduler.
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

void TSchedulerThread::SwitchContextFrom(TFiber* currentFiber)
{
    currentFiber->InvokeContextOutHandlers();
    currentFiber->GetContext()->SwitchTo(&SchedulerContext_);
    currentFiber->InvokeContextInHandlers();

    CheckForCanceledFiber(currentFiber);
}

void TSchedulerThread::SetCurrentFiber(TFiberPtr fiber)
{
    CurrentFiber_ = std::move(fiber);
    SetCurrentMemoryTag(CurrentFiber_->GetMemoryTag());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#include "scheduler_base.h"
#include "fiber.h"
#include "private.h"
#include "profiling_helpers.h"

#include <yt/core/misc/finally.h>
#include <yt/core/misc/shutdown.h>

#include <util/thread/lfstack.h>

#define REUSE_FIBERS

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerThreadBase::~TSchedulerThreadBase()
{
    Shutdown();
}

template <class TAction>
void CheckedAction(std::atomic<ui64>* atomicEpoch, ui64 mask, TAction&& action)
{
    bool alreadyDone = false;
    ui64 epoch;
    while (true) {
        epoch = atomicEpoch->load(std::memory_order_acquire);
        if (epoch & mask) {
            // Action requested; await.
            alreadyDone = true;
            break;
        }
        if (atomicEpoch->compare_exchange_strong(epoch, epoch | mask, std::memory_order_release)) {
            break;
        }
    }

    if (!alreadyDone) {
        action(epoch);
    }
}

void TSchedulerThreadBase::Start()
{
    CheckedAction(&Epoch_, StartedEpochMask, [&] (ui64 epoch) {
        if (!(epoch & ShutdownEpochMask)) {
            YT_LOG_DEBUG_IF(EnableLogging_, "Starting thread (Name: %v)", ThreadName_);

            try {
                Thread_.Start();
            } catch (const std::exception& ex) {
                fprintf(stderr, "Error starting %s thread\n%s\n",
                    ThreadName_.c_str(),
                    ex.what());

                YT_ABORT();
            }

            ThreadId_ = static_cast<TThreadId>(Thread_.Id());

            OnStart();
        } else {
            // Pretend that thread was started and (immediately) stopped.
            ThreadStartedEvent_.NotifyAll();
        }
    });

    ThreadStartedEvent_.Wait();
}

void TSchedulerThreadBase::Shutdown()
{
    CheckedAction(&Epoch_, ShutdownEpochMask, [&] (ui64 epoch) {
        if (epoch & StartedEpochMask) {
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
    });

    ThreadShutdownEvent_.Wait();
}

void TSchedulerThreadBase::ThreadMain()
{
    TThread::SetCurrentThreadName(ThreadName_.c_str());

    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        OnThreadStart();
        YT_LOG_DEBUG_IF(EnableLogging_, "Thread started (Name: %v)", ThreadName_);

        ThreadStartedEvent_.NotifyAll();

        while (!IsShutdown()) {
            auto cookie = CallbackEventCount_->PrepareWait();

            if (OnLoop(&cookie)) {
                continue;
            }

            if (IsShutdown()) {
                break;
            }

            CallbackEventCount_->Wait(cookie);
        }

        OnThreadShutdown();
        YT_LOG_DEBUG_IF(EnableLogging_, "Thread stopped (Name: %v)", ThreadName_);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)", ThreadName_);
    }
}

////////////////////////////////////////////////////////////////////////////////

thread_local TFiberReusingAdapter* CurrentThread = nullptr;


DECLARE_REFCOUNTED_STRUCT(TRefCountedGauge)

struct TRefCountedGauge
    : public TIntrinsicRefCounted
    , public NProfiling::TSimpleGauge
{
    TRefCountedGauge(const NYPath::TYPath& path, const NProfiling::TTagIdList& tagIds)
        : NProfiling::TSimpleGauge(path, tagIds)
    { }
};

DEFINE_REFCOUNTED_TYPE(TRefCountedGauge)

struct TFiberContext
{
    TExceptionSafeContext ThreadContext;
    TClosure AfterSwitch;
    TFiberPtr ResumerFiber;
    TFiberPtr CurrentFiber;

    TRefCountedGaugePtr WaitingFibersCounter;
};

thread_local TFiberContext* FiberContext = nullptr;

TExceptionSafeContext& ThreadContext()
{
    return FiberContext->ThreadContext;
}

TClosure& AfterSwitch()
{
    return FiberContext->AfterSwitch;
}

TFiberPtr& ResumerFiber()
{
    return FiberContext->ResumerFiber;
}

static TFiberPtr NullFiberPtr;

TFiberPtr& CurrentFiber()
{
    return FiberContext ? FiberContext->CurrentFiber : NullFiberPtr;
}

TRefCountedGaugePtr WaitingFibersCounter()
{
    return FiberContext->WaitingFibersCounter;
}

void SetAfterSwitch(TClosure&& closure)
{
    YT_VERIFY(!AfterSwitch());
    AfterSwitch() = std::move(closure);
}

////////////////////////////////////////////////////////////////////////////////

void SwitchImpl(TExceptionSafeContext* src, TExceptionSafeContext* dest)
{
    src->SwitchTo(dest);

    // Allows set new AfterSwitch inside it.
    if (auto afterSwitch = std::move(AfterSwitch())) {
        YT_VERIFY(!AfterSwitch());
        afterSwitch.Run();
    }

    // TODO: Allow to set after switch inside itself
    YT_VERIFY(!AfterSwitch());
}

void SwitchFromThread(TFiberPtr target)
{
    auto id = target->GetId();
    YT_LOG_TRACE("Switching out thread (TargetFiberId: %llx)", id);

    YT_VERIFY(!CurrentFiber());
    CurrentFiber() = std::move(target);
    SwitchImpl(&ThreadContext(), CurrentFiber()->GetContext());
    YT_VERIFY(!CurrentFiber());

    YT_LOG_TRACE("Switched into thread (InitialFiberId: %llx)", id);
}

void SwitchFromFiber(TFiberPtr target)
{
    auto* currentFiber = CurrentFiber().Get();

    YT_VERIFY(currentFiber);
    currentFiber->OnSwitchOut();

    auto id = currentFiber->GetId();
    YT_LOG_TRACE("Switching out fiber (Id: %llx)", id);

    auto* context = currentFiber->GetContext();
    auto* targetContext = target ? target->GetContext() : &ThreadContext();

    // Here current fiber could be destroyed. But it must be saved in AfterSwitch callback or other place.
    YT_VERIFY(currentFiber->GetRefCount() > 1);
    CurrentFiber() = std::move(target);
    SwitchImpl(context, targetContext);

    YT_LOG_TRACE("Switched into fiber (Id: %llx)", id);

    currentFiber->OnSwitchIn();
}

////////////////////////////////////////////////////////////////////////////////

static NProfiling::TProfiler Profiler("/action_queue");
static NProfiling::TMonotonicCounter CreatedFibersCounter("/created_fibers");
static NProfiling::TSimpleGauge AliveFibersCounter("/alive_fibers");
static NProfiling::TSimpleGauge IdleFibersCounter("/idle_fibers");

void TFiberReusingAdapter::CancelWait()
{
    Cookie_ = std::nullopt;
    CallbackEventCount_->CancelWait();
}

void TFiberReusingAdapter::PrepareWait()
{
    YT_VERIFY(!Cookie_);
    Cookie_ = CallbackEventCount_->PrepareWait();
}

void TFiberReusingAdapter::Wait()
{
    YT_VERIFY(Cookie_);
    CallbackEventCount_->Wait(*Cookie_);
    Cookie_ = std::nullopt;
}

#ifdef REUSE_FIBERS
TLockFreeStack<TFiberPtr> IdleFibers;
#endif

class TDerivedFiber
    : public TFiber
{
public:
    void DoRunNaked();

    void Main();

    ~TDerivedFiber();

private:
    bool Terminated = false;

};

TDerivedFiber::~TDerivedFiber()
{
    YT_VERIFY(Terminated);
}

void TDerivedFiber::DoRunNaked()
{
    YT_VERIFY(!Terminated);

    OnStartRunning();

    Main();

    Terminated = true;

    SwitchFromFiber(nullptr);

    YT_ABORT();
}

void TDerivedFiber::Main()
{
    {
        Profiler.Increment(CreatedFibersCounter);
        Profiler.Increment(AliveFibersCounter, 1);

        YT_LOG_DEBUG("Fiber started");
    }

    auto* currentFiber = CurrentFiber().Get();
    TFiberReusingAdapter* threadThis = nullptr;

    // Break loop to terminate fiber
    while (true) {
        YT_VERIFY(!ResumerFiber());

        threadThis = CurrentThread;

        auto callback = threadThis->BeginExecute();
        YT_VERIFY(threadThis);

        if (callback) {
            threadThis->CancelWait();
            try {
                callback.Run();
            } catch (const TFiberCanceledException&) { }

            callback.Reset();

            currentFiber->ResetForReuse();
            SetCurrentFiberId(currentFiber->GetId());
        } else if (!threadThis->IsShutdown()) {
            threadThis->Wait();
        }

        auto* resumerFiber = ResumerFiber().Get();

        if (resumerFiber) {
            // Suspend current fiber.
            YT_VERIFY(currentFiber);

            NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
#ifdef REUSE_FIBERS
            // Switch out and add fiber to idle fibers.
            // Save fiber in AfterSwitch because it can be immediately concurrently reused.
            SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([current = MakeStrong(currentFiber)] () mutable {
                // Reuse the fiber but regenerate its id.
                current->ResetForReuse();

                YT_LOG_TRACE("Make fiber idle (FiberId: %llx)",
                    current->GetId());

                Profiler.Increment(IdleFibersCounter, 1);
                IdleFibers.Enqueue(std::move(current));
            }));

            // Switched to ResumerFiber or thread main.
            SwitchFromFiber(std::move(ResumerFiber()));
#else
            SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([
                current = MakeStrong(currentFiber),
                resume = std::move(ResumerFiber())
            ] () mutable {
                current.Reset();
                SwitchFromThread(std::move(resume));
            }));

            break;
#endif
        }

        // Renew thread pointer.
        threadThis = CurrentThread;

#ifdef REUSE_FIBERS
        // currentFiber->IsCanceled() used in shutdown to destroy idle fibers
        if (!threadThis || threadThis->IsShutdown() || currentFiber->IsCanceled()) {
#else
        YT_VERIFY(!currentFiber->IsCanceled());
        if (!threadThis || threadThis->IsShutdown()) {
#endif
            // Do not reuse fiber in this rear case. Otherwise too many idle fibers are collected.
            YT_VERIFY(!ResumerFiber());

            NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
            SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([current = MakeStrong(currentFiber)] () mutable {
                YT_LOG_TRACE("Destroying fiber (FiberId: %llx)",
                    current->GetId());

                current.Reset();
            }));

            break;
        }

        threadThis->EndExecute();
        threadThis->PrepareWait();
    }

    {
        YT_LOG_DEBUG("Fiber finished");

        Profiler.Increment(AliveFibersCounter, -1);
    }

    // Terminating fiber.
}

#ifdef REUSE_FIBERS

void DestroyIdleFibers()
{
    std::vector<TFiberPtr> fibers;
    IdleFibers.DequeueAll(&fibers);

    TFiberContext fiberContext;

    FiberContext = &fiberContext;
    auto finally = Finally([] {
        FiberContext = nullptr;
    });

    for (const auto& fiber : fibers) {
        YT_VERIFY(fiber->GetRefCount() == 1);
        fiber->GetCanceler().Run(TError("Idle fiber destroyed"));

        SwitchFromThread(std::move(fiber));
    }

    fibers.clear();
}

REGISTER_SHUTDOWN_CALLBACK(0, DestroyIdleFibers)

#endif

bool TFiberReusingAdapter::OnLoop(TEventCount::TCookie* cookie)
{
    Cookie_ = *cookie;

    TFiberContext fiberContext;

    fiberContext.WaitingFibersCounter = New<TRefCountedGauge>(
        "/waiting_fibers",
        GetThreadTagIds(true, ThreadName_));

    CurrentThread = this;
    FiberContext = &fiberContext;
    auto finally = Finally([] {
        CurrentThread = nullptr;
        FiberContext = nullptr;
    });

    TFiberPtr fiber = New<TDerivedFiber>();

    SwitchFromThread(std::move(fiber));
    // Can return from WaitFor if there are no idle fibers.

    // Used when fiber yielded.
    EndExecute();

    // Result depends on last BeginExecute result (CancelWait called or not)
    // should set proper cookie

    if (Cookie_) {
        *cookie = *Cookie_;
    }

    return !Cookie_;
}

void ResumeFiber(TFiberPtr fiber)
{
    YT_LOG_TRACE("Resuming fiber (TargetFiberId: %llx)", fiber->GetId());

    YT_VERIFY(CurrentFiber());
    ResumerFiber() = CurrentFiber();
    SwitchFromFiber(std::move(fiber));
    YT_VERIFY(!ResumerFiber());
}

void UnwindFiber(TFiberPtr fiber)
{
    YT_LOG_TRACE("Unwinding fiber (TargetFiberId: %llx)", fiber->GetId());

    fiber->GetCanceler().Run(TError("Fiber is unwinding"));

    GetFinalizerInvoker()->Invoke(
        BIND_DONT_CAPTURE_TRACE_CONTEXT(&ResumeFiber, Passed(std::move(fiber))));
}

// Guard reduces frame count in backtrace.
class TResumeGuard
{
public:
    explicit TResumeGuard(TFiberPtr fiber)
        : Fiber_(std::move(fiber))
    { }

    explicit TResumeGuard(TResumeGuard&& other)
        : Fiber_(std::move(other.Fiber_))
    { }

    TResumeGuard(const TResumeGuard&) = delete;

    TResumeGuard& operator=(const TResumeGuard&) = delete;
    TResumeGuard& operator=(TResumeGuard&&) = delete;

    void operator()()
    {
        YT_VERIFY(Fiber_);
        ResumeFiber(std::move(Fiber_));
    }

    ~TResumeGuard()
    {
        if (Fiber_) {
            UnwindFiber(std::move(Fiber_));
        }
    }

private:
    TFiberPtr Fiber_;
};

// Handler to support native fibers in Perl bindings.
thread_local IScheduler* CurrentScheduler;

void SetCurrentScheduler(IScheduler* scheduler)
{
    YT_VERIFY(!CurrentScheduler);
    CurrentScheduler = scheduler;
}

void WaitUntilSet(TFuture<void> future, IInvokerPtr invoker)
{
    YT_VERIFY(future);
    YT_ASSERT(invoker);

    if (CurrentScheduler) {
        CurrentScheduler->WaitUntilSet(std::move(future), std::move(invoker));
        return;
    }

    if (auto* currentFiber = CurrentFiber().Get()) {
        if (currentFiber->IsCanceled()) {
            future.Cancel(currentFiber->GetCancelationError());
        }

        currentFiber->SetFuture(future);
        auto finally = Finally([&] {
            currentFiber->ResetFuture();
        });

        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([
            invoker = std::move(invoker),
            future = std::move(future),
            fiber = MakeStrong(currentFiber)
        ] () mutable {
            future.Subscribe(BIND_DONT_CAPTURE_TRACE_CONTEXT([
                invoker = std::move(invoker),
                fiber = std::move(fiber)
            ] (const TError&) mutable {
                YT_LOG_DEBUG("Waking up fiber (TargetFiberId: %llx)",
                    fiber->GetId());
                invoker->Invoke(BIND(TResumeGuard{std::move(fiber)}));
            }));
        }));

        // Switch to resumer.
        auto switchTarget = std::move(ResumerFiber());
        YT_ASSERT(!ResumerFiber());

        // If there is no resumer switch to idle fiber. Or switch to thread main.
#ifdef REUSE_FIBERS
        if (!switchTarget) {
            IdleFibers.Dequeue(&switchTarget);

            if (switchTarget) {
                Profiler.Increment(IdleFibersCounter, -1);
            }
        }
#endif

        currentFiber->InvokeContextOutHandlers();

        auto waitingFibersCounter = WaitingFibersCounter();

        Profiler.Increment(*waitingFibersCounter, 1);
        SwitchFromFiber(std::move(switchTarget));
        Profiler.Increment(*waitingFibersCounter, -1);

        // ContextInHandlers should be invoked before unwinding during cancelation.
        currentFiber->InvokeContextInHandlers();

        if (currentFiber->IsCanceled()) {
            YT_LOG_DEBUG("Throwing fiber cancelation exception");
            throw TFiberCanceledException();
        }

    } else {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

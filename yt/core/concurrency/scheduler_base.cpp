#include "scheduler_base.h"
#include "fiber.h"
#include "private.h"

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

void TSchedulerThreadBase::Start()
{
    CheckedAction(StartedEpochMask, [&] (ui64 epoch) {
        if (!(epoch & ShutdownEpochMask)) {
            YT_LOG_DEBUG_IF(EnableLogging_, "Starting thread (Name: %v)", ThreadName_);

            try {
                Thread_.Start();
            } catch (const std::exception& ex) {
                fprintf(stderr, "Error starting %s thread\n%s\n",
                    ThreadName_.c_str(),
                    ex.what());
                _exit(100);
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
    CheckedAction(ShutdownEpochMask, [&] (ui64 epoch) {
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

            YT_LOG_DEBUG_IF(EnableLogging_, "On loop");
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

NProfiling::TProfiler Profiler("/action_queue");
NProfiling::TMonotonicCounter CreatedFibersCounter("/created_fibers");
NProfiling::TSimpleGauge AliveFibersCounter("/alive_fibers");
NProfiling::TSimpleGauge WaitingFibersCounter("/waiting_fibers");
NProfiling::TSimpleGauge IdleFibersCounter("/idle_fibers");

thread_local TFiberPtr ResumerFiber = nullptr;
thread_local TFiberReusingAdapter* TFiberReusingAdapter::ThreadThis_ = nullptr;

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

// Fiber main is specific to TFiberReusingAdapter, so it must be stored in fiber cache accessible by only
void TFiberReusingAdapter::FiberMain()
{
    {
        Profiler.Increment(CreatedFibersCounter);
        Profiler.Increment(AliveFibersCounter, 1);

        YT_LOG_DEBUG("Fiber started");
    }

    // Break loop to terminate fiber
    while (true) {
        YT_VERIFY(!ResumerFiber);

        auto callback = ThreadThis_->BeginExecute();
        YT_VERIFY(ThreadThis_);

        if (callback) {
            ThreadThis_->CancelWait();
            try {
                callback.Run();
            } catch (const TFiberCanceledException&) { }

            callback.Reset();
        } else if (!ThreadThis_->IsShutdown()) {
            ThreadThis_->Wait();
        }

        if (ResumerFiber) {
            // Suspend current fiber.
            YT_VERIFY(CurrentFiber);

            NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
            YT_VERIFY(!AfterSwitch);
#ifdef REUSE_FIBERS
            // Switch out and add fiber to idle fibers.
            // Save fiber in AfterSwitch because it can be immediately concurrently reused.
            AfterSwitch = BIND_DONT_CAPTURE_TRACE_CONTEXT([current = CurrentFiber] () mutable {
                // Reuse the fiber but regenerate its id.
                current->ResetForReuse();

                YT_LOG_DEBUG("Make fiber idle (FiberId: %llx)",
                    current->GetId());

                Profiler.Increment(IdleFibersCounter, 1);
                IdleFibers.Enqueue(std::move(current));
            });

            // Switched to ResumerFiber or thread main.
            SwitchToFiber(std::move(ResumerFiber));
#else
            AfterSwitch = BIND_DONT_CAPTURE_TRACE_CONTEXT([
                current = CurrentFiber,
                resume = std::move(ResumerFiber)
            ] () mutable {
                current.Reset();
                SwitchToFiber(std::move(resume));
            });

            break;
#endif
        } else {
            CurrentFiber->ResetForReuse();
            SetCurrentFiberId(CurrentFiber->GetId());
        }

        // CurrentFiber->IsCanceled() used in shutdown to destroy idle fibers
        if (!ThreadThis_ || ThreadThis_->IsShutdown() || CurrentFiber->IsCanceled()) {
            // Do not reuse fiber in this rear case. Otherwise too many idle fibers are collected.
            YT_VERIFY(!AfterSwitch);
            YT_VERIFY(!ResumerFiber);

            NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
            YT_VERIFY(!AfterSwitch);
            AfterSwitch = BIND_DONT_CAPTURE_TRACE_CONTEXT([current = CurrentFiber] () mutable {
                YT_LOG_DEBUG("Destroying fiber (FiberId: %llx)",
                    current->GetId());

                current.Reset();
            });

            break;
        }

        ThreadThis_->EndExecute();
        ThreadThis_->PrepareWait();
    }

    {
        YT_LOG_DEBUG("Fiber finished");

        Profiler.Increment(AliveFibersCounter, -1);
    }

    // Terminating fiber.
}

void TFiberReusingAdapter::DestroyIdleFibers()
{
    std::vector<TFiberPtr> fibers;
    IdleFibers.DequeueAll(&fibers);

    for (auto& fiber : fibers) {
        YT_VERIFY(fiber->GetRefCount() == 1);
        fiber->Cancel();

        SwitchToFiber(std::move(fiber));
    }

    fibers.clear();
}

REGISTER_SHUTDOWN_CALLBACK(0, TFiberReusingAdapter::DestroyIdleFibers)

bool TFiberReusingAdapter::OnLoop(TEventCount::TCookie* cookie)
{
    Cookie_ = *cookie;

    ThreadThis_ = this;
    auto finally = Finally([] {
        ThreadThis_ = nullptr;
    });

    TFiberPtr fiber = New<TFiber>(BIND_DONT_CAPTURE_TRACE_CONTEXT(
        &FiberMain));

    YT_VERIFY(!CurrentFiber);
    SwitchToFiber(std::move(fiber));
    // Can return from WaitFor if there are no idle fibers.
    YT_VERIFY(!CurrentFiber);

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
    YT_LOG_DEBUG("Resuming fiber (TargetFiberId: %llx)", fiber->GetId());

    YT_VERIFY(CurrentFiber);
    ResumerFiber = CurrentFiber;
    YT_VERIFY(!AfterSwitch);
    SwitchToFiber(std::move(fiber));
    YT_VERIFY(!ResumerFiber);
}

void UnwindFiber(TFiberPtr fiber)
{
    fiber->Cancel();

    YT_LOG_DEBUG("Unwinding fiber (TargetFiberId: %llx)", fiber->GetId());

    GetFinalizerInvoker()->Invoke(
        BIND_DONT_CAPTURE_TRACE_CONTEXT(&ResumeFiber, Passed(std::move(fiber))));
}

// Guard reduces frame count in backtrace.
struct TResumeGuard
{
    explicit TResumeGuard(TFiberPtr fiber)
        : Fiber(std::move(fiber))
    { }

    explicit TResumeGuard(TResumeGuard&& other)
        : Fiber(std::move(other.Fiber))
    { }

    TResumeGuard(const TResumeGuard&) = delete;
    TResumeGuard& operator=(const TResumeGuard&) = delete;

    TResumeGuard& operator=(TResumeGuard&&) = delete;

    TFiberPtr Fiber;

    void operator()()
    {
        YT_VERIFY(Fiber);
        ResumeFiber(std::move(Fiber));
    }

    ~TResumeGuard()
    {
        if (!Fiber) {
            return;
        }

        UnwindFiber(std::move(Fiber));
    }
};

// Handler to support native fibers in Perl bindings.
thread_local IScheduler* CurrentScheduler;

void SetCurrentScheduler(IScheduler* scheduler)
{
    YT_VERIFY(!CurrentScheduler);
    CurrentScheduler = scheduler;
}

void WaitForImpl(TAwaitable awaitable, IInvokerPtr invoker)
{
    if (CurrentScheduler) {
        CurrentScheduler->WaitFor(std::move(awaitable), std::move(invoker));
        return;
    }

    if (CurrentFiber) {
        if (CurrentFiber->IsCanceled()) {
            // TODO(lukyan): Cancel awaitable.
            YT_LOG_DEBUG("Throwing fiber cancelation exception");
            throw TFiberCanceledException();
        }

        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        YT_VERIFY(!AfterSwitch);
        AfterSwitch = BIND_DONT_CAPTURE_TRACE_CONTEXT([=, fiber = CurrentFiber, once = true] () mutable {
            YT_VERIFY(once);
            once = false;

            auto awaitableId = THash<NYT::TAwaitable>()(awaitable);

            YT_LOG_DEBUG("Rescheduling fiber (TargetFiberId: %llx, AwaitableId: %llx)",
                fiber->GetId(),
                awaitableId);

            if (awaitable) {
                awaitable.Subscribe(BIND_DONT_CAPTURE_TRACE_CONTEXT([
                    invoker = std::move(invoker),
                    fiber = std::move(fiber),
                    awaitableId,
                    once = true
                ] () mutable {
                    YT_VERIFY(once);
                    once = false;

                    YT_LOG_DEBUG("Waking up fiber (TargetFiberId: %llx, AwaitableId: %llx)",
                        fiber->GetId(),
                        awaitableId);
                    invoker->Invoke(BIND(TResumeGuard{std::move(fiber)}));
                }));
            } else {
                invoker->Invoke(BIND(TResumeGuard{std::move(fiber)}));
            }
        });

        // Switch to resumer.
        auto switchTarget = std::move(ResumerFiber);
        YT_VERIFY(!ResumerFiber);

        // If there is no resumer switch to idle fiber. Or switch to thread main.
#ifdef REUSE_FIBERS
        if (!switchTarget) {
            IdleFibers.Dequeue(&switchTarget);

            if (switchTarget) {
                Profiler.Increment(IdleFibersCounter, -1);
            }
        }
#endif

        // TODO(lukyan): Set awaitable (or cancel handler) and return guard.
        CurrentFiber->SetAwaitable(awaitable);
        CurrentFiber->InvokeContextOutHandlers();

        Profiler.Increment(WaitingFibersCounter, 1);
        SwitchToFiber(std::move(switchTarget));
        Profiler.Increment(WaitingFibersCounter, -1);

        // ContextInHandlers should be invoked before unwinding during cancelation.
        CurrentFiber->InvokeContextInHandlers();

        if (CurrentFiber->IsCanceled()) {
            YT_LOG_DEBUG("Throwing fiber cancelation exception");
            throw TFiberCanceledException();
        }

        CurrentFiber->ResetAwaitable();
    } else {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
    }
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

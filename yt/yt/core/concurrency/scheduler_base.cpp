#include "scheduler_base.h"
#include "fiber.h"
#include "private.h"
#include "profiling_helpers.h"
#include "fls.h"

#include <yt/core/misc/finally.h>
#include <yt/core/misc/shutdown.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <util/thread/lfstack.h>

#define REUSE_FIBERS

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSchedulerThreadBase::TSchedulerThreadBase(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName)
    : CallbackEventCount_(std::move(callbackEventCount))
    , ThreadName_(threadName)
    , Thread_(ThreadTrampoline, (void*) this)
{ }

TSchedulerThreadBase::~TSchedulerThreadBase()
{
    Shutdown();
}

void TSchedulerThreadBase::OnStart()
{ }

void TSchedulerThreadBase::BeforeShutdown()
{ }

void TSchedulerThreadBase::AfterShutdown()
{ }

void TSchedulerThreadBase::OnThreadStart()
{ }

void TSchedulerThreadBase::OnThreadShutdown()
{ }

TThreadId TSchedulerThreadBase::GetId() const
{
    return ThreadId_;
}

bool TSchedulerThreadBase::IsStarted() const
{
    return Epoch_.load(std::memory_order_relaxed) & StartingEpochMask;
}

bool TSchedulerThreadBase::IsShutdown() const
{
    return Epoch_.load(std::memory_order_relaxed) & StoppingEpochMask;
}

void* TSchedulerThreadBase::ThreadTrampoline(void* opaque)
{
    static_cast<TSchedulerThreadBase*>(opaque)->ThreadMain();
    return nullptr;
}

void TSchedulerThreadBase::Start()
{
    CheckedAction(&Epoch_, StartingEpochMask, [&] (ui64 epoch) {
        if (!(epoch & StoppingEpochMask)) {
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
}

void TSchedulerThreadBase::Shutdown()
{
    CheckedAction(&Epoch_, StoppingEpochMask, [&] (ui64 epoch) {
        if (epoch & StartingEpochMask) {
            // There is a tiny chance that thread is not started yet, and call to TThread::Join may fail
            // in this case. Ensure proper event sequencing by synchronizing with thread startup.
            ThreadStartedEvent_.Wait();

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
        YT_LOG_DEBUG("Thread started (Name: %v)", ThreadName_);

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
        YT_LOG_DEBUG("Thread stopped (Name: %v)", ThreadName_);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)", ThreadName_);
    }
}

////////////////////////////////////////////////////////////////////////////////

thread_local TFiberReusingAdapter* CurrentThread = nullptr;

struct TFiberContext
{
    TExceptionSafeContext ThreadContext;
    TClosure AfterSwitch;
    TFiberPtr ResumerFiber;
    TFiberPtr CurrentFiber;
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

static NProfiling::TCounter CreatedFibersCounter = NProfiling::TRegistry{"/action_queue"}.Counter("/created_fibers");

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

NYTAlloc::TMemoryTag SwapMemoryTag(NYTAlloc::TMemoryTag tag)
{
    auto result = NYTAlloc::GetCurrentMemoryTag();
    NYTAlloc::SetCurrentMemoryTag(tag);
    return result;
}

NYTAlloc::EMemoryZone SwapMemoryZone(NYTAlloc::EMemoryZone zone)
{
    auto result = NYTAlloc::GetCurrentMemoryZone();
    NYTAlloc::SetCurrentMemoryZone(zone);
    return result;
}

class TSwitchHandler
{
public:
    TSwitchHandler()
    {
        PushContextHandler(
            [this] () noexcept {
                // Now values may differ from initial.
                OnSwitch();
            }, [this] () noexcept {
                OnSwitch();
            });
    }

    TSwitchHandler(const TSwitchHandler&) = delete;
    TSwitchHandler(TSwitchHandler&&) = delete;

    ~TSwitchHandler()
    {
        PopContextHandler();

        YT_VERIFY(TraceContext_.Get() == nullptr);
        YT_VERIFY(MemoryTag_ == NYTAlloc::NullMemoryTag);
        YT_VERIFY(MemoryZone_ == NYTAlloc::EMemoryZone::Normal);
        YT_VERIFY(FsdHolder_ == nullptr);
    }

    void OnSwitch()
    {
        // In user defined context switch callbacks (ContextSwitchGuard) Swap must be used.
        // It preserves context from fiber resumer.

        TraceContext_ = NTracing::SwitchTraceContext(TraceContext_);
        MemoryTag_ = SwapMemoryTag(MemoryTag_);
        MemoryZone_ = SwapMemoryZone(MemoryZone_);
        FsdHolder_ = NDetail::SetCurrentFsdHolder(FsdHolder_);
    }

private:
    NYTAlloc::TMemoryTag MemoryTag_ = NYTAlloc::NullMemoryTag;
    NYTAlloc::EMemoryZone MemoryZone_ = NYTAlloc::EMemoryZone::Normal;
    NTracing::TTraceContextPtr TraceContext_ = nullptr;
    NDetail::TFsdHolder* FsdHolder_ = nullptr;
};

namespace {

void RunInFiberContext(TClosure callback)
{
    NDetail::TFsdHolder fsdHolder;
    TSwitchHandler switchHandler;

    auto oldFsd = NDetail::SetCurrentFsdHolder(&fsdHolder);
    YT_VERIFY(oldFsd == nullptr);
    auto finally = Finally([&] {
        auto oldFsd = NDetail::SetCurrentFsdHolder(nullptr);
        YT_VERIFY(oldFsd == &fsdHolder);

        // Supports case when current fiber has been resumed, but finished without WaitFor.
        // There is preserved context of resumer fiber saved in switchHandler. Restore it.
        // If there are no values for resumer the following call will swap null with null.
        switchHandler.OnSwitch();
    });

    callback.Run();
}

} // namespace

void TDerivedFiber::Main()
{
    {
        CreatedFibersCounter.Increment();
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
                RunInFiberContext(std::move(callback));
            } catch (const TFiberCanceledException&) { }

            currentFiber->ResetForReuse();
            SetCurrentFiberId(currentFiber->GetId());
        } else if (!threadThis->IsShutdown()) {
            threadThis->Wait();
        }

        auto* resumerFiber = ResumerFiber().Get();

        // Trace context can be restored for resumer fiber, so current trace context and memory tag are
        // not necessarily null. Check them after switch from and returning into current fiber.

        if (resumerFiber) {
            // Suspend current fiber.
            YT_VERIFY(currentFiber);

#ifdef REUSE_FIBERS

            {
                NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
                // Switch out and add fiber to idle fibers.
                // Save fiber in AfterSwitch because it can be immediately concurrently reused.
                SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([current = MakeStrong(currentFiber)] () mutable {
                    // Reuse the fiber but regenerate its id.
                    current->ResetForReuse();

                    YT_LOG_TRACE("Making fiber idle (FiberId: %llx)",
                        current->GetId());

                    IdleFibers.Enqueue(std::move(current));
                }));
            }

            // Switched to ResumerFiber or thread main.
            SwitchFromFiber(std::move(ResumerFiber()));
#else
            {
                NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
                SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([
                    current = MakeStrong(currentFiber),
                    resume = std::move(ResumerFiber())
                ] () mutable {
                    current.Reset();
                    SwitchFromThread(std::move(resume));
                }));
            }

            break;
#endif
        }

        // Renew thread pointer.
        threadThis = CurrentThread;

#ifdef REUSE_FIBERS
        // currentFiber->IsCanceled() used in shutdown to destroy idle fibers
        if (!threadThis || threadThis->IsShutdown() || currentFiber->IsCanceled())
#else
        YT_VERIFY(!currentFiber->IsCanceled());
        if (!threadThis || threadThis->IsShutdown())
#endif
        {
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

    TResumeGuard(TResumeGuard&& other)
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

TFiberPtr GetYieldTarget()
{
    // Switch to resumer.
    auto targetFiber = std::move(ResumerFiber());
    YT_ASSERT(!ResumerFiber());

    // If there is no resumer switch to idle fiber. Or switch to thread main.
#ifdef REUSE_FIBERS
    if (!targetFiber) {
        IdleFibers.Dequeue(&targetFiber);
    }
#endif
    return targetFiber;
}

void BaseYield(TClosure afterSwitch)
{
    YT_VERIFY(CurrentFiber());

    SetAfterSwitch(std::move(afterSwitch));

    // Switch to resumer.
    auto switchTarget = GetYieldTarget();

    SwitchFromFiber(std::move(switchTarget));

    YT_VERIFY(ResumerFiber());
}

void Yield(TClosure afterSwitch)
{
    auto* currentFiber = CurrentFiber().Get();
    currentFiber->InvokeContextOutHandlers();
    BaseYield(std::move(afterSwitch));
    currentFiber->InvokeContextInHandlers();
}

void WaitUntilSet(TFuture<void> future, IInvokerPtr invoker)
{
    YT_VERIFY(future);
    YT_ASSERT(invoker);

    if (CurrentScheduler) {
        CurrentScheduler->WaitUntilSet(std::move(future), std::move(invoker));
        return;
    }

    auto* currentFiber = CurrentFiber().Get();

    if (!currentFiber) {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
        YT_VERIFY(future.TimedWait(TInstant::Max()));
        return;
    }

    if (currentFiber->IsCanceled()) {
        future.Cancel(currentFiber->GetCancelationError());
    }

    currentFiber->SetFuture(future);
    auto finally = Finally([&] {
        currentFiber->ResetFuture();
    });

    TClosure afterSwitch;
    {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        afterSwitch = BIND_DONT_CAPTURE_TRACE_CONTEXT([
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

                invoker->Invoke(BIND_DONT_CAPTURE_TRACE_CONTEXT(TResumeGuard{std::move(fiber)}));
            }));
        });
    };

    Yield(std::move(afterSwitch));

    if (currentFiber->IsCanceled()) {
        YT_LOG_DEBUG("Throwing fiber cancelation exception");
        throw TFiberCanceledException();
    }
}

////////////////////////////////////////////////////////////////////////////////

TFiberReusingAdapter::TFiberReusingAdapter(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName)
    : TSchedulerThreadBase(
        callbackEventCount,
        threadName)
{ }

TFiberReusingAdapter::TFiberReusingAdapter(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName,
    const NProfiling::TTagSet& /*tags*/)
    : TFiberReusingAdapter(
        std::move(callbackEventCount),
        std::move(threadName))
{ }

bool TFiberReusingAdapter::OnLoop(TEventCount::TCookie* cookie)
{
    Cookie_ = *cookie;

    TFiberContext fiberContext;

    CurrentThread = this;
    FiberContext = &fiberContext;
    auto finally = Finally([] {
        CurrentThread = nullptr;
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

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

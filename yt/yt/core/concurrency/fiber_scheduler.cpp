#include "fiber_scheduler.h"

#include "profiling_helpers.h"
#include "execution_stack.h"
#include "atomic_flag_spinlock.h"
#include "private.h"

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <util/thread/lfstack.h>

#include <util/system/context.h>

#define REUSE_FIBERS

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRefCountedGauge)

class TRefCountedGauge
    : public TRefCounted
    , public NProfiling::TGauge
{
public:
    explicit TRefCountedGauge(const NProfiling::TRegistry& profiler)
        : NProfiling::TGauge(profiler.Gauge("/waiting_fibers"))
    { }

    void Increment(i64 delta)
    {
        auto value = Value_.fetch_add(delta, std::memory_order_relaxed) + delta;
        NProfiling::TGauge::Update(value);
    }

private:
    std::atomic<i64> Value_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TRefCountedGauge)

////////////////////////////////////////////////////////////////////////////////

void RunInFiberContext(TClosure callback);

void SwitchFromThread(TFiberPtr target);

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Merge with TFiberContext?
thread_local TFiberScheduler* CurrentThread = nullptr;

// Non POD TLS sometimes does not work correctly in dynamic library.
struct TFiberContext
{
    TExceptionSafeContext ThreadContext;
    TClosure AfterSwitch;
    TFiberPtr ResumerFiber;
    TFiberPtr CurrentFiber;

    TRefCountedGaugePtr WaitingFibersCounter;
};

thread_local TFiberContext* FiberContext = nullptr;

////////////////////////////////////////////////////////////////////////////////

TFiberScheduler::TFiberScheduler(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName)
    : TSchedulerThreadBase(
        callbackEventCount,
        threadName)
{ }

TFiberScheduler::TFiberScheduler(
    std::shared_ptr<TEventCount> callbackEventCount,
    const TString& threadName,
    const NProfiling::TTagSet& /*tags*/)
    : TFiberScheduler(
        std::move(callbackEventCount),
        std::move(threadName))
{ }

void TFiberScheduler::CancelWait()
{
    Cookie_ = std::nullopt;
    CallbackEventCount_->CancelWait();
}

void TFiberScheduler::PrepareWait()
{
    YT_VERIFY(!Cookie_);
    Cookie_ = CallbackEventCount_->PrepareWait();
}

void TFiberScheduler::Wait()
{
    YT_VERIFY(Cookie_);
    CallbackEventCount_->Wait(*Cookie_);
    Cookie_ = std::nullopt;
}

bool TFiberScheduler::OnLoop(TEventCount::TCookie* cookie)
{
    Cookie_ = *cookie;

    TFiberContext fiberContext;

    fiberContext.WaitingFibersCounter = New<TRefCountedGauge>(
        NProfiling::TRegistry{"/action_queue"}.WithTag("thread", ThreadName_));

    CurrentThread = this;
    FiberContext = &fiberContext;
    auto finally = Finally([] {
        CurrentThread = nullptr;
        FiberContext = nullptr;
    });

    auto fiber = New<TFiber>();

    SwitchFromThread(std::move(fiber));
    // Can return from WaitFor if there are no idle fibers.

    // Called when fiber was yielded.
    EndExecute();

    // Result depends on last BeginExecute result (CancelWait called or not)
    // should set proper cookie.

    if (Cookie_) {
        *cookie = *Cookie_;
    }

    return !Cookie_;
}

////////////////////////////////////////////////////////////////////////////////

class TFiberRegistry
{
public:
    std::list<TFiber*>::iterator Register(TFiber* fiber)
    {
        TGuard<std::atomic_flag> guard(Lock_);
        return Fibers_.insert(Fibers_.begin(), fiber);
    }

    void Unregister(std::list<TFiber*>::iterator iterator)
    {
        TGuard<std::atomic_flag> guard(Lock_);
        Fibers_.erase(iterator);
    }

private:
    std::atomic_flag Lock_ = ATOMIC_FLAG_INIT;
    std::list<TFiber*> Fibers_;

};

TFiberRegistry* GetFiberRegistry()
{
    return Singleton<TFiberRegistry>();
}

////////////////////////////////////////////////////////////////////////////////

class TFiberExecutionStackProfiler
    : public ISensorProducer
{
public:
    TFiberExecutionStackProfiler()
    {
        TProfiler{""}.AddProducer("/fiber_execution_stack", MakeStrong(this));
    }

    void StackAllocated(int stackSize)
    {
        BytesAllocated_.fetch_add(stackSize, std::memory_order_relaxed);
        BytesAlive_.fetch_add(stackSize, std::memory_order_relaxed);
    }

    void StackFreed(int stackSize)
    {
        BytesFreed_.fetch_add(stackSize, std::memory_order_relaxed);
        BytesAlive_.fetch_sub(stackSize, std::memory_order_relaxed);
    }

    void CollectSensors(ISensorWriter* writer)
    {
        writer->AddCounter("/bytes_allocated", BytesAllocated_);
        writer->AddCounter("/bytes_freed", BytesFreed_);
        writer->AddGauge("/bytes_alive", BytesAlive_);
    }

    static TFiberExecutionStackProfiler* Get()
    {
        struct TLeaker
        {
            TIntrusivePtr<TFiberExecutionStackProfiler> Ptr = New<TFiberExecutionStackProfiler>();
        };

        return LeakySingleton<TLeaker>()->Ptr.Get();
    }

private:
    std::atomic<i64> BytesAllocated_ = 0;
    std::atomic<i64> BytesFreed_ = 0;
    std::atomic<i64> BytesAlive_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFiber
    : public TRefCounted
    , public ITrampoLine
{
public:
    explicit TFiber(EExecutionStackKind stackKind = EExecutionStackKind::Small)
        : Stack_(CreateExecutionStack(stackKind))
        , Context_({
            this,
            TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize())})
        , Registry_(GetFiberRegistry())
        , Iterator_(Registry_->Register(this))
    {
        TFiberExecutionStackProfiler::Get()->StackAllocated(Stack_->GetSize());
    }

    ~TFiber()
    {
        YT_VERIFY(Terminated);
        TFiberExecutionStackProfiler::Get()->StackFreed(Stack_->GetSize());
        GetFiberRegistry()->Unregister(Iterator_);
    }

    bool CheckFreeStackSpace(size_t space) const
    {
        return reinterpret_cast<char*>(Stack_->GetStack()) + space < __builtin_frame_address(0);
    }

    TExceptionSafeContext* GetContext()
    {
        return &Context_;
    }

    virtual void DoRunNaked() override;

private:
    std::shared_ptr<TExecutionStack> Stack_;
    TExceptionSafeContext Context_;

    // No way to select static/thread_local variable in GDB from particular shared library.
    TFiberRegistry* const Registry_;
    const std::list<TFiber*>::iterator Iterator_;
    bool Terminated = false;
};

DEFINE_REFCOUNTED_TYPE(TFiber)

////////////////////////////////////////////////////////////////////////////////

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

TRefCountedGaugePtr GetWaitingFibersCounter()
{
    return FiberContext->WaitingFibersCounter;
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
    YT_VERIFY(!CurrentFiber());
    CurrentFiber() = std::move(target);
    SwitchImpl(&ThreadContext(), CurrentFiber()->GetContext());
    YT_VERIFY(!CurrentFiber());
}

void SwitchFromFiber(TFiberPtr target)
{
    auto* currentFiber = CurrentFiber().Get();
    YT_VERIFY(currentFiber);

    auto* context = currentFiber->GetContext();
    auto* targetContext = target ? target->GetContext() : &ThreadContext();

    // Here current fiber could be destroyed. But it must be saved in AfterSwitch callback or other place.
    YT_VERIFY(currentFiber->GetRefCount() > 1);

    CurrentFiber() = std::move(target);
    SwitchImpl(context, targetContext);
}

////////////////////////////////////////////////////////////////////////////////

thread_local bool FiberShutdown = false;

#ifdef REUSE_FIBERS
TLockFreeStack<TFiberPtr> IdleFibers;

void DestroyIdleFibers()
{
    std::vector<TFiberPtr> fibers;
    IdleFibers.DequeueAll(&fibers);

    TFiberContext fiberContext;

    FiberContext = &fiberContext;
    FiberShutdown = true;
    auto finally = Finally([] {
        FiberContext = nullptr;
        FiberShutdown = false;
    });

    for (const auto& fiber : fibers) {
        YT_VERIFY(fiber->GetRefCount() == 1);
        SwitchFromThread(std::move(fiber));
    }

    fibers.clear();
}

REGISTER_SHUTDOWN_CALLBACK(0, DestroyIdleFibers)
#endif

static NProfiling::TCounter CreatedFibersCounter = NProfiling::TProfiler{"/action_queue"}.Counter("/created_fibers");

void FiberMain()
{
    {
        CreatedFibersCounter.Increment();
        YT_LOG_DEBUG("Fiber started");
    }

    auto* currentFiber = CurrentFiber().Get();
    TFiberScheduler* threadThis = nullptr;

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
                // TODO: Remove all memory guards before SetAfterSwitch?
                // TODO: Use simple callbacks without memory allocation.
                // Make TFiber::MakeIdle method instead of lambda function

                NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
                // Switch out and add fiber to idle fibers.
                // Save fiber in AfterSwitch because it can be immediately concurrently reused.
                SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([current = MakeStrong(currentFiber)] () mutable {
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
        if (!threadThis || threadThis->IsShutdown() || FiberShutdown)
#else
        if (!threadThis || threadThis->IsShutdown())
#endif
        {
            // Do not reuse fiber in this rear case. Otherwise too many idle fibers are collected.
            YT_VERIFY(!ResumerFiber());

            NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
            SetAfterSwitch(BIND_DONT_CAPTURE_TRACE_CONTEXT([current = MakeStrong(currentFiber)] () mutable {
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
}

void TFiber::DoRunNaked()
{
    YT_VERIFY(!Terminated);
    FiberMain();
    // Terminating fiber.
    Terminated = true;
    // All allocated objects in this frame must be destroyed here.
    SwitchFromFiber(nullptr);
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void YieldFiber(TClosure afterSwitch)
{
    YT_VERIFY(CurrentFiber());

    SetAfterSwitch(std::move(afterSwitch));

    // Try to get resumer.
    auto targetFiber = std::move(ResumerFiber());
    YT_ASSERT(!ResumerFiber());

    // If there is no resumer switch to idle fiber. Or switch to thread main.
#ifdef REUSE_FIBERS
    if (!targetFiber) {
        IdleFibers.Dequeue(&targetFiber);
    }
#endif

    auto waitingFibersCounter = GetWaitingFibersCounter();
    waitingFibersCounter->Increment(1);

    SwitchFromFiber(std::move(targetFiber));
    YT_VERIFY(ResumerFiber());

    waitingFibersCounter->Increment(-1);
}

void ResumeFiber(TFiberPtr fiber)
{
    YT_VERIFY(CurrentFiber());
    ResumerFiber() = CurrentFiber();
    SwitchFromFiber(std::move(fiber));
    YT_VERIFY(!ResumerFiber());
}

bool CheckFreeStackSpace(size_t space)
{
    auto* currentFiber = CurrentFiber().Get();
    return !currentFiber || currentFiber->CheckFreeStackSpace(space);
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

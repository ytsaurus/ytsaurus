#include "fiber_scheduler_thread.h"

#include "profiling_helpers.h"
#include "execution_stack.h"
#include "atomic_flag_spinlock.h"
#include "private.h"

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <util/thread/lfstack.h>

#include <util/system/context.h>

#include <thread>

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
        auto value = Value_.fetch_add(delta, std::memory_order::relaxed) + delta;
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

// Non POD TLS sometimes does not work correctly in dynamic library.
struct TFiberContext
{
    TExceptionSafeContext ThreadContext;
    TClosure AfterSwitch;
    TFiberPtr ResumerFiber;
    TFiberPtr CurrentFiber;
    TFiberSchedulerThread* FiberThread = nullptr;

    TRefCountedGaugePtr WaitingFibersCounter;
};

static thread_local TFiberContext* FiberContext;

////////////////////////////////////////////////////////////////////////////////

TFiberSchedulerThread::TFiberSchedulerThread(
    const TString& threadGroupName,
    const TString& threadName,
    EThreadPriority threadPriority,
    int shutdownPriority)
    : TThread(threadName, threadPriority, shutdownPriority)
    , ThreadGroupName_(threadGroupName)
{ }

void TFiberSchedulerThread::ThreadMain()
{
    // Hold this strongly.
    auto this_ = MakeStrong(this);

    try {
        YT_LOG_DEBUG("Thread started (Name: %v)",
            GetThreadName());

        TFiberContext fiberContext;
        fiberContext.FiberThread = this;
        fiberContext.WaitingFibersCounter = New<TRefCountedGauge>(
            NProfiling::TRegistry{"/action_queue"}.WithTag("thread", ThreadGroupName_).WithHot());

        FiberContext = &fiberContext;
        auto finally = Finally([] {
            FiberContext = nullptr;
        });

        auto fiber = New<TFiber>();

        SwitchFromThread(std::move(fiber));

        YT_LOG_DEBUG("Thread stopped (Name: %v)",
            GetThreadName());
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unhandled exception in executor thread (Name: %v)",
            GetThreadName());
    }
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

class TFiberProfiler
    : public ISensorProducer
{
public:
    TFiberProfiler()
    {
        TProfiler{""}.AddProducer("/fiber_execution_stack", MakeStrong(this));
    }

    void OnStackAllocated(i64 stackSize)
    {
        BytesAllocated_.fetch_add(stackSize, std::memory_order::relaxed);
        BytesAlive_.fetch_add(stackSize, std::memory_order::relaxed);
    }

    void OnStackFreed(i64 stackSize)
    {
        BytesFreed_.fetch_add(stackSize, std::memory_order::relaxed);
        BytesAlive_.fetch_sub(stackSize, std::memory_order::relaxed);
    }

    void OnFiberCreated()
    {
        CreatedFibersCounter_.Increment();
    }

    void CollectSensors(ISensorWriter* writer) override
    {
        writer->AddCounter("/bytes_allocated", BytesAllocated_);
        writer->AddCounter("/bytes_freed", BytesFreed_);
        writer->AddGauge("/bytes_alive", BytesAlive_);
    }

    static TFiberProfiler* Get()
    {
        return LeakyRefCountedSingleton<TFiberProfiler>().Get();
    }

private:
    std::atomic<i64> BytesAllocated_ = 0;
    std::atomic<i64> BytesFreed_ = 0;
    std::atomic<i64> BytesAlive_ = 0;

    NProfiling::TCounter CreatedFibersCounter_ = NProfiling::TProfiler{"/action_queue"}.Counter("/created_fibers");
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
        TFiberProfiler::Get()->OnStackAllocated(Stack_->GetSize());
    }

    ~TFiber()
    {
        YT_VERIFY(Terminated);
        TFiberProfiler::Get()->OnStackFreed(Stack_->GetSize());
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

    void DoRunNaked() override;

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

Y_NO_INLINE TExceptionSafeContext& ThreadContext()
{
    return FiberContext->ThreadContext;
}

Y_NO_INLINE TClosure& AfterSwitch()
{
    return FiberContext->AfterSwitch;
}

Y_NO_INLINE TFiberPtr& ResumerFiber()
{
    return FiberContext->ResumerFiber;
}

static TFiberPtr NullFiberPtr;

Y_NO_INLINE TFiberPtr& CurrentFiber()
{
    return FiberContext ? FiberContext->CurrentFiber : NullFiberPtr;
}

Y_NO_INLINE TFiberSchedulerThread* GetFiberThread()
{
    return FiberContext->FiberThread;
}

Y_NO_INLINE void SetAfterSwitch(TClosure&& closure)
{
    YT_VERIFY(!AfterSwitch());
    AfterSwitch() = std::move(closure);
}

Y_NO_INLINE TRefCountedGaugePtr GetWaitingFibersCounter()
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
        afterSwitch();
    }

    // TODO(lukyan): Allow to set after switch inside itself
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

#ifdef REUSE_FIBERS

class TFiberManager
{
public:
    static TFiberManager* Get()
    {
        return LeakySingleton<TFiberManager>();
    }

    void EnqueueIdleFiber(TFiberPtr fiber)
    {
        IdleFibers_.Enqueue(std::move(fiber));
        if (DestroyingIdleFibers_.load()) {
            DoDestroyIdleFibers();
        }
    }

    TFiberPtr TryDequeueIdleFiber()
    {
        TFiberPtr fiber;
        IdleFibers_.Dequeue(&fiber);
        return fiber;
    }

private:
    const TShutdownCookie ShutdownCookie_ = RegisterShutdownCallback(
        "FiberManager",
        BIND_NO_PROPAGATE(&TFiberManager::DestroyIdleFibers, this),
        /*priority*/ -100);

    TLockFreeStack<TFiberPtr> IdleFibers_;
    std::atomic<bool> DestroyingIdleFibers_ = false;


    TFiberManager() = default;

    void DestroyIdleFibers()
    {
        DestroyingIdleFibers_.store(true);
        DoDestroyIdleFibers();
    }

    void DoDestroyIdleFibers()
    {
        auto destroy_fibers_impl = [&] {
            std::vector<TFiberPtr> fibers;
            IdleFibers_.DequeueAll(&fibers);

            TFiberContext fiberContext;
            FiberContext = &fiberContext;

            auto finally = Finally([] {
                FiberContext = nullptr;
            });

            for (const auto& fiber : fibers) {
                YT_VERIFY(fiber->GetRefCount() == 1);
                SwitchFromThread(std::move(fiber));
            }

            fibers.clear();
        };

    #ifdef _unix_
        // The current thread could be already exiting and MacOS has some issues
        // with registering new thread-local terminators in this case:
        // https://github.com/lionheart/openradar-mirror/issues/20926
        // As a matter of workaround, we offload all finalization logic to a separate
        // temporary thread.
        std::thread thread([&] {
            ::TThread::SetCurrentThreadName("IdleFiberDtor");

            destroy_fibers_impl();
        });
        thread.join();
    #else
        // Starting threads in exit handlers on Windows causes immediate calling exit
        // so the routine will not be executed. Moreover, if we try to join this thread we'll get deadlock
        // because this thread will try to acquire atexit lock which is owned by this thread.
        destroy_fibers_impl();
    #endif
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

#endif

void FiberMain()
{
    {
        TFiberProfiler::Get()->OnFiberCreated();
        YT_LOG_DEBUG("Fiber started");
    }

    auto* currentFiber = CurrentFiber().Get();
    TFiberSchedulerThread* fiberThread = nullptr;

    // Break loop to terminate fiber
    while (fiberThread = GetFiberThread()) {
        YT_VERIFY(!ResumerFiber());

        // We wrap fiberThread->OnExecute() into a propagating storage guard to ensure
        // that the propagating storage created there won't spill into the fiber callbacks.
        YT_VERIFY(GetCurrentPropagatingStorage().IsNull());
        TCallback<void()> callback;
        {
            TNullPropagatingStorageGuard guard;
            callback = fiberThread->OnExecute();
        }

        if (callback) {
             try {
                RunInFiberContext(std::move(callback));
            } catch (const TFiberCanceledException&) { }
        } else {
            break;
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
                SetAfterSwitch(BIND_NO_PROPAGATE([current = MakeStrong(currentFiber)] () mutable {
                    TFiberManager::Get()->EnqueueIdleFiber(std::move(current));
                }));
            }

            // Switched to ResumerFiber or thread main.
            SwitchFromFiber(std::move(ResumerFiber()));
#else
            {
                NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
                SetAfterSwitch(BIND_NO_PROPAGATE([
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
    }

    {
        YT_LOG_DEBUG("Fiber finished");

        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        SetAfterSwitch(BIND_NO_PROPAGATE([current = MakeStrong(currentFiber)] () mutable {
            current.Reset();
        }));

    }
}

void TFiber::DoRunNaked()
{
    // Allows set new AfterSwitch inside it.
    if (auto afterSwitch = std::move(AfterSwitch())) {
        YT_VERIFY(!AfterSwitch());
        afterSwitch();
    }

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
        targetFiber = TFiberManager::Get()->TryDequeueIdleFiber();
    }
#endif

    if (!targetFiber) {
        targetFiber = New<TFiber>();
    }

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

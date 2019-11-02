#include "fiber.h"
#include "private.h"
#include "action_queue.h"
#include "atomic_flag_spinlock.h"
#include "fls.h"
#include "scheduler.h"
#include "thread_affinity.h"

#include <yt/core/ytalloc/memory_tag.h>

#include <yt/core/profiling/timing.h>

#include <util/generic/singleton.h>

namespace NYT::NConcurrency {

using namespace NTracing;
using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;

static class TFiberIdGenerator
{
public:
    TFiberIdGenerator()
    {
        Seed_.store(static_cast<TFiberId>(::time(nullptr)));
    }

    TFiberId Generate()
    {
        constexpr TFiberId Factor = std::numeric_limits<TFiberId>::max() - 173864;
        static_assert(Factor % 2 == 1); // Factor must be coprime with 2^n.

        while (true) {
            auto seed = Seed_++;
            auto id = seed * Factor;
            if (id != InvalidFiberId) {
                return id;
            }
        }
    }

private:
    std::atomic<TFiberId> Seed_;

} FiberIdGenerator;

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
    // TODO(sandello): Make it an intrusive list.
    std::atomic_flag Lock_ = ATOMIC_FLAG_INIT;
    std::list<TFiber*> Fibers_;

};

// Cache registry in static variable to simplify introspection.
static TFiberRegistry* FiberRegistry;

TFiberRegistry* GetFiberRegistry()
{
    if (!FiberRegistry) {
        FiberRegistry = Singleton<TFiberRegistry>();
    }
    return FiberRegistry;
}

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber(TClosure callee, EExecutionStackKind stackKind)
    : Callee_(std::move(callee))
    , Stack_(CreateExecutionStack(stackKind))
    , Context_({
        this,
        TArrayRef(static_cast<char*>(Stack_->GetStack()), Stack_->GetSize())})
{
    RegenerateId();
    Iterator_ = GetFiberRegistry()->Register(this);

    PushContextHandler(
        [this] () noexcept {
             NYTAlloc::SetCurrentMemoryTag(MemoryTag_);
             NYTAlloc::SetCurrentMemoryZone(MemoryZone_);
        },
        [this] () noexcept {
            MemoryTag_ = NYTAlloc::GetCurrentMemoryTag();
            MemoryZone_ = NYTAlloc::GetCurrentMemoryZone();
        });
}

TFiber::~TFiber()
{
    YT_VERIFY(IsTerminated());
    GetFiberRegistry()->Unregister(Iterator_);
}

TFiberId TFiber::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

TFiberId TFiber::RegenerateId()
{
    Id_ = FiberIdGenerator.Generate();
    return Id_;
}

EFiberState TFiber::GetState() const
{
    // THREAD_AFFINITY(OwnerThread);
    // NB: These annotations are fake since owner may change.

    return State_;
}

void TFiber::SetRunning()
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    YT_ASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Running;
    Awaitable_.Reset();
    RunStartInstant_ = NProfiling::GetCpuInstant();
    InstallTraceContext(RunStartInstant_, std::move(SavedTraceContext_));

    NDetail::SetCurrentFsdHolder(&FsdHolder_);
}

void TFiber::SetSleeping(TAwaitable awaitable)
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    UnwindIfCanceled();
    FinishRunning();
    YT_ASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Sleeping;
    YT_ASSERT(!Awaitable_);
    Awaitable_ = std::move(awaitable);
}

void TFiber::SetSuspended()
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    FinishRunning();
    YT_ASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Suspended;
    Awaitable_.Reset();
}

TExceptionSafeContext* TFiber::GetContext()
{
    return &Context_;
}

void TFiber::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();

    bool expected = false;
    if (!Canceled_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
        return;
    }

    TAwaitable awaitable;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        awaitable = std::move(Awaitable_);
    }

    if (awaitable) {
        YT_LOG_DEBUG("Sending cancelation to fiber, propagating to the awaited future (TargetFiberId: %llx)",
            Id_);
        awaitable.Cancel();
    } else {
        YT_LOG_DEBUG("Sending cancelation to fiber (TargetFiberId: %llx)",
            Id_);
    }
}

const TClosure& TFiber::GetCanceler()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    if (!Canceler_) {
        TMemoryTagGuard guard(NullMemoryTag);
        Canceler_ = BIND_DONT_CAPTURE_TRACE_CONTEXT(&TFiber::Cancel, MakeWeak(this));
    }

    return Canceler_;
}

bool TFiber::IsCancelable() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    return static_cast<bool>(Canceler_);
}

bool TFiber::IsCanceled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Canceled_.load(std::memory_order_relaxed);
}

void TFiber::UnwindIfCanceled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (IsCanceled()) {
        YT_LOG_DEBUG("Throwing fiber cancelation exception");
        throw TFiberCanceledException();
    }
}

bool TFiber::IsTerminated() const
{
    // THREAD_AFFINITY(OwnerThread);

    return State_ == EFiberState::Terminated;
}

void TFiber::DoRunNaked()
{
    try {
        Callee_.Run();
    } catch (const TFiberCanceledException&) {
        // Thrown intentionally, ignore.
        YT_LOG_DEBUG("Fiber canceled");
    }
    // NB: All other uncaught exceptions will lead to std::terminate().
    // This way we preserve the much-needed backtrace.

    FinishRunning();
    State_ = EFiberState::Terminated;

    ReturnFromFiber();

    YT_ABORT();
}

void TFiber::PushContextHandler(std::function<void()> out, std::function<void()> in)
{
    SwitchHandlers_.push_back({std::move(out), std::move(in)});
}

void TFiber::PopContextHandler()
{
    YT_ASSERT(!SwitchHandlers_.empty());
    SwitchHandlers_.pop_back();
}

void TFiber::InvokeContextOutHandlers()
{
    for (auto it = SwitchHandlers_.rbegin(); it != SwitchHandlers_.rend(); ++it) {
        if (it->Out) {
            it->Out();
        }
    }
}

void TFiber::InvokeContextInHandlers()
{
    for (auto it = SwitchHandlers_.rbegin(); it != SwitchHandlers_.rend(); ++it) {
        if (it->In) {
            it->In();
        }
    }
}

bool TFiber::CheckFreeStackSpace(size_t space) const
{
    return reinterpret_cast<char*>(Stack_->GetStack()) + space < __builtin_frame_address(0);
}

NProfiling::TCpuDuration TFiber::GetRunCpuTime() const
{
    // THREAD_AFFINITY(OwnerThread);
    YT_ASSERT(State_ == EFiberState::Running);

    return RunCpuTime_ + std::max<NProfiling::TCpuDuration>(0, NProfiling::GetCpuInstant() - RunStartInstant_);
}

void TFiber::FinishRunning()
{
    if (State_ != EFiberState::Running) {
        return;
    }
    auto now = NProfiling::GetCpuInstant();
    SavedTraceContext_ = UninstallTraceContext(now);
    RunCpuTime_ += std::max<NProfiling::TCpuDuration>(0, now - RunStartInstant_);

    NDetail::SetCurrentFsdHolder(nullptr);
}

////////////////////////////////////////////////////////////////////////////////

TClosure GetCurrentFiberCanceler()
{
    auto* fiber = TryGetCurrentFiber();
    return fiber ? fiber->GetCanceler() : TClosure();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency


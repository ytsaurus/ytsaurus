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

#ifdef DEBUG

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

#endif

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber(TClosure callee, EExecutionStackKind stackKind)
    : Callee_(std::move(callee))
    , Stack_(CreateExecutionStack(stackKind))
    , Context_(Stack_.get(), this)
{
    RegenerateId();
#ifdef DEBUG
    Iterator_ = GetFiberRegistry()->Register(this);
#endif
}

TFiber::~TFiber()
{
    YT_VERIFY(IsTerminated());
    for (int index = 0; index < Fsd_.size(); ++index) {
        const auto& slot = Fsd_[index];
        if (slot) {
            NDetail::FlsDestruct(index, slot);
        }
    }
#ifdef DEBUG
    GetFiberRegistry()->Unregister(Iterator_);
#endif
}

TFiberId TFiber::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

TFiberId TFiber::RegenerateId()
{
    Id_ = GenerateFiberId();
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
    AwaitedFuture_.Reset();
    RunStartInstant_ = NProfiling::GetCpuInstant();
    InstallTraceContext(RunStartInstant_, std::move(SavedTraceContext_));
}

void TFiber::SetSleeping(TFuture<void> awaitedFuture)
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    UnwindIfCanceled();
    FinishRunning();
    YT_ASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Sleeping;
    YT_ASSERT(!AwaitedFuture_);
    AwaitedFuture_ = std::move(awaitedFuture);
}

void TFiber::SetSuspended()
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    FinishRunning();
    YT_ASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Suspended;
    AwaitedFuture_.Reset();
}

TExecutionContext* TFiber::GetContext()
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

    TFuture<void> awaitedFuture;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        awaitedFuture = std::move(AwaitedFuture_);
    }

    if (awaitedFuture) {
        YT_LOG_DEBUG("Sending cancelation to fiber, propagating to the awaited future (TargetFiberId: %llx)",
            Id_);
        awaitedFuture.Cancel();
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

uintptr_t& TFiber::FsdAt(int index)
{
    // THREAD_AFFINITY(OwnerThread);
    if (Y_UNLIKELY(index >= Fsd_.size())) {
        FsdResize();
    }
    return Fsd_[index];
}

void TFiber::FsdResize()
{
    int oldSize = static_cast<int>(Fsd_.size());
    int newSize = NDetail::FlsCountSlots();

    YT_ASSERT(newSize > oldSize);

    Fsd_.resize(newSize);

    for (int index = oldSize; index < newSize; ++index) {
        Fsd_[index] = 0;
    }
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

    GetCurrentScheduler()->Return();

    YT_ABORT();
}

void TFiber::PushContextHandler(std::function<void()> out, std::function<void()> in)
{
    SwitchHandlers_.push_front({std::move(out), std::move(in)});
}

void TFiber::PopContextHandler()
{
    YT_VERIFY(!SwitchHandlers_.empty());
    SwitchHandlers_.pop_front();
}

void TFiber::InvokeContextOutHandlers()
{
    for (auto& handler : SwitchHandlers_) {
        handler.Out();
    }
}

void TFiber::InvokeContextInHandlers()
{
    for (auto& handler : SwitchHandlers_) {
        handler.In();
    }
}

TMemoryTag TFiber::GetMemoryTag() const
{
    return MemoryTag_;
}

void TFiber::SetMemoryTag(TMemoryTag tag)
{
    MemoryTag_ = tag;
}

EMemoryZone TFiber::GetMemoryZone() const
{
    return MemoryZone_;
}

void TFiber::SetMemoryZone(EMemoryZone zone)
{
    MemoryZone_ = zone;
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
}

////////////////////////////////////////////////////////////////////////////////

static std::atomic<int> SmallFiberStackPoolSize = {1024};
static std::atomic<int> LargeFiberStackPoolSize = {1024};

int GetFiberStackPoolSize(EExecutionStackKind stackKind)
{
    switch (stackKind) {
        case EExecutionStackKind::Small: return SmallFiberStackPoolSize.load(std::memory_order_relaxed);
        case EExecutionStackKind::Large: return LargeFiberStackPoolSize.load(std::memory_order_relaxed);
        default:                         YT_ABORT();
    }
}

void SetFiberStackPoolSize(EExecutionStackKind stackKind, int poolSize)
{
    if (poolSize < 0) {
        THROW_ERROR_EXCEPTION("Invalid fiber stack pool size %v is given for %Qlv stacks",
            poolSize,
            stackKind);
    }
    switch (stackKind) {
        case EExecutionStackKind::Small: SmallFiberStackPoolSize = poolSize; break;
        case EExecutionStackKind::Large: LargeFiberStackPoolSize = poolSize; break;
        default:                         YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TClosure GetCurrentFiberCanceler()
{
    auto* scheduler = TryGetCurrentScheduler();
    return scheduler ? scheduler->GetCurrentFiber()->GetCanceler() : TClosure();
}

namespace NDetail {

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency


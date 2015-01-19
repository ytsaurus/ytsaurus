#include "stdafx.h"
#include "action_queue.h"
#include "fiber.h"
#include "scheduler.h"
#include "fls.h"
#include "thread_affinity.h"
#include "private.h"

#include <core/misc/lazy_ptr.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ConcurrencyLogger;
static std::atomic<TFiberId> FiberIdGenerator(InvalidFiberId + 1);

#ifdef DEBUG
// TODO(sandello): Make it an intrusive list.
static std::atomic_flag FiberRegistryLock = ATOMIC_FLAG_INIT;
static std::list<TFiber*> FiberRegistry;
#endif

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber(TClosure callee, EExecutionStack stack)
    : Id_(FiberIdGenerator++)
    , State_(EFiberState::Suspended)
    , Callee_(std::move(callee))
    , Stack_(CreateExecutionStack(stack))
    , Context_(CreateExecutionContext(Stack_.get(), &TFiber::Trampoline))
    , Canceled_(false)
{
#ifdef DEBUG
    while (FiberRegistryLock.test_and_set(std::memory_order_acquire));
    Iterator_ = FiberRegistry.insert(FiberRegistry.begin(), this);
    FiberRegistryLock.clear(std::memory_order_release);
#endif
}

TFiber::~TFiber()
{
    YCHECK(IsTerminated());
    for (int index = 0; index < Fsd_.size(); ++index) {
        const auto& slot = Fsd_[index];
        if (slot) {
            NDetail::FlsDestruct(index, slot);
        }
    }
#ifdef DEBUG
    while (FiberRegistryLock.test_and_set(std::memory_order_acquire));
    FiberRegistry.erase(Iterator_);
    FiberRegistryLock.clear(std::memory_order_release);
#endif
}

TFiberId TFiber::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    YASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Running;
    AwaitedFuture_.Reset();
}

void TFiber::SetSleeping(TFuture<void> awaitedFuture)
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    YASSERT(State_ != EFiberState::Terminated);
    State_ = EFiberState::Sleeping;
    YASSERT(!AwaitedFuture_);
    AwaitedFuture_ = std::move(awaitedFuture);
}

void TFiber::SetSuspended()
{
    // THREAD_AFFINITY(OwnerThread);

    TGuard<TSpinLock> guard(SpinLock_);
    YASSERT(State_ != EFiberState::Terminated);
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
    if (!Canceled_.compare_exchange_strong(expected, true, std::memory_order_relaxed))
        return;

    TFuture<void> awaitedFuture;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        awaitedFuture = std::move(AwaitedFuture_);
    }

    if (awaitedFuture) {
        awaitedFuture.Cancel();
        LOG_DEBUG("Sending cancelation to fiber %x, propagating to the awaited future",
            Id_);
    } else {
        LOG_DEBUG("Sending cancelation to fiber %x",
            Id_);
    }
}

const TClosure& TFiber::GetCanceler()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    if (!Canceler_) {
        Canceler_ = BIND(&TFiber::Cancel, MakeWeak(this));
    }

    return Canceler_;
}

bool TFiber::IsCancelable() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return static_cast<bool>(Canceler_);
}

bool TFiber::IsCanceled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Canceled_.load(std::memory_order_relaxed);
}

bool TFiber::IsTerminated() const
{
    // THREAD_AFFINITY(OwnerThread);

    return State_ == EFiberState::Terminated;
}

uintptr_t& TFiber::FsdAt(int index)
{
    // THREAD_AFFINITY(OwnerThread);
    if (UNLIKELY(index >= Fsd_.size())) {
        FsdResize();
    }
    return Fsd_[index];
}

void TFiber::FsdResize()
{
    int oldSize = static_cast<int>(Fsd_.size());
    int newSize = NDetail::FlsCountSlots();

    YASSERT(newSize > oldSize);

    Fsd_.resize(newSize);

    for (int index = oldSize; index < newSize; ++index) {
        Fsd_[index] = 0;
    }
}

void TFiber::Trampoline(void* opaque)
{
    auto* fiber = reinterpret_cast<TFiber*>(opaque);
    YASSERT(fiber);

    try {
        fiber->Callee_.Run();
    } catch (const TFiberCanceledException&) {
        // Thrown intentionally, ignore.
    }
    // NB: All other uncaught exceptions will lead to std::terminate().
    // This way we preserve the much-needed backtrace.

    fiber->State_ = EFiberState::Terminated;

    GetCurrentScheduler()->Return();

    YUNREACHABLE();
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

} // namespace NConcurrency
} // namespace NYT


#include "stdafx.h"
#include "action_queue.h"
#include "fiber.h"
#include "scheduler.h"

#include <core/misc/lazy_ptr.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Fiber-local storage support.
/*
 * Each registered slot maintains a ctor (to be called when a fiber accesses
 * the slot for the first time) and a dtor (to be called when a fiber terminates).
 *
 * To avoid contention, slot registry has a fixed size (see |MaxFlsSlots|).
 * |FlsGet| and |FlsGet| are lock-free.
 *
 * |FlsAllocateSlot|, however, acquires a global lock.
 */
struct TFlsSlot
{
    TFiber::TFlsSlotCtor Ctor;
    TFiber::TFlsSlotDtor Dtor;
};

static const int FlsMaxSize = 1024;
static int FlsSize = 0;

static std::atomic_flag FlsLock = ATOMIC_FLAG_INIT;
static TFlsSlot FlsSlots[FlsMaxSize] = {};

TFiber::TFiber(TClosure callee, EExecutionStack stack)
    : State_(EFiberState::Suspended)
    , Stack_(CreateExecutionStack(stack))
    , Context_(CreateExecutionContext(Stack_.get(), &TFiber::Trampoline))
    , Exception_(nullptr)
    , Callee_(std::move(callee))
{
    // XXX(babenko): VS2013 compat
    Canceled_ = false;
}

TFiber::~TFiber()
{
    YCHECK(CanReturn());
}

EFiberState TFiber::GetState() const
{
    return State_;
}

void TFiber::SetState(EFiberState state)
{
    State_ = state;
}

TExecutionContext* TFiber::GetContext()
{
    return &Context_;
}

std::exception_ptr TFiber::GetException()
{
    std::exception_ptr result;
    std::swap(result, Exception_);
    return result;
}

void TFiber::Cancel()
{
    Canceled_.store(true, std::memory_order_relaxed);
}

bool TFiber::IsCanceled() const
{
    return Canceled_.load(std::memory_order_relaxed);
}

bool TFiber::CanReturn() const
{
    return
        State_ == EFiberState::Terminated ||
        State_ == EFiberState::Canceled ||
        State_ == EFiberState::Crashed;
}

int TFiber::FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor)
{
    while (FlsLock.test_and_set(std::memory_order_acquire)) {
        /* Spin. */
    }

    int index = FlsSize++;
    YCHECK(index < FlsMaxSize);

    auto& slot = FlsSlots[index];
    slot.Ctor = std::move(ctor);
    slot.Dtor = std::move(dtor);

    FlsLock.clear(std::memory_order_release);

    return index;
}

TFiber::TFlsSlotValue TFiber::FlsGet(int index)
{
    FlsEnsure(index);
    return Fls_[index];
}

void TFiber::FlsSet(int index, TFlsSlotValue value)
{
    FlsEnsure(index);
    Fls_[index] = std::move(value);
}

void TFiber::FlsEnsure(int index)
{
    if (LIKELY(index < Fls_.size())) {
        return;
    }

    YASSERT(index >= 0 && index < FlsMaxSize);

    int oldSize = static_cast<int>(Fls_.size());
    int newSize = FlsSize;

    YASSERT(newSize > oldSize && newSize > index);

    Fls_.resize(newSize);

    for (int index = oldSize; index < newSize; ++index) {
        Fls_[index] = FlsSlots[index].Ctor();
    }
}

void TFiber::Trampoline(void* opaque)
{
    auto* fiber = reinterpret_cast<TFiber*>(opaque);
    YASSERT(fiber);

    try {
        fiber->Callee_.Run();
        fiber->State_ = EFiberState::Terminated;
    } catch (const TFiberCanceledException&) {
        // Thrown intentionally, ignore.
        fiber->State_ = EFiberState::Canceled;
    } catch (...) {
        fiber->Exception_ = std::current_exception();
        fiber->State_ = EFiberState::Crashed;
    }

    GetCurrentScheduler()->Return();

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

static TLazyIntrusivePtr<TActionQueue> UnwindThread(
    TActionQueue::CreateFactory("Unwind", false, false));

TClosure GetCurrentFiberCanceler()
{
    return BIND(
        &TFiber::Cancel,
        MakeStrong(GetCurrentScheduler()->GetCurrentFiber()));
}

void ResumeFiber(TFiberPtr fiber)
{
    YCHECK(fiber->GetState() == EFiberState::Sleeping);
    fiber->SetState(EFiberState::Suspended);

    GetCurrentScheduler()->YieldTo(std::move(fiber));
}

void UnwindFiber(TFiberPtr fiber)
{
    if (!UnwindThread.HasValue()) {
        UnwindThread->Detach();
    }

    fiber->Cancel();

    BIND(&ResumeFiber, Passed(std::move(fiber)))
        .Via(UnwindThread->GetInvoker())
        .Run();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


#include "stdafx.h"
#include "action_queue.h"
#include "fiber.h"
#include "scheduler.h"
#include "fls.h"

#include <core/misc/lazy_ptr.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static std::atomic<TFiberId> FiberIdGenerator(InvalidFiberId + 1);

TFiber::TFiber(TClosure callee, EExecutionStack stack)
    : Id_(FiberIdGenerator++)
    , State_(EFiberState::Suspended)
    , Stack_(CreateExecutionStack(stack))
    , Context_(CreateExecutionContext(Stack_.get(), &TFiber::Trampoline))
    , Callee_(std::move(callee))
{
    // XXX(babenko): VS2013 compat
    Canceled_ = false;
}

TFiber::~TFiber()
{
    YCHECK(CanReturn());

    for (int index = 0; index < Fsd_.size(); ++index) {
        const auto& slot = Fsd_[index];
        if (slot) {
            NDetail::FlsDestruct(index, slot);
        }
    }
}

TFiberId TFiber::GetId() const
{
    return Id_;
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
        State_ == EFiberState::Canceled;
}

uintptr_t& TFiber::FsdAt(int index)
{
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
        fiber->State_ = EFiberState::Terminated;
    } catch (const TFiberCanceledException&) {
        // Thrown intentionally, ignore.
        fiber->State_ = EFiberState::Canceled;
    }

    // NB: All other uncaught exceptions will lead to std::terminate().
    // This way we preserve the much-needed backtrace.

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
    fiber->Cancel();

    BIND(&ResumeFiber, Passed(std::move(fiber)))
        .Via(UnwindThread->GetInvoker())
        .Run();
}

void ShutdownUnwindThread()
{
    if (UnwindThread.HasValue()) {
        UnwindThread->Shutdown();
        UnwindThread.Reset();
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


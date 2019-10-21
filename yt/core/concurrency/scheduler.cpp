#include "scheduler.h"
#include "fiber.h"
#include "fls.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

thread_local IInvokerPtr CurrentInvoker;
thread_local IScheduler* CurrentScheduler;
thread_local TFiberId CurrentFiberId;
thread_local TFiber* CurrentFiber;

////////////////////////////////////////////////////////////////////////////////

NProfiling::TCpuDuration GetCurrentRunCpuTime()
{
    YT_ASSERT(CurrentFiber);
    return CurrentFiber->GetRunCpuTime();
}

////////////////////////////////////////////////////////////////////////////////

bool CheckFreeStackSpace(size_t space)
{
    auto* fiber = TryGetCurrentFiber();
    return !fiber || fiber->CheckFreeStackSpace(space);
}

////////////////////////////////////////////////////////////////////////////////

void Yield()
{
    Y_UNUSED(WaitFor(VoidFuture));
}

void SwitchTo(IInvokerPtr invoker)
{
    YT_ASSERT(invoker);
    YT_ASSERT(CurrentScheduler);
    CurrentScheduler->SwitchTo(std::move(invoker));
}

void ReturnFromFiber()
{
    YT_ASSERT(CurrentScheduler);
    CurrentScheduler->Return();
}

void YieldToFiber(TFiberPtr&& other)
{
    YT_ASSERT(CurrentScheduler);
    CurrentScheduler->YieldTo(std::move(other));
}

namespace NDetail {

void WaitForImpl(TAwaitable awaitable, IInvokerPtr invoker)
{
    YT_ASSERT(awaitable);
    YT_ASSERT(invoker);
    auto* scheduler = CurrentScheduler;
    if (scheduler) {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        scheduler->WaitFor(std::move(awaitable), std::move(invoker));
    } else {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TContextSwitchGuard::TContextSwitchGuard(std::function<void()> out, std::function<void()> in)
{
    if (auto* fiber = TryGetCurrentFiber()) {
        fiber->PushContextHandler(std::move(out), std::move(in));
    }
}

TContextSwitchGuard::~TContextSwitchGuard()
{
    if (auto* fiber = TryGetCurrentFiber()) {
        fiber->PopContextHandler();
    }
}

////////////////////////////////////////////////////////////////////////////////

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvokerPtr invoker)
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept { Restore(); },
        nullptr)
    , Active_(true)
    , SavedInvoker_(std::move(invoker))
{
    CurrentInvoker.Swap(SavedInvoker_);
}

void TCurrentInvokerGuard::Restore()
{
    if (!Active_) {
        return;
    }
    Active_ = false;
    CurrentInvoker.Swap(SavedInvoker_);
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    Restore();
}

////////////////////////////////////////////////////////////////////////////////

TOneShotContextSwitchGuard::TOneShotContextSwitchGuard(std::function<void()> handler)
    : TContextSwitchGuard(
        [this, handler = std::move(handler)] () noexcept {
            if (!Active_) {
                return;
            }
            Active_ = false;
            handler();
        },
        nullptr)
    , Active_(true)
{ }

////////////////////////////////////////////////////////////////////////////////

TForbidContextSwitchGuard::TForbidContextSwitchGuard()
    : TOneShotContextSwitchGuard([] { YT_ABORT(); })
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

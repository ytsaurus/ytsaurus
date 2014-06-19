#include "stdafx.h"
#include "scheduler.h"
#include "scheduler.h"
#include "fls.h"
#include "fiber.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TLS_STATIC IScheduler* CurrentScheduler = nullptr;

IScheduler* GetCurrentScheduler()
{
    YCHECK(CurrentScheduler);
    return CurrentScheduler;
}

IScheduler* TryGetCurrentScheduler()
{
    return CurrentScheduler;
}

TCurrentSchedulerGuard::TCurrentSchedulerGuard(IScheduler* scheduler)
    : SavedScheduler_(CurrentScheduler)
{
    CurrentScheduler = scheduler;
}

TCurrentSchedulerGuard::~TCurrentSchedulerGuard()
{
    CurrentScheduler = SavedScheduler_;
}

////////////////////////////////////////////////////////////////////////////////

TFiberId GetCurrentFiberId()
{
    auto* scheduler = TryGetCurrentScheduler();
    if (!scheduler) {
        return InvalidFiberId;
    }
    auto* fiber = scheduler->GetCurrentFiber();
    if (!fiber) {
        return InvalidFiberId;
    }
    return fiber->GetId();
}

void Yield()
{
    GetCurrentScheduler()->Yield();
}

void SwitchTo(IInvokerPtr invoker)
{
    YASSERT(invoker);
    GetCurrentScheduler()->SwitchTo(std::move(invoker));
}

void WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    YASSERT(future);
    YASSERT(invoker);

    if (future.IsSet()) {
        return;
    }

    if (future.IsCanceled()) {
        throw TFiberCanceledException();
    }

    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        scheduler->WaitFor(std::move(future), std::move(invoker));
    } else {
        // If we call WaitFor from fiber-unfriendly thread, we fallback to blocking wait.
        future.Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


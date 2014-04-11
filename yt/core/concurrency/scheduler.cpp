#include "stdafx.h"
#include "fiber.h"
#include "scheduler.h"
#include "fls.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

IScheduler::~IScheduler()
{ }

////////////////////////////////////////////////////////////////////////////////

TLS_STATIC IScheduler* CurrentScheduler = nullptr;

IScheduler* GetCurrentScheduler()
{
    YCHECK(CurrentScheduler);
    return CurrentScheduler;
}

IScheduler* GetCurrentSchedulerUnsafe()
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

    GetCurrentScheduler()->WaitFor(std::move(future), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


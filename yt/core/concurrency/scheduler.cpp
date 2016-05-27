#include "scheduler.h"
#include "fiber.h"
#include "fls.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

PER_THREAD IScheduler* CurrentScheduler = nullptr;

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
    Y_ASSERT(invoker);
    GetCurrentScheduler()->SwitchTo(std::move(invoker));
}

void SubscribeContextSwitched(TClosure callback)
{
    GetCurrentScheduler()->SubscribeContextSwitched(std::move(callback));
}

void UnsubscribeContextSwitched(TClosure callback)
{
    GetCurrentScheduler()->UnsubscribeContextSwitched(std::move(callback));
}

////////////////////////////////////////////////////////////////////////////////

TContextSwitchedGuard::TContextSwitchedGuard(TClosure callback)
    : Callback_(std::move(callback))
{
    if (Callback_) {
        SubscribeContextSwitched(Callback_);
    }
}

TContextSwitchedGuard::~TContextSwitchedGuard()
{
    if (Callback_) {
        UnsubscribeContextSwitched(Callback_);
    }
}

////////////////////////////////////////////////////////////////////////////////

void UninterruptableWaitFor(TFuture<void> future)
{
    UninterruptableWaitFor(std::move(future), GetCurrentInvoker());
}

void UninterruptableWaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    Y_ASSERT(future);
    Y_ASSERT(invoker);

    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        scheduler->UninterruptableWaitFor(future, std::move(invoker));
    } else {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YCHECK(invoker == GetCurrentInvoker());
        YCHECK(invoker == GetSyncInvoker());
        future.Get();
    }

    Y_ASSERT(future.IsSet());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


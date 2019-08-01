#pragma once
#ifndef SCHEDULER_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduler.h"
// For the sake of sane code completion.
#include "scheduler.h"
#endif
#undef SCHEDULER_INL_H_

#include <yt/core/actions/invoker_util.h>

#include <yt/core/ytalloc/memory_tag.h>

#include <library/ytalloc/api/ytalloc.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future)
{
    return WaitFor(std::move(future), GetCurrentInvoker());
}

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker)
{
    YT_ASSERT(future);
    YT_ASSERT(invoker);

    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        NYTAlloc::TMemoryTagGuard guard(NYTAlloc::NullMemoryTag);
        scheduler->WaitFor(future.template As<void>(), std::move(invoker));
        YT_ASSERT(future.IsSet());
    } else {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YT_VERIFY(invoker == GetCurrentInvoker());
        YT_VERIFY(invoker == GetSyncInvoker());
    }

    return future.Get();
}

////////////////////////////////////////////////////////////////////////////////

extern Y_POD_THREAD(IScheduler*) CurrentScheduler;

Y_FORCE_INLINE IScheduler* GetCurrentScheduler()
{
    YT_ASSERT(CurrentScheduler);
    return CurrentScheduler;
}

Y_FORCE_INLINE IScheduler* TryGetCurrentScheduler()
{
    return CurrentScheduler;
}

Y_FORCE_INLINE void SetCurrentScheduler(IScheduler* scheduler)
{
    YT_VERIFY(!CurrentScheduler);
    CurrentScheduler = scheduler;
}

////////////////////////////////////////////////////////////////////////////////

extern Y_POD_THREAD(TFiberId) CurrentFiberId;

Y_FORCE_INLINE TFiberId GetCurrentFiberId()
{
    return CurrentFiberId;
}

Y_FORCE_INLINE void SetCurrentFiberId(TFiberId id)
{
    CurrentFiberId = id;
    NYTAlloc::SetCurrentFiberId(id);
}

////////////////////////////////////////////////////////////////////////////////

extern Y_POD_THREAD(const TFiber*) CurrentFiber;

Y_FORCE_INLINE const TFiber* GetCurrentFiber()
{
    YT_ASSERT(CurrentFiber);
    return CurrentFiber;
}

Y_FORCE_INLINE void SetCurrentFiber(const TFiber* fiber)
{
    CurrentFiber = fiber;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

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

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration.
IInvokerPtr GetSyncInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

extern thread_local IInvokerPtr CurrentInvoker;

Y_FORCE_INLINE IInvokerPtr GetCurrentInvoker()
{
    return CurrentInvoker ? CurrentInvoker : GetSyncInvoker();
}

Y_FORCE_INLINE void SetCurrentInvoker(IInvokerPtr invoker)
{
    CurrentInvoker = std::move(invoker);
}

////////////////////////////////////////////////////////////////////////////////

extern thread_local TFiberId CurrentFiberId;

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

extern thread_local TFiber* CurrentFiber;

Y_FORCE_INLINE TFiber* TryGetCurrentFiber()
{
    return CurrentFiber;
}

Y_FORCE_INLINE void SetCurrentFiber(TFiber* fiber)
{
    CurrentFiber = fiber;
}

////////////////////////////////////////////////////////////////////////////////

extern thread_local IScheduler* CurrentScheduler;

Y_FORCE_INLINE void SetCurrentScheduler(IScheduler* scheduler)
{
    YT_VERIFY(!CurrentScheduler);
    CurrentScheduler = scheduler;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void WaitForImpl(TAwaitable awaitable, IInvokerPtr invoker);

} // namespace NDetail

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker)
{
    NDetail::WaitForImpl(future.AsAwaitable(), std::move(invoker));
    return future.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

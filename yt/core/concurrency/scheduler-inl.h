#pragma once
#ifndef SCHEDULER_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduler.h"
// For the sake of sane code completion.
#include "scheduler.h"
#endif
#undef SCHEDULER_INL_H_

#include <yt/core/actions/invoker_util.h>
#include <yt/core/misc/memory_tag.h>

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
    Y_ASSERT(future);
    Y_ASSERT(invoker);

    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        TMemoryTagGuard guard(NullMemoryTag);
        scheduler->WaitFor(future.template As<void>(), std::move(invoker));
        Y_ASSERT(future.IsSet());
    } else {
        // When called from a fiber-unfriendly context, we fallback to blocking wait.
        YCHECK(invoker == GetCurrentInvoker());
        YCHECK(invoker == GetSyncInvoker());
    }

    return future.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

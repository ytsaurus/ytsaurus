#ifndef SCHEDULER_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduler.h"
#endif
#undef SCHEDULER_INL_H_

#include <core/actions/invoker_util.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future)
{
    return WaitFor(std::move(future), GetCurrentInvoker());
}

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker)
{
    YASSERT(future);
    YASSERT(invoker);

    if (future.IsCanceled()) {
        throw TFiberCanceledException();
    }

    auto* scheduler = TryGetCurrentScheduler();
    if (scheduler) {
        scheduler->WaitFor(future.template As<void>(), std::move(invoker));
        YASSERT(future.IsSet());
    } else {
        // If called from a fiber-unfriendly thread, we fallback to blocking wait.
        YCHECK(invoker == GetCurrentInvoker());
        YCHECK(invoker == GetSyncInvoker());
    }

    return future.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

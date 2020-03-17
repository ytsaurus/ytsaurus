#pragma once
#ifndef SCHEDULER_API_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduler_api.h"
// For the sake of sane code completion.
#include "scheduler_api.h"
#endif
#undef THREAD_AFFINITY_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
[[nodiscard]] TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker)
{
    YT_ASSERT(future);
    YT_ASSERT(invoker);

    WaitUntilSet(future.AsVoid(), std::move(invoker));

    return future.Get();
}

inline void Yield()
{
    WaitUntilSet(VoidFuture);
}

inline void SwitchTo(IInvokerPtr invoker)
{
    WaitUntilSet(VoidFuture, invoker);
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
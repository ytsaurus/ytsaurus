#ifndef FIBER_INL_H_
#   error "Direct inclusion of this file is not allowed, include fiber.h"
#endif
#undef FIBER_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T WaitFor(TFuture<T> future, IInvokerPtr invoker)
{
    WaitFor(future.IgnoreResult(), std::move(invoker));
    YCHECK(future.IsSet());
    return future.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

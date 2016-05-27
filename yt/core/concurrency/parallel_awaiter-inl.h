#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_awaiter.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TParallelAwaiter::Await(TFuture<T> future, TCallback<void(const TErrorOr<T>&)> onResult)
{
    Y_ASSERT(future);
    if (TryAwait()) {
        future.Subscribe(BIND(
            &TParallelAwaiter::HandleResult<T>,
            MakeStrong(this),
            Passed(std::move(onResult))));
    }
}

template <class T>
void TParallelAwaiter::Await(TFuture<T> future)
{
    Await(std::move(future), TCallback<void(const TErrorOr<T>&)>());
}

template <class T>
void TParallelAwaiter::HandleResult(TCallback<void(const TErrorOr<T>&)> onResult, const TErrorOr<T>& result)
{
    if (onResult) {
        CancelableInvoker_->Invoke(BIND(std::move(onResult), result));
    }
    HandleResultImpl();
}

inline int TParallelAwaiter::GetRequestCount() const
{
    return RequestCount_;
}

inline int TParallelAwaiter::GetResponseCount() const
{
    return ResponseCount_;
}

inline bool TParallelAwaiter::IsCompleted() const
{
    return Completed_;
}

inline TFuture<void> TParallelAwaiter::GetAsyncCompleted() const
{
    return CompletedPromise_;
}

inline bool TParallelAwaiter::IsCanceled() const
{
    return Canceled_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

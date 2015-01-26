#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_awaiter.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(IInvokerPtr invoker)
{
    YCHECK(invoker);

    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(invoker);
}

inline bool TParallelAwaiter::TryAwait()
{
    TGuard<TSpinLock> guard(SpinLock_);
    YASSERT(!Completed_);

    if (Canceled_ || Terminated_) {
        return false;
    }

    ++RequestCount_;
    return true;
}

template <class T>
void TParallelAwaiter::Await(TFuture<T> future, TCallback<void(const TErrorOr<T>&)> onResult)
{
    YASSERT(future);

    if (TryAwait()) {
        future.Subscribe(BIND(&TParallelAwaiter::HandleResult<T>, MakeStrong(this), Passed(std::move(onResult))));
    }
}

template <class T>
void TParallelAwaiter::Await(TFuture<T> future)
{
    Await(future, TCallback<void(const TErrorOr<T>&)>());
}

template <class T>
void TParallelAwaiter::HandleResult(TCallback<void(const TErrorOr<T>&)> onResult, const TErrorOr<T>& result)
{
    if (onResult) {
        CancelableInvoker_->Invoke(BIND(onResult, result));
    }

    bool fireCompleted = false;
    TClosure onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Canceled_ || Terminated_)
            return;

        ++ResponseCount_;

        fireCompleted = (ResponseCount_ == RequestCount_) && Completed_;

        if (fireCompleted) {
            onComplete = std::move(OnComplete_);
            Terminated_ = true;
        }
    }

    if (fireCompleted) {
        FireCompleted(std::move(onComplete));
    }
}

inline TFuture<void> TParallelAwaiter::Complete(TClosure onComplete)
{
    bool fireCompleted;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YASSERT(!Completed_);
        if (Canceled_ || Terminated_) {
            return CompletedPromise_;
        }

        Completed_ = true;

        fireCompleted = (RequestCount_ == ResponseCount_);

        if (fireCompleted) {
            Terminated_ = true;
        } else {
            OnComplete_ = std::move(onComplete);
        }
    }

    if (fireCompleted) {
        FireCompleted(std::move(onComplete));
    }

    return CompletedPromise_;
}

inline void TParallelAwaiter::FireCompleted(TClosure onComplete)
{
    auto this_ = MakeStrong(this);
    CancelableInvoker_->Invoke(BIND([=] () {
        UNUSED(this_);
        if (onComplete) {
            onComplete.Run();
        }
        CompletedPromise_.Set();
    }));
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

inline void TParallelAwaiter::Cancel()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Canceled_)
        return;

    CancelableContext_->Cancel();
    OnComplete_.Reset();
    Canceled_ = true;
    Terminated_ = true;
}

inline bool TParallelAwaiter::IsCanceled() const
{
    return Canceled_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

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

    Canceled_ = false;

    Completed_ = false;
    CompletedPromise_ = NewPromise();

    Terminated_ = false;

    RequestCount_ = 0;
    ResponseCount_ = 0;

    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(invoker);
}

inline bool TParallelAwaiter::TryAwait()
{
    TGuard<TSpinLock> guard(SpinLock_);
    YASSERT(!Completed_);

    if (Canceled_ || Terminated_)
        return false;

    ++RequestCount_;
    return true;
}

template <class T>
void TParallelAwaiter::Await(TFuture<T> result, TCallback<void(const T&)> onResult)
{
    DoAwait(
        std::move(result),
        BIND(&TParallelAwaiter::HandleResult<const T&>, MakeStrong(this), Passed(std::move(onResult))));
}

template <class T>
void TParallelAwaiter::Await(TFuture<T> result, TCallback<void(T)> onResult)
{
    DoAwait(
        std::move(result),
        BIND(&TParallelAwaiter::HandleResult<T>, MakeStrong(this), Passed(std::move(onResult))));
}

inline void TParallelAwaiter::Await(TFuture<void> result, TClosure onResult)
{
    DoAwait(
        std::move(result),
        BIND(&TParallelAwaiter::HandleVoidResult, MakeStrong(this), Passed(std::move(onResult))));
}

template <class T>
void TParallelAwaiter::Await(TFuture<T> result)
{
    DoAwait(
        std::move(result),
        BIND(&TParallelAwaiter::HandleResult<T>, MakeStrong(this), TCallback<void(T)>()));
}

inline void TParallelAwaiter::Await(TFuture<void> result)
{
    DoAwait(
        std::move(result),
        BIND(&TParallelAwaiter::HandleVoidResult, MakeStrong(this), TClosure()));
}

template <class TResult, class THandler>
void TParallelAwaiter::DoAwait(TResult result, THandler onResultHandler)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(std::move(onResultHandler));
        result.OnCanceled(BIND(&TParallelAwaiter::HandleCancel, MakeStrong(this)));
    }
}

template <class T>
void TParallelAwaiter::HandleResult(TCallback<void(T)> onResult, T result)
{
    if (onResult) {
        CancelableInvoker_->Invoke(BIND(onResult, result));
    }

    HandleResponse();
}

inline void TParallelAwaiter::HandleVoidResult(TClosure onResult)
{
    if (onResult) {
        CancelableInvoker_->Invoke(onResult);
    }

    HandleResponse();
}

inline void TParallelAwaiter::HandleCancel()
{
    Cancel();
}

inline void TParallelAwaiter::HandleResponse()
{
    bool fireCompleted = false;
    TClosure onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Canceled_ || Terminated_)
            return;

        ++ResponseCount_;

        fireCompleted = (ResponseCount_ == RequestCount_) && Completed_;

        if (fireCompleted) {
            onComplete = OnComplete_;
            Terminate();
        }
    }

    if (fireCompleted) {
        DoFireCompleted(std::move(onComplete));
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

        OnComplete_ = onComplete;
        Completed_ = true;

        fireCompleted = (RequestCount_ == ResponseCount_);

        if (fireCompleted) {
            Terminate();
        }
    }

    if (fireCompleted) {
        DoFireCompleted(onComplete);
    }

    return CompletedPromise_;
}

inline void TParallelAwaiter::DoFireCompleted(TClosure onComplete)
{
    auto this_ = MakeStrong(this);
    CancelableInvoker_->Invoke(BIND([this, this_, onComplete] () {
        if (onComplete) {
            onComplete.Run();
        }
        CompletedPromise_.Set();
    }));
}

inline void TParallelAwaiter::Cancel()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Canceled_)
        return;

    CancelableContext_->Cancel();
    CompletedPromise_.Cancel();
    Canceled_ = true;
    Terminate();
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

inline void TParallelAwaiter::Terminate()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    OnComplete_.Reset();
    Terminated_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

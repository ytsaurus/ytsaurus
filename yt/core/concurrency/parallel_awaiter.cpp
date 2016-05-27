#include "parallel_awaiter.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TParallelAwaiter)

TParallelAwaiter::TParallelAwaiter(IInvokerPtr invoker)
{
    YCHECK(invoker);

    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(invoker);
}

TParallelAwaiter::~TParallelAwaiter() = default;

TFuture<void> TParallelAwaiter::Complete(TClosure onComplete)
{
    bool fireCompleted;
    {
        TGuard<TSpinLock> guard(SpinLock_);

        Y_ASSERT(!Completed_);
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

void TParallelAwaiter::Cancel()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Canceled_)
        return;

    CancelableContext_->Cancel();
    OnComplete_.Reset();
    Canceled_ = true;
    Terminated_ = true;
}

bool TParallelAwaiter::TryAwait()
{
    TGuard<TSpinLock> guard(SpinLock_);
    Y_ASSERT(!Completed_);

    if (Canceled_ || Terminated_) {
        return false;
    }

    ++RequestCount_;
    return true;
}

void TParallelAwaiter::HandleResultImpl()
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
            onComplete = std::move(OnComplete_);
            Terminated_ = true;
        }
    }

    if (fireCompleted) {
        FireCompleted(std::move(onComplete));
    }
}

void TParallelAwaiter::FireCompleted(TClosure onComplete)
{
    CancelableInvoker_->Invoke(BIND(
        &TParallelAwaiter::FireCompletedImpl,
        MakeStrong(this),
        Passed(std::move(onComplete))));
}

void TParallelAwaiter::FireCompletedImpl(TClosure onComplete)
{
    if (onComplete) {
        onComplete.Run();
    }
    CompletedPromise_.Set();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT




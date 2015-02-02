#pragma once

#include "public.h"

#include <core/actions/future.h>
#include <core/actions/invoker_util.h>
#include <core/actions/cancelable_context.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TParallelAwaiter
    : public TRefCounted
{
public:
    explicit TParallelAwaiter(IInvokerPtr invoker);

    template <class T>
    void Await(TFuture<T> future, TCallback<void(const TErrorOr<T>&)> onResult);
    template <class T>
    void Await(TFuture<T> future);

    TFuture<void> Complete(TClosure onComplete = TClosure());
    
    int GetRequestCount() const;
    int GetResponseCount() const;

    bool IsCompleted() const;
    TFuture<void> GetAsyncCompleted() const;

    void Cancel();
    bool IsCanceled() const;

private:
    TSpinLock SpinLock_;

    bool Canceled_ = false;

    bool Completed_ = false;
    TPromise<void> CompletedPromise_ = NewPromise<void>();
    TClosure OnComplete_;

    bool Terminated_ = false;

    volatile int RequestCount_ = 0;
    volatile int ResponseCount_ = 0;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;


    bool TryAwait();

    template <class T>
    void HandleResult(TCallback<void(const TErrorOr<T>&)> onResult, const TErrorOr<T>& result);

    void FireCompleted(TClosure onComplete);

};

DEFINE_REFCOUNTED_TYPE(TParallelAwaiter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_

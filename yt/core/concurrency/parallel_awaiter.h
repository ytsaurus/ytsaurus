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
    void Await(TFuture<T> result, TCallback<void(T)> onResult);

    template <class T>
    void Await(TFuture<T> result, TCallback<void(const T&)> onResult);

    void Await(TFuture<void> result, TClosure onResult);

    template <class T>
    void Await(TFuture<T> result);

    void Await(TFuture<void> result);

    TFuture<void> Complete(TClosure onComplete = TClosure());
    
    void Cancel();

    int GetRequestCount() const;
    int GetResponseCount() const;

    bool IsCompleted() const;
    TFuture<void> GetAsyncCompleted() const;

    bool IsCanceled() const;

private:
    TSpinLock SpinLock_;

    bool Canceled_;

    bool Completed_;
    TPromise<void> CompletedPromise_;
    TClosure OnComplete_;

    bool Terminated_;

    int RequestCount_;
    int ResponseCount_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;


    bool TryAwait();

    void Terminate();

    template <class TResult, class THandler>
    void DoAwait(TResult result, THandler onResultHandler);

    template <class T>
    void HandleResult(TCallback<void(T)> onResult, T result);

    void HandleVoidResult(TClosure onResult);

    void HandleCancel();

    void HandleResponse();

    void DoFireCompleted(TClosure onComplete);

};

DEFINE_REFCOUNTED_TYPE(TParallelAwaiter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_

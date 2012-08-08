#pragma once

#include "common.h"
#include "future.h"
#include "invoker_util.h"
#include "cancelable_context.h"

#include <ytlib/profiling/profiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TParallelAwaiter
    : public TRefCounted
{
public:
    explicit TParallelAwaiter(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler = NULL,
        const NYTree::TYPath& timerPath = "");
    explicit TParallelAwaiter(
        NProfiling::TProfiler* profiler = NULL,
        const NYTree::TYPath& timerPath = "");

    template <class T>
    void Await(
        TFuture<T> result,
        TCallback<void(T)> onResult = TCallback<void(T)>());
    template <class T>
    void Await(
        TFuture<T> result,
        const NYTree::TYPath& timerPathSuffix,
        TCallback<void(T)> onResult = TCallback<void(T)>());

    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        TCallback<void()> onResult = TCallback<void()>());
    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        const NYTree::TYPath& timerPathSuffix,
        TCallback<void()> onResult = TCallback<void()>());


    void Complete(TClosure onComplete = TClosure());
    void Cancel();

    int GetRequestCount() const;
    int GetResponseCount() const;
    bool IsCanceled() const;

private:
    TSpinLock SpinLock;
    bool Canceled;
    bool Completed;
    bool Terminated;
    int RequestCount;
    int ResponseCount;
    TClosure OnComplete;
    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableInvoker;
    NProfiling::TProfiler* Profiler;
    NProfiling::TTimer Timer;

    template <class Signature>
    bool WrapOnResult(TCallback<Signature> onResult, TCallback<Signature>& wrappedOnResult);

    void MaybeInvokeOnComplete(const NYTree::TYPath& timerPathSuffix);

    void Init(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler,
        const NYTree::TYPath& timerPath);
    void Terminate();

    template <class T>
    void OnResult(
        const NYTree::TYPath& timerPathSuffix,
        TCallback<void(T)> onResult,
        T result);

    void OnResult(
        const NYTree::TYPath& timerPathSuffix,
        TCallback<void()> onResult);

};

typedef TIntrusivePtr<TParallelAwaiter> TParallelAwaiterPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_

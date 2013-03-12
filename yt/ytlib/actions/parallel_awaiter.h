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
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");
    explicit TParallelAwaiter(
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");

    template <class T>
    void Await(
        TFuture<T> result,
        TCallback<void(T)> onResult = TCallback<void(T)>());
    template <class T>
    void Await(
        TFuture<T> result,
        const Stroka& timerKey,
        TCallback<void(T)> onResult = TCallback<void(T)>());

    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        TCallback<void()> onResult = TCallback<void()>());
    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        const Stroka& timerKey,
        TCallback<void()> onResult = TCallback<void()>());

    TFuture<void> Complete(TClosure onComplete = TClosure());
    void Cancel();

    int GetRequestCount() const;
    int GetResponseCount() const;

    bool IsCompleted() const;
    TFuture<void> GetAsyncCompleted() const;

    bool IsCanceled() const;

private:
    TSpinLock SpinLock;

    bool Canceled;

    bool Completed;
    TPromise<void> CompletedPromise;
    TClosure OnComplete;

    bool Terminated;

    int RequestCount;
    int ResponseCount;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableInvoker;

    NProfiling::TProfiler* Profiler;
    NProfiling::TTimer Timer;

    bool TryAwait();

    void MaybeFireCompleted(const Stroka& key);
    void DoFireCompleted(TClosure onComplete);

    void Init(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler,
        const NYPath::TYPath& timerPath);

    void Terminate();


    template <class T>
    void OnResult(
        const Stroka& key,
        TCallback<void(T)> onResult,
        T result);

    void OnResult(
        const Stroka& key,
        TCallback<void()> onResult);

};

typedef TIntrusivePtr<TParallelAwaiter> TParallelAwaiterPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_

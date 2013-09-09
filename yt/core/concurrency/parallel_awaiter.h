#pragma once

#include "public.h"

#include <core/actions/future.h>
#include <core/actions/invoker_util.h>
#include <core/actions/cancelable_context.h>

#include <core/misc/nullable.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TParallelAwaiter
    : public TRefCounted
{
public:
    explicit TParallelAwaiter(
        IInvokerPtr invoker);

    TParallelAwaiter(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler,
        const NYPath::TYPath& timingPath);

    template <class T>
    void Await(
        TFuture<T> result,
        TCallback<void(T)> onResult = TCallback<void(T)>());
    template <class T>
    void Await(
        TFuture<T> result,
        const NProfiling::TTagIdList& tagIds,
        TCallback<void(T)> onResult = TCallback<void(T)>());

    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        TCallback<void()> onResult = TCallback<void()>());
    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        const NProfiling::TTagIdList& tagIds,
        TCallback<void()> onResult = TCallback<void()>());

    TFuture<void> Complete(
        TClosure onComplete = TClosure(),
        const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);
    
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
    NProfiling::TTagIdList CompletedTagIds;

    bool Terminated;

    int RequestCount;
    int ResponseCount;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableInvoker;

    NProfiling::TProfiler* Profiler;
    NProfiling::TTimer Timer;

    void Init(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler,
        const TNullable<NYPath::TYPath>& timingPath);

    bool TryAwait();

    void OnResultImpl(const NProfiling::TTagIdList& tagIds);
    void DoFireCompleted(TClosure onComplete);

    void Terminate();

    template <class T>
    void OnResult(
        const NProfiling::TTagIdList& tagIds,
        TCallback<void(T)> onResult,
        T result);

    void OnResult(
        const NProfiling::TTagIdList& tagIds,
        TCallback<void()> onResult);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_

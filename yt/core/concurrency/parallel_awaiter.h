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
        TCallback<void(T)> onResult = TCallback<void(T)>(),
        TClosure onCancel = TClosure());
    template <class T>
    void Await(
        TFuture<T> result,
        const NProfiling::TTagIdList& tagIds,
        TCallback<void(T)> onResult = TCallback<void(T)>(),
        TClosure onCancel = TClosure());

    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        TCallback<void()> onResult = TCallback<void()>(),
        TClosure onCancel = TClosure());
    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        const NProfiling::TTagIdList& tagIds,
        TCallback<void()> onResult = TCallback<void()>(),
        TClosure onCancel = TClosure());

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
    TSpinLock SpinLock_;

    bool Canceled_;

    bool Completed_;
    TPromise<void> CompletedPromise_;
    TClosure OnComplete_;
    NProfiling::TTagIdList CompletedTagIds_;

    bool Terminated_;

    int RequestCount_;
    int ResponseCount_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    NProfiling::TProfiler* Profiler_;
    NProfiling::TTimer Timer_;


    void Init(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler,
        const TNullable<NYPath::TYPath>& timingPath);

    bool TryAwait();

    void Terminate();

    template <class T>
    void HandleResult(
        const NProfiling::TTagIdList& tagIds,
        TCallback<void(T)> onResult,
        T result);

    void HandleResult(
        const NProfiling::TTagIdList& tagIds,
        TCallback<void()> onResult);

    void HandleCancel(
        const NProfiling::TTagIdList& tagIds,
        TClosure onCancel);

    void HandleResponse(const NProfiling::TTagIdList& tagIds);
    void DoFireCompleted(TClosure onComplete);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_

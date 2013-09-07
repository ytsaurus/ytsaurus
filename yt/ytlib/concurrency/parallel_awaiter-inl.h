#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_awaiter.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

#include <ytlib/concurrency/thread_affinity.h>

#include <ytlib/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(
    IInvokerPtr invoker)
{
    Init(invoker, nullptr, Null);
}

inline TParallelAwaiter::TParallelAwaiter(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const NYPath::TYPath& timingPath)
{
    Init(invoker, profiler, timingPath);
}

inline void TParallelAwaiter::Init(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const TNullable<NYPath::TYPath>& timingPath)
{
    YCHECK(invoker);

    Canceled = false;

    Completed = false;
    CompletedPromise = NewPromise();

    Terminated = false;

    RequestCount = 0;
    ResponseCount = 0;

    CancelableContext = New<TCancelableContext>();
    CancelableInvoker = CancelableContext->CreateInvoker(invoker);

    Profiler = profiler;

    if (Profiler && timingPath) {
        Timer = Profiler->TimingStart(
            *timingPath,
            NProfiling::EmptyTagIds,
            NProfiling::ETimerMode::Parallel);
    }
}

inline bool TParallelAwaiter::TryAwait()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(!Completed);

    if (Canceled || Terminated)
        return false;

    ++RequestCount;
    return true;
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    const NProfiling::TTagIdList& tagIds,
    TCallback<void(T)> onResult)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(BIND(
            &TParallelAwaiter::OnResult<T>,
            MakeStrong(this),
            tagIds,
            Passed(std::move(onResult))));
    }
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    const NProfiling::TTagIdList& tagIds,
    TCallback<void(void)> onResult)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(BIND(
            (void(TParallelAwaiter::*)(const NProfiling::TTagIdList&, TCallback<void()>)) &TParallelAwaiter::OnResult,
            MakeStrong(this),
            tagIds,
            Passed(std::move(onResult))));
    }
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    TCallback<void(T)> onResult)
{
    Await(std::move(result), NProfiling::EmptyTagIds, std::move(onResult));
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    TCallback<void()> onResult)
{
    Await(std::move(result), NProfiling::EmptyTagIds, std::move(onResult));
}

inline void TParallelAwaiter::OnResultImpl(const NProfiling::TTagIdList& tagIds)
{
    bool fireCompleted = false;
    TClosure onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (Canceled || Terminated)
            return;

        if (Profiler) {
            Profiler->TimingCheckpoint(Timer, tagIds);
        }

        ++ResponseCount;

        fireCompleted = (ResponseCount == RequestCount) && Completed;

        if (fireCompleted) {
            onComplete = OnComplete;
            Terminate();
        }
    }

    if (fireCompleted) {
        DoFireCompleted(std::move(onComplete));
    }
}

inline void TParallelAwaiter::DoFireCompleted(TClosure onComplete)
{
    auto this_ = MakeStrong(this);
    CancelableInvoker->Invoke(BIND([this, this_, onComplete] () {
        if (onComplete) {
            onComplete.Run();
        }
        CompletedPromise.Set();
    }));
}

template <class T>
void TParallelAwaiter::OnResult(
    const NProfiling::TTagIdList& tagIds,
    TCallback<void(T)> onResult,
    T result)
{
    if (onResult) {
        CancelableInvoker->Invoke(BIND(onResult, result));
    }

    OnResultImpl(tagIds);
}

inline void TParallelAwaiter::OnResult(
    const NProfiling::TTagIdList& tagIds,
    TCallback<void()> onResult)
{
    if (onResult) {
        CancelableInvoker->Invoke(onResult);
    }

    OnResultImpl(tagIds);
}

inline TFuture<void> TParallelAwaiter::Complete(
    TClosure onComplete,
    const NProfiling::TTagIdList& tagIds)
{
    bool fireCompleted;
    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(!Completed);
        if (Canceled || Terminated) {
            return CompletedPromise;
        }

        OnComplete = onComplete;
        CompletedTagIds = tagIds;
        Completed = true;

        fireCompleted = (RequestCount == ResponseCount);

        if (fireCompleted) {
            Terminate();
        }
    }

    if (fireCompleted) {
        DoFireCompleted(onComplete);
    }

    return CompletedPromise;
}

inline void TParallelAwaiter::Cancel()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (Canceled)
        return;

    CancelableContext->Cancel();
    Canceled = true;
    Terminate();
}

inline int TParallelAwaiter::GetRequestCount() const
{
    return RequestCount;
}

inline int TParallelAwaiter::GetResponseCount() const
{
    return ResponseCount;
}

inline bool TParallelAwaiter::IsCompleted() const
{
    return Completed;
}

inline TFuture<void> TParallelAwaiter::GetAsyncCompleted() const
{
    return CompletedPromise;
}

inline bool TParallelAwaiter::IsCanceled() const
{
    return Canceled;
}

inline void TParallelAwaiter::Terminate()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);

    if (Terminated)
        return;

    if (Completed && Profiler) {
        Profiler->TimingStop(Timer, CompletedTagIds);
    }

    OnComplete.Reset();
    Terminated = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

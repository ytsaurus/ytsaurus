#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_awaiter.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

#include <ytlib/misc/thread_affinity.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const NYPath::TYPath& timerPath)
{
    Init(invoker, profiler, timerPath);
}

inline TParallelAwaiter::TParallelAwaiter(
    NProfiling::TProfiler* profiler,
    const NYPath::TYPath& timerPath)
{
    Init(GetSyncInvoker(), profiler, timerPath);
}

inline void TParallelAwaiter::Init(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const NYPath::TYPath& timerPath)
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
    if (Profiler) {
        Timer = Profiler->TimingStart(timerPath, NProfiling::ETimerMode::Parallel);
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
    const Stroka& timerKey,
    TCallback<void(T)> onResult)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(BIND(
            &TParallelAwaiter::OnResult<T>,
            MakeStrong(this),
            timerKey,
            Passed(std::move(onResult))));
    }
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    const Stroka& timerKey,
    TCallback<void(void)> onResult)
{
    YASSERT(result);

    if (TryAwait()) {
        result.Subscribe(BIND(
            (void(TParallelAwaiter::*)(const NYPath::TYPath&, TCallback<void()>)) &TParallelAwaiter::OnResult,
            MakeStrong(this),
            timerKey,
            Passed(std::move(onResult))));
    }
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    TCallback<void(T)> onResult)
{
    Await(std::move(result), "", std::move(onResult));
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    TCallback<void()> onResult)
{
    Await(std::move(result), "", std::move(onResult));
}

inline void TParallelAwaiter::MaybeFireCompleted(const Stroka& timerKey)
{
    bool fireCompleted = false;
    TClosure onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (Canceled || Terminated)
            return;

        if (Profiler && !timerKey.empty()) {
            Profiler->TimingCheckpoint(Timer, timerKey);
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
    CancelableInvoker->Invoke(BIND([=] () {
        if (!onComplete.IsNull()) {
            onComplete.Run();
        }
        this_->CompletedPromise.Set();
    }));
}

template <class T>
void TParallelAwaiter::OnResult(
    const Stroka& timerKey,
    TCallback<void(T)> onResult,
    T result)
{
    if (!onResult.IsNull()) {
        CancelableInvoker->Invoke(BIND(onResult, result));
    }

    MaybeFireCompleted(timerKey);
}

inline void TParallelAwaiter::OnResult(
    const Stroka& timerKey,
    TCallback<void()> onResult)
{
    if (!onResult.IsNull()) {
        CancelableInvoker->Invoke(onResult);
    }

    MaybeFireCompleted(timerKey);
}

inline TFuture<void> TParallelAwaiter::Complete(TClosure onComplete)
{
    bool fireCompleted;
    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(!Completed);
        if (Canceled || Terminated) {
            return CompletedPromise;
        }

        OnComplete = onComplete;
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

    OnComplete.Reset();

    if (Profiler) {
        Profiler->TimingStop(Timer);
    }

    Terminated = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

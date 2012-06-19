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
    const NYTree::TYPath& timerPath)
{
    Init(invoker, profiler, timerPath);
}

inline TParallelAwaiter::TParallelAwaiter(
    NProfiling::TProfiler* profiler,
    const NYTree::TYPath& timerPath)
{
    Init(TSyncInvoker::Get(), profiler, timerPath);
}

inline void TParallelAwaiter::Init(
    IInvokerPtr invoker,
    NProfiling::TProfiler* profiler,
    const NYTree::TYPath& timerPath)
{
    YASSERT(invoker);

    Canceled = false;
    Completed = false;
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

template <class Signature>
bool TParallelAwaiter::WrapOnResult(TCallback<Signature> onResult, TCallback<Signature>& wrappedOnResult)
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(!Completed);

    if (Canceled || Terminated)
        return false;

    ++RequestCount;

    if (!onResult.IsNull()) {
        wrappedOnResult = onResult.Via(CancelableInvoker);
    }
    return true;
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    const NYTree::TYPath& timerPathSuffix,
    TCallback<void(T)> onResult)
{
    YASSERT(!result.IsNull());

    TCallback<void(T)> wrappedOnResult;
    if (WrapOnResult(onResult, wrappedOnResult)) {
        result.Subscribe(BIND(
            &TParallelAwaiter::OnResult<T>,
            MakeStrong(this),
            timerPathSuffix,
            Passed(MoveRV(wrappedOnResult))));
    }
}

inline void TParallelAwaiter::Await(
    TFuture<void> result,
    const NYTree::TYPath& timerPathSuffix,
    TCallback<void(void)> onResult)
{
    YASSERT(!result.IsNull());

    TCallback<void(void)> wrappedOnResult;
    if (WrapOnResult(onResult, wrappedOnResult)) {
        result.Subscribe(BIND(
            (void(TParallelAwaiter::*)(const NYTree::TYPath&, TCallback<void()>))&TParallelAwaiter::OnResult,
            MakeStrong(this),
            timerPathSuffix,
            Passed(MoveRV(wrappedOnResult))));
    }
}

template <class T>
void TParallelAwaiter::Await(
    TFuture<T> result,
    TCallback<void(T)> onResult)
{
    Await(MoveRV(result), "", MoveRV(onResult));
}

inline void TParallelAwaiter::MaybeInvokeOnComplete(const NYTree::TYPath& timerPathSuffix)
{
    bool invokeOnComplete = false;
    TClosure onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (Canceled || Terminated)
            return;

        if (Profiler && !timerPathSuffix.empty()) {
            Profiler->TimingCheckpoint(Timer, timerPathSuffix);
        }

        ++ResponseCount;

        onComplete = OnComplete;
        invokeOnComplete = ResponseCount == RequestCount && Completed;
        if (invokeOnComplete) {
            Terminate();
        }
    }

    if (invokeOnComplete && !onComplete.IsNull()) {
        onComplete.Run();
    }
}

template <class T>
void TParallelAwaiter::OnResult(
    const NYTree::TYPath& timerPathSuffix,
    TCallback<void(T)> onResult,
    T result)
{
    if (!onResult.IsNull()) {
        onResult.Run(result);
    }

    MaybeInvokeOnComplete(timerPathSuffix);
}

inline void TParallelAwaiter::OnResult(
    const NYTree::TYPath& timerPathSuffix,
    TCallback<void()> onResult)
{
    if (!onResult.IsNull()) {
        onResult.Run();
    }
    MaybeInvokeOnComplete(timerPathSuffix);
}


inline void TParallelAwaiter::Complete(TClosure onComplete)
{
    if (!onComplete.IsNull()) {
        onComplete = onComplete.Via(CancelableInvoker);
    }

    bool invokeOnComplete;
    {
        TGuard<TSpinLock> guard(SpinLock);

        YASSERT(!Completed);
        if (Canceled || Terminated)
            return;

        OnComplete = onComplete;
        Completed = true;
        
        invokeOnComplete = RequestCount == ResponseCount;
        if (invokeOnComplete) {
            Terminate();
        }
    }

    if (invokeOnComplete && !onComplete.IsNull()) {
        onComplete.Run();
    }
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

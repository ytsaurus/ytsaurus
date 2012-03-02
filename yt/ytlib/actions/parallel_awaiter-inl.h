#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include action_util.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

#include <ytlib/misc/thread_affinity.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(
    IInvoker* invoker,
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
    IInvoker* invoker,
    NProfiling::TProfiler* profiler,
    const NYTree::TYPath& timerPath)
{
    YASSERT(invoker);

    Canceled = false;
    Completed = false;
    Terminated = false;
    RequestCount = 0;
    ResponseCount = 0;
    CancelableInvoker = New<TCancelableInvoker>(invoker);
    Profiler = profiler;
    if (Profiler) {
        Timer = Profiler->TimingStart(timerPath, NProfiling::ETimerMode::Parallel);
    }
}

template <class T>
void TParallelAwaiter::Await(
    TIntrusivePtr< TFuture<T> > result,
    const NYTree::TYPath& timerPathSuffix,
    TIntrusivePtr< IParamAction<T> > onResult)
{
    YASSERT(result);

    typename IParamAction<T>::TPtr wrappedOnResult;
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(!Completed);

        if (Canceled || Terminated)
            return;

        ++RequestCount;

        if (onResult) {
            wrappedOnResult = onResult->Via(CancelableInvoker);
        }
    }

    result->Subscribe(FromMethod(
        &TParallelAwaiter::OnResult<T>,
        TPtr(this),
        timerPathSuffix,
        wrappedOnResult));
}

template <class T>
void TParallelAwaiter::Await(
    TIntrusivePtr< TFuture<T> > result,
    TIntrusivePtr< IParamAction<T> > onResult)
{
    Await(result, "", onResult);
}

template <class T>
void TParallelAwaiter::OnResult(
    T result,
    const NYTree::TYPath& timerPathSuffix,
    typename IParamAction<T>::TPtr onResult)
{
    if (onResult) {
        onResult->Do(result);
    }

    bool invokeOnComplete = false;
    IAction::TPtr onComplete;
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

    if (invokeOnComplete && onComplete) {
        onComplete->Do();
    }
}

inline void TParallelAwaiter::Complete(IAction::TPtr onComplete)
{
    if (onComplete) {
        onComplete = onComplete->Via(CancelableInvoker);
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

    if (invokeOnComplete && onComplete) {
        onComplete->Do();
    }
}

inline void TParallelAwaiter::Cancel()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (Canceled)
        return;

    CancelableInvoker->Cancel();
    Canceled = true;
    Terminate();
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

#ifndef PARALLEL_AWAITER_INL_H_
#error "Direct inclusion of this file is not allowed, include action_util.h"
#endif
#undef PARALLEL_AWAITER_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(IInvoker::TPtr invoker)
    : Canceled(false)
    , Completed(false)
    , Terminated(false)
    , RequestCount(0)
    , ResponseCount(0)
    , CancelableInvoker(new TCancelableInvoker(invoker))
{ }


template<class T>
void TParallelAwaiter::Await(
    TIntrusivePtr< TAsyncResult<T> > result,
    TIntrusivePtr< IParamAction<T> > onResult)
{
    typename IParamAction<T>::TPtr wrappedOnResult;
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(!Completed);

        if (Canceled || Terminated)
            return;

        ++RequestCount;

        if (~onResult != NULL) {
            wrappedOnResult = onResult->Via(~CancelableInvoker);
        }
    }

    result->Subscribe(FromMethod(
        &TParallelAwaiter::OnResult<T>,
        TPtr(this),
        wrappedOnResult));
}

template<class T>
void TParallelAwaiter::OnResult(T result, typename IParamAction<T>::TPtr onResult)
{
    if (~onResult != NULL) {
        onResult->Do(result);
    }

    bool invokeOnComplete = false;
    IAction::TPtr onComplete;
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (Canceled || Terminated)
            return;

        ++ResponseCount;
        
        onComplete = OnComplete;
        invokeOnComplete = ResponseCount == RequestCount && Completed;
        if (invokeOnComplete) {
            Terminate();
        }
    }

    if (invokeOnComplete && ~onComplete != NULL) {
        onComplete->Do();
    }
}

inline void TParallelAwaiter::Complete(IAction::TPtr onComplete)
{
    if (~onComplete != NULL) {
        onComplete = onComplete->Via(~CancelableInvoker);
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

    if (invokeOnComplete && ~onComplete != NULL) {
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
    OnComplete.Drop();
    Terminated = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

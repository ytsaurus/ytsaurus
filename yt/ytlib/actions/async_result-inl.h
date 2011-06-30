#ifndef ASYNC_RESULT_INL_H_
#error "Direct inclusion of this file is not allowed, include action_util.h"
#endif
#undef ASYNC_RESULT_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAsyncResult<T>::TAsyncResult()
    : IsSet(false)
    , Value(T())
{}

template <class T>
TAsyncResult<T>::TAsyncResult(T value)
    : IsSet(true)
    , Value(value)
{}

template <class T>
void TAsyncResult<T>::Set(T value)
{
    YASSERT(!IsSet);
    Value = value;
    IsSet = true;
    {
        TGuard<TSpinLock> guard(SpinLock);
        Event* event = ~ReadyEvent;
        if (event != NULL) {
            event->Signal();
        }
        IParamAction<T>* subscriber = ~Subscriber;
        if (subscriber != NULL) {
            subscriber->Do(value);
            Subscriber = NULL;
        }
    }
}

template <class T>
T TAsyncResult<T>::Get() const
{
    if (IsSet) {
        return Value;
    }
    if (~ReadyEvent == NULL) {
        TGuard<TSpinLock> guard(SpinLock);
        if (~ReadyEvent == NULL) {
            ReadyEvent.Reset(new Event());
        }
    }
    ReadyEvent->Wait();
    YASSERT(IsSet);
    return Value;
}

template <class T>
bool TAsyncResult<T>::TryGet(T* value) const
{
    if (IsSet) {
        *value = Value;
        return true;
    }
    return false;
}

template <class T>
void TAsyncResult<T>::Subscribe(typename IParamAction<T>::TPtr action)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (IsSet) {
        action->Do(Value);
    } else {
        Subscriber = action;
    }
}

template <class T, class TOther>
void ApplyFuncThunk(
    T value,
    typename TAsyncResult<TOther>::TPtr otherResult,
    typename IParamFunc<T, TOther>::TPtr func)
{
    otherResult->Set(func->Do(value));
}

template <class T>
template <class TOther>
TIntrusivePtr< TAsyncResult<TOther> >
TAsyncResult<T>::Apply(TIntrusivePtr< IParamFunc<T, TOther> > func)
{
    typename TAsyncResult<TOther>::TPtr otherResult = new TAsyncResult<TOther>();
    Subscribe(FromMethod(&ApplyFuncThunk<T, TOther>, otherResult, func));
    return otherResult;
}

template <class T, class TOther>
void AsyncApplyFuncThunk(
    T value,
    typename TAsyncResult<TOther>::TPtr otherResult,
    typename IParamFunc<T, typename TAsyncResult<TOther>::TPtr>::TPtr func)
{
    func->Do(value)->Subscribe(FromMethod(
        &TAsyncResult<TOther>::Set, otherResult));
}

template <class T>
template <class TOther>
TIntrusivePtr< TAsyncResult<TOther> >
TAsyncResult<T>::Apply(TIntrusivePtr< IParamFunc<T, TIntrusivePtr< TAsyncResult<TOther> > > > func)
{
    typename TAsyncResult<TOther>::TPtr otherResult = new TAsyncResult<TOther>();
    Subscribe(FromMethod(&AsyncApplyFuncThunk<T, TOther>, otherResult, func));
    return otherResult;
}

////////////////////////////////////////////////////////////////////////////////

inline TParallelAwaiter::TParallelAwaiter(IInvoker::TPtr invoker)
    : Canceled(false)
    , Completed(false)
    , Terminated(false)
    , RequestCount(0)
    , ResponseCount(0)
    , UserInvoker(invoker)
    , CancelableInvoker(new TCancelableInvoker())
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
            wrappedOnResult = onResult->Via(~CancelableInvoker)->Via(UserInvoker);
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
        onComplete = onComplete->Via(~CancelableInvoker)->Via(UserInvoker);
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
    UserInvoker.Drop();
    Terminated = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

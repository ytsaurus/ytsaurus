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
{ }

template <class T>
void TAsyncResult<T>::Set(T value)
{
    yvector<typename IParamAction<T>::TPtr> subscribers;

    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(!IsSet);
        Value = value;
        IsSet = true;

        Event* event = ~ReadyEvent;
        if (event != NULL) {
            event->Signal();
        }

        Subscribers.swap(subscribers);
    }

    for (typename yvector<typename IParamAction<T>::TPtr>::iterator it = subscribers.begin();
        it != subscribers.end();
        ++it)
    {
        (*it)->Do(value);
    }
}

template <class T>
T TAsyncResult<T>::Get() const
{
    TGuard<TSpinLock> guard(SpinLock);

    if (IsSet) {
        return Value;
    }

    if (~ReadyEvent == NULL) {
        ReadyEvent.Reset(new Event());
    }

    guard.Release();
    ReadyEvent->Wait();

    YASSERT(IsSet);

    return Value;
}

template <class T>
bool TAsyncResult<T>::TryGet(T* value) const
{
    TGuard<TSpinLock> guard(SpinLock);
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
        guard.Release();
        action->Do(Value);
    } else {
        Subscribers.push_back(action);
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
    typename TAsyncResult<TOther>::TPtr otherResult = New< TAsyncResult<TOther> >();
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
    typename TAsyncResult<TOther>::TPtr otherResult = New< TAsyncResult<TOther> >();
    Subscribe(FromMethod(&AsyncApplyFuncThunk<T, TOther>, otherResult, func));
    return otherResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

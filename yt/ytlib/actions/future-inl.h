#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include "../misc/foreach.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T>::TFuture()
    : IsSet_(false)
    , Value(T())
{}

template <class T>
TFuture<T>::TFuture(T value)
    : IsSet_(true)
    , Value(value)
{ }

template <class T>
void TFuture<T>::Set(T value)
{
    yvector<typename IParamAction<T>::TPtr> subscribers;

    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(!IsSet_);
        Value = value;
        IsSet_ = true;

        Event* event = ~ReadyEvent;
        if (event != NULL) {
            event->Signal();
        }

        Subscribers.swap(subscribers);
    }

    FOREACH(auto& subscriber, subscribers) {
        subscriber->Do(value);
    }
}

template <class T>
T TFuture<T>::Get() const
{
    TGuard<TSpinLock> guard(SpinLock);

    if (IsSet_) {
        return Value;
    }

    if (~ReadyEvent == NULL) {
        ReadyEvent.Reset(new Event());
    }

    guard.Release();
    ReadyEvent->Wait();

    YASSERT(IsSet_);

    return Value;
}

template <class T>
bool TFuture<T>::TryGet(T* value) const
{
    TGuard<TSpinLock> guard(SpinLock);
    if (IsSet_) {
        *value = Value;
        return true;
    }
    return false;
}

template <class T>
bool TFuture<T>::IsSet() const
{
    return IsSet_;
}

template <class T>
void TFuture<T>::Subscribe(typename IParamAction<T>::TPtr action)
{
    YASSERT(~action != NULL);

    TGuard<TSpinLock> guard(SpinLock);
    if (IsSet_) {
        guard.Release();
        action->Do(Value);
    } else {
        Subscribers.push_back(action);
    }
}

template <class T, class TOther>
void ApplyFuncThunk(
    T value,
    typename TFuture<TOther>::TPtr otherResult,
    typename IParamFunc<T, TOther>::TPtr func)
{
    YASSERT(~otherResult != NULL);
    YASSERT(~func != NULL);

    otherResult->Set(func->Do(value));
}

template <class T>
template <class TOther>
TIntrusivePtr< TFuture<TOther> >
TFuture<T>::Apply(TIntrusivePtr< IParamFunc<T, TOther> > func)
{
    auto otherResult = New< TFuture<TOther> >();
    Subscribe(FromMethod(&ApplyFuncThunk<T, TOther>, otherResult, func));
    return otherResult;
}

template <class T, class TOther>
void AsyncApplyFuncThunk(
    T value,
    typename TFuture<TOther>::TPtr otherResult,
    typename IParamFunc<T, typename TFuture<TOther>::TPtr>::TPtr func)
{
    YASSERT(~func != NULL);

    func->Do(value)->Subscribe(FromMethod(
        &TFuture<TOther>::Set, otherResult));
}

template <class T>
template <class TOther>
TIntrusivePtr< TFuture<TOther> >
TFuture<T>::Apply(TIntrusivePtr< IParamFunc<T, TIntrusivePtr< TFuture<TOther> > > > func)
{
    auto otherResult = New< TFuture<TOther> >();
    Subscribe(FromMethod(&AsyncApplyFuncThunk<T, TOther>, otherResult, func));
    return otherResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

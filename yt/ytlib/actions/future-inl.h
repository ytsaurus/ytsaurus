#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/actions/bind.h>

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
    , Value(MoveRV(value))
{ }

template <class T>
void TFuture<T>::Set(T value)
{
    yvector< TCallback<void(T)> > subscribers;

    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(!IsSet_);
        Value = MoveRV(value);
        IsSet_ = true;

        Event* event = ~ReadyEvent;
        if (event) {
            event->Signal();
        }

        Subscribers.swap(subscribers);
    }

    FOREACH(auto& subscriber, subscribers) {
        subscriber.Run(Value);
    }
}

template <class T>
T TFuture<T>::Get() const
{
    TGuard<TSpinLock> guard(SpinLock);

    if (IsSet_) {
        return Value;
    }

    if (!ReadyEvent) {
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
void TFuture<T>::Subscribe(TCallback<void(T)> action)
{
    YASSERT(!action.IsNull());

    TGuard<TSpinLock> guard(SpinLock);
    if (IsSet_) {
        guard.Release();
        action.Run(Value);
    } else {
        Subscribers.push_back(MoveRV(action));
    }
}

template <class T, class R>
void ApplyFuncThunk(
    TIntrusivePtr< TFuture<R> > otherResult,
    TCallback<R(T)> func,
    T value)
{
    otherResult->Set(func.Run(MoveRV(value)));
}

template <class T>
template <class R>
TIntrusivePtr< TFuture<R> >
TFuture<T>::Apply(TCallback<R(T)> func)
{
    YASSERT(!func.IsNull());

    auto otherResult = New< TFuture<R> >();
    Subscribe(BIND(&ApplyFuncThunk<T, R>, otherResult, Passed(MoveRV(func))));
    return otherResult;
}

template <class T, class R>
void AsyncApplyFuncThunk(
    TIntrusivePtr< TFuture<R> > otherResult,
    TCallback<TIntrusivePtr< TFuture<R> >(T)> func,
    T value)
{
    YASSERT(!func.IsNull());

    func.Run(MoveRV(value))->Subscribe(BIND(&TFuture<R>::Set, MoveRV(otherResult)));
}

template <class T>
template <class R>
TIntrusivePtr< TFuture<R> >
TFuture<T>::Apply(TCallback<TIntrusivePtr< TFuture<R> >(T)> func)
{
    YASSERT(!func.IsNull());

    auto otherResult = New< TFuture<R> >();
    Subscribe(BIND(&AsyncApplyFuncThunk<T, R>, otherResult, Passed(MoveRV(func))));
    return otherResult;
}

template <class T>
template <class R>
TIntrusivePtr< TFuture<R> > TFuture<T>::CastTo()
{
    return Apply(BIND([] (T value) { return R(value); }));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPromiseWaiter
    : public TRefCounted
{
public:
    TPromiseWaiter(
        TIntrusivePtr< TFuture<T> > promise,
        TDuration timeout,
        TCallback<void(T)> onResult,
        TCallback<void()> onTimeout)
        : OnResult(onResult)
        , OnTimeout(onTimeout)
        , Flag(0)
    {
        promise->Subscribe(
            BIND(&TPromiseWaiter::DoResult, MakeStrong(this)));
        TDelayedInvoker::Submit(
            BIND(&TPromiseWaiter::DoTimeout, MakeStrong(this)),
            timeout);
    }

private:
    TCallback<void(T)> OnResult;
    TCallback<void()> OnTimeout;

    TAtomic Flag;

    bool AtomicAquire()
    {
        return AtomicCas(&Flag, 1, 0);
    }

    void DoResult(T value)
    {
        if (AtomicAquire()) {
            OnResult.Run(value);
        }
    }

    void DoTimeout()
    {
        if (AtomicAquire()) {
            OnTimeout.Run();
        }
    }
};

template <class T>
void WaitForPromise(
    TIntrusivePtr< TFuture<T> > promise,
    TDuration timeout,
    TCallback<void(T)> onResult,
    TCallback<void()> onTimeout)
{
    New< TPromiseWaiter<T> >(promise, timeout, onResult, onTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

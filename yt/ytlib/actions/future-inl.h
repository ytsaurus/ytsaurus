#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/actions/bind.h>
#include <ytlib/actions/callback.h>

#include <util/system/event.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPromiseState
    : public TIntrinsicRefCounted
{
public:
    typedef TCallback<void(T)> TListener;
    typedef std::vector<TListener> TListeners;

private:
    TNullable<T> Value;
    mutable TSpinLock SpinLock;
    mutable ::THolder<Event> ReadyEvent;

    TListeners Listeners;

public:
    TPromiseState()
    { }

    template <class U>
    explicit TPromiseState(U&& value)
        : Value(ForwardRV<U>(value))
    {
        static_assert(
            NMpl::TIsConvertible<U, T>::Value,
            "U have to be convertible to T");
    }

    const T& Get() const
    {
        TGuard<TSpinLock> guard(SpinLock);
        
        if (Value.IsInitialized()) {
            return Value.Get();
        }
        
        if (!ReadyEvent) {
            ReadyEvent.Reset(new Event());
        }
        
        guard.Release();
        ReadyEvent->Wait();
        
        YASSERT(Value.IsInitialized());       
        return Value.Get();
    }

    bool IsSet() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock);
        return Value.IsInitialized();
    }

    TNullable<T> TryGet() const
    {
        TGuard<TSpinLock> guard(SpinLock);
        return Value;
    }

    template <class U>
    void Set(U&& value)
    {
        static_assert(
            NMpl::TIsConvertible<U, T>::Value,
            "U have to be convertible to T");

        TListeners listeners;
        
        {
            TGuard<TSpinLock> guard(SpinLock);
            YASSERT(!Value.IsInitialized());
            Value.Assign(ForwardRV<U>(value));
        
            auto* event = ~ReadyEvent;
            if (event) {
                event->Signal();
            }
        
            Listeners.swap(listeners);
        }
        
        const T& storedValue = Value.Get();
        FOREACH (auto& listener, listeners) {
            listener.Run(storedValue);
        }
    }

    inline void Subscribe(const TListener& listener);
    inline void Subscribe(
        TDuration timeout,
        const TListener& onValue,
        const TClosure& onTimeout);
};

template <class T>
class TPromiseAwaiter
    : public TIntrinsicRefCounted
{
public:
    TPromiseAwaiter(
        TPromiseState<T>* state,
        TDuration timeout,
        const TCallback<void(T)>& onValue,
        const TClosure& onTimeout)
        : OnValue(onValue)
        , OnTimeout(onTimeout)
        , CallbackAlreadyRan(0)
    {
        YASSERT(state);

        state->Subscribe(
            BIND(&TPromiseAwaiter::DoValue, MakeStrong(this)));
        TDelayedInvoker::Submit(
            BIND(&TPromiseAwaiter::DoTimeout, MakeStrong(this)), timeout);
    }

private:
    TCallback<void(T)> OnValue;
    TClosure OnTimeout;

    TAtomic CallbackAlreadyRan; 

    bool AtomicAcquire()
    {
        return AtomicCas(&CallbackAlreadyRan, 1, 0);
    }

    void DoValue(T value)
    {
        if (AtomicAcquire()) {
            OnValue.Run(MoveRV(value));
        }
    }

    void DoTimeout()
    {
        if (AtomicAcquire()) {
            OnTimeout.Run();
        }
    }
};

template <class T>
void TPromiseState<T>::Subscribe(const typename TPromiseState<T>::TListener& listener)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (Value.IsInitialized()) {
        guard.Release();
        listener.Run(Value.Get());
    } else {
        Listeners.push_back(listener);
    }
}

template <class T>
void TPromiseState<T>::Subscribe(TDuration timeout, const TListener& onValue, const TClosure& onTimeout)
{
    New< TPromiseAwaiter<T> >(this, timeout, onValue, onTimeout);
}

////////////////////////////////////////////////////////////////////////////////

template <>
class TPromiseState<void>
    : public TIntrinsicRefCounted
{
public:
    typedef TCallback<void()> TListener;
    typedef std::vector<TListener> TListeners;

private:
    bool ValueIsInitialized;
    mutable TSpinLock SpinLock;
    mutable ::THolder<Event> ReadyEvent;

    TListeners Listeners;

public:
    TPromiseState(bool initialized = false)
        : ValueIsInitialized(initialized)
    { }

    bool IsSet() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock);
        return ValueIsInitialized;
    }

    void Set()
    {
        TListeners listeners;

        {
            TGuard<TSpinLock> guard(SpinLock);
            YASSERT(!ValueIsInitialized);
            ValueIsInitialized = true;

            auto* event = ~ReadyEvent;
            if (event) {
                event->Signal();
            }

            Listeners.swap(listeners);
        }

        FOREACH (auto& listener, listeners) {
            listener.Run();
        }
    }

    inline void Subscribe(const TListener& listener);
    inline void Subscribe(
        TDuration timeout,
        const TListener& onValue,
        const TClosure& onTimeout);
};

template <>
class TPromiseAwaiter<void>
    : public TIntrinsicRefCounted
{
public:
    TPromiseAwaiter(
        TPromiseState<void>* state,
        TDuration timeout,
        const TClosure& onValue,
        const TClosure& onTimeout)
        : OnValue(onValue)
        , OnTimeout(onTimeout)
        , CallbackAlreadyRan(0)
    {
        YASSERT(state);

        state->Subscribe(
            BIND(&TPromiseAwaiter::DoValue, MakeStrong(this)));
        TDelayedInvoker::Submit(
            BIND(&TPromiseAwaiter::DoTimeout, MakeStrong(this)), timeout);
    }

private:
    TClosure OnValue;
    TClosure OnTimeout;

    TAtomic CallbackAlreadyRan; 

    bool AtomicAcquire()
    {
        return AtomicCas(&CallbackAlreadyRan, 1, 0);
    }

    void DoValue()
    {
        if (AtomicAcquire()) {
            OnValue.Run();
        }
    }

    void DoTimeout()
    {
        if (AtomicAcquire()) {
            OnTimeout.Run();
        }
    }
};

void TPromiseState<void>::Subscribe(const TPromiseState<void>::TListener& listener)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (ValueIsInitialized) {
        guard.Release();
        listener.Run();
    } else {
        Listeners.push_back(listener);
    }
}

void TPromiseState<void>::Subscribe(
    TDuration timeout,
    const TListener& onValue,
    const TClosure& onTimeout)
{
    New< TPromiseAwaiter<void> >(this, timeout, onValue, onTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T>::TFuture()
    : Impl(NULL)
{ }

template <class T>
TFuture<T>::TFuture(const TFuture<T>& other)
    : Impl(other.Impl)
{ }

template <class T>
TFuture<T>::TFuture(TFuture<T>&& other)
    : Impl(MoveRV(other.Impl))
{ }

template <class T>
bool TFuture<T>::IsNull() const
{
    return Impl.Get() == NULL;
}

template <class T>
void TFuture<T>::Reset()
{
    Impl.Reset();
}

template <class T>
void TFuture<T>::Swap(TFuture& other)
{
    Impl.Swap(other.Impl);
}

template <class T>
TFuture<T>& TFuture<T>::operator=(const TFuture<T>& other)
{
    TFuture(other).Swap(*this);
    return *this;
}

template <class T>
TFuture<T>& TFuture<T>::operator=(TFuture<T>&& other)
{
    TFuture(MoveRV(other)).Swap(*this);
    return *this;
}

template <class T>
bool TFuture<T>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

template <class T>
const T& TFuture<T>::Get() const
{
    YASSERT(Impl);
    return Impl->Get();
}

template <class T>
TNullable<T> TFuture<T>::TryGet() const
{
    YASSERT(Impl);
    return Impl->TryGet();
}

template <class T>
void TFuture<T>::Subscribe(const TCallback<void(T)>& listener)
{
    YASSERT(Impl);
    return Impl->Subscribe(listener);
}

template <class T>
void TFuture<T>::Subscribe(
    TDuration timeout,
    const TCallback<void(T)>& onValue,
    const TClosure& onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, onValue, onTimeout);
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(const TCallback<R(T)>& mutator)
{
    TPromise<R> mutated;
    // TODO(sandello): Make cref here.
    Subscribe(BIND([mutated, mutator] (T value) mutable {
        mutated.Set(mutator.Run(value));
    }));
    return mutated;
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(const TCallback<TFuture<R>(T)>& mutator)
{
    TPromise<R> mutated;
    // TODO(sandello): Make cref here.
    //Subscribe(BIND([mutated, mutator] (T outerValue) mutable {
    //    mutator.Run(outerValue).Subscribe(BIND([mutated] (R innerValue) mutable {
    //        mutated.Set(MoveRV(innerValue));
    //    }));
    //}));
    return mutated;
}

template <class T>
TFuture<T>::TFuture(const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state)
    : Impl(state)
{ }

template <class T>
TFuture<T>::TFuture(TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state)
    : Impl(MoveRV(state))
{ }

template <class T>
bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl == rhs.Impl;
}

template <class T>
bool operator!=(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl != rhs.Impl;
}

////////////////////////////////////////////////////////////////////////////////

#if 0
bool TFuture<void>::IsSet() const
{
    return Impl->IsSet();
}

void TFuture<void>::Subscribe(const TListener& listener)
{
    Impl->Subscribe(listener);
}
#endif

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture< typename NMpl::TDecay<T>::TType > MakeFuture(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TFuture<U>(New< NYT::NDetail::TPromiseState<U> >(ForwardRV<T>(value)));
}

#if 0
TFuture<void> MakeFuture()
{
    return TFuture<void>(New< NYT::NDetail::TPromiseState<void> >(true));
}
#endif

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPromise<T>::TPromise()
    : Impl(New< NYT::NDetail::TPromiseState<T> >())
{ }

template <class T>
TPromise<T>::TPromise(const TPromise<T>& other)
    : Impl(other.Impl)
{ }

template <class T>
TPromise<T>::TPromise(TPromise<T>&& other)
    : Impl(MoveRV(other.Impl))
{ }

template <class T>
bool TPromise<T>::IsNull() const
{
    return Impl.Get() == NULL;
}

template <class T>
void TPromise<T>::Reset()
{
    Impl.Reset();
}

template <class T>
void TPromise<T>::Swap(TPromise& other)
{
    Impl.Swap(other.Impl);
}

template <class T>
TPromise<T>& TPromise<T>::operator=(const TPromise<T>& other)
{
    TPromise(other).Swap(*this);
    return *this;
}

template <class T>
TPromise<T>& TPromise<T>::operator=(TPromise<T>&& other)
{
    TPromise(MoveRV(other)).Swap(*this);
    return *this;
}

template <class T>
bool TPromise<T>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

template <class T>
void TPromise<T>::Set(const T& value)
{
    YASSERT(Impl);
    Impl->Set(value);
}

template <class T>
void TPromise<T>::Set(T&& value)
{
    YASSERT(Impl);
    Impl->Set(MoveRV(value));
}

template <class T>
TFuture<T> TPromise<T>::ToFuture() const
{
    return TFuture<T>(Impl);
}

// XXX(sandello): Kill this method.
template <class T>
TPromise<T>::operator TFuture<T>() const
{
    return TFuture<T>(Impl);
}

template <class T>
TPromise<T>::TPromise(const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state)
    : Impl(state)
{ }

template <class T>
TPromise<T>::TPromise(TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state)
    : Impl(MoveRV(state))
{ }

template <class T>
bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl == rhs.Impl;
}

template <class T>
bool operator!=(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl != rhs.Impl;
}

////////////////////////////////////////////////////////////////////////////////

#if 0
TPromise<void>::TPromise()
    : Impl(New< NYT::NDetail::TPromiseState<void> >())
{ }

void TPromise<void>::Set()
{
    Impl->Set();
}

TPromise<void>::operator TFuture<void>() const
{
    return TFuture<void>(Impl);
}
#endif

template <class T>
TPromise< typename NMpl::TDecay<T>::TType > MakePromise(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TPromise<U>(New< NYT::NDetail::TPromiseState<U> >(ForwardRV<T>(value)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

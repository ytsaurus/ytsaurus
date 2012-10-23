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
// #TPromiseState<>

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
        
        if (Value.HasValue()) {
            return Value.Get();
        }
        
        if (!ReadyEvent) {
            ReadyEvent.Reset(new Event());
        }
        
        guard.Release();
        ReadyEvent->Wait();
        
        YASSERT(Value.HasValue());       
        return Value.Get();
    }

    TNullable<T> TryGet() const
    {
        TGuard<TSpinLock> guard(SpinLock);
        return Value;
    }

    bool IsSet() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock);
        return Value.HasValue();
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
            YASSERT(!Value.HasValue());
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

    void Subscribe(const TListener& listener);
    void Subscribe(
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
inline void TPromiseState<T>::Subscribe(
    const typename TPromiseState<T>::TListener& listener)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (Value.HasValue()) {
        guard.Release();
        listener.Run(Value.Get());
    } else {
        Listeners.push_back(listener);
    }
}

template <class T>
inline void TPromiseState<T>::Subscribe(
    TDuration timeout,
    const TListener& onValue,
    const TClosure& onTimeout)
{
    New< TPromiseAwaiter<T> >(this, timeout, onValue, onTimeout);
}

////////////////////////////////////////////////////////////////////////////////
// #TPromiseState<void>

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

    void Get() const
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (ValueIsInitialized) {
            return;
        }

        if (!ReadyEvent) {
            ReadyEvent.Reset(new Event());
        }

        guard.Release();
        ReadyEvent->Wait();

        YASSERT(ValueIsInitialized);
    }

    void Subscribe(const TListener& listener);
    void Subscribe(
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

inline void TPromiseState<void>::Subscribe(
    const TPromiseState<void>::TListener& listener)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (ValueIsInitialized) {
        guard.Release();
        listener.Run();
    } else {
        Listeners.push_back(listener);
    }
}

inline void TPromiseState<void>::Subscribe(
    TDuration timeout,
    const TListener& onValue,
    const TClosure& onTimeout)
{
    New< TPromiseAwaiter<void> >(this, timeout, onValue, onTimeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////
// #TFuture<>

template <class T>
inline TFuture<T>::TFuture()
    : Impl(NULL)
{ }

template <class T>
inline TFuture<T>::TFuture(const TFuture<T>& other)
    : Impl(other.Impl)
{ }

template <class T>
inline TFuture<T>::TFuture(TFuture<T>&& other)
    : Impl(MoveRV(other.Impl))
{ }

template <class T>
inline TFuture<T>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TFuture::Impl : NULL;   
}

template <class T>
inline void TFuture<T>::Reset()
{
    Impl.Reset();
}

template <class T>
inline void TFuture<T>::Swap(TFuture& other)
{
    Impl.Swap(other.Impl);
}

template <class T>
inline TFuture<T>& TFuture<T>::operator=(const TFuture<T>& other)
{
    TFuture(other).Swap(*this);
    return *this;
}

template <class T>
inline TFuture<T>& TFuture<T>::operator=(TFuture<T>&& other)
{
    TFuture(MoveRV(other)).Swap(*this);
    return *this;
}

template <class T>
inline bool TFuture<T>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

template <class T>
inline const T& TFuture<T>::Get() const
{
    YASSERT(Impl);
    return Impl->Get();
}

template <class T>
inline TNullable<T> TFuture<T>::TryGet() const
{
    YASSERT(Impl);
    return Impl->TryGet();
}

template <class T>
inline void TFuture<T>::Subscribe(const TCallback<void(T)>& listener)
{
    YASSERT(Impl);
    return Impl->Subscribe(listener);
}

template <class T>
inline void TFuture<T>::Subscribe(
    TDuration timeout,
    const TCallback<void(T)>& onValue,
    const TClosure& onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, onValue, onTimeout);
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(const TCallback<void(T)>& mutator)
{
    auto mutated = NewPromise<void>();
    // TODO(sandello): Make cref here.
    Subscribe(BIND([mutated, mutator] (T value) mutable {
        mutator.Run(value);
        mutated.Set();
    }));
    return mutated;
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(const TCallback<TFuture<void>(T)>& mutator)
{
    auto mutated = NewPromise<void>();

    // TODO(sandello): Make cref here.
    auto inner = BIND([mutated] () mutable {
        mutated.Set();
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([mutator, inner] (T outerValue) mutable {
        mutator.Run(outerValue).Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(const TCallback<R(T)>& mutator)
{
    auto mutated = NewPromise<R>();
    // TODO(sandello): Make cref here.
    Subscribe(BIND([mutated, mutator] (T value) mutable {
        mutated.Set(mutator.Run(value));
    }));
    return mutated;
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(const TCallback<TFuture<R>(T)>& mutator)
{
    auto mutated = NewPromise<R>();

    // TODO(sandello): Make cref here.
    auto inner = BIND([mutated] (R innerValue) mutable {
        mutated.Set(MoveRV(innerValue));
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([mutator, inner] (T outerValue) mutable {
        mutator.Run(outerValue).Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

template <class T>
inline TFuture<T>::TFuture(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state)
    : Impl(state)
{ }

template <class T>
inline TFuture<T>::TFuture(
    TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state)
    : Impl(MoveRV(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TFuture<void>

inline TFuture<void>::TFuture()
    : Impl(NULL)
{ }

inline TFuture<void>::TFuture(const TFuture<void>& other)
    : Impl(other.Impl)
{ }

inline TFuture<void>::TFuture(TFuture<void>&& other)
    : Impl(MoveRV(other.Impl))
{ }

inline TFuture<void>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TFuture::Impl : NULL; 
}

inline void TFuture<void>::Reset()
{
    Impl.Reset();
}

inline void TFuture<void>::Swap(TFuture& other)
{
    Impl.Swap(other.Impl);
}

inline TFuture<void>& TFuture<void>::operator=(const TFuture<void>& other)
{
    TFuture(other).Swap(*this);
    return *this;
}

inline TFuture<void>& TFuture<void>::operator=(TFuture<void>&& other)
{
    TFuture(MoveRV(other)).Swap(*this);
    return *this;
}

inline bool TFuture<void>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

inline void TFuture<void>::Get() const
{
    YASSERT(Impl);
    Impl->Get();
}

inline void TFuture<void>::Subscribe(const TClosure& listener)
{
    YASSERT(Impl);
    return Impl->Subscribe(listener);
}

inline void TFuture<void>::Subscribe(
    TDuration timeout,
    const TClosure& onValue,
    const TClosure& onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, onValue, onTimeout);
}

inline TFuture<void> TFuture<void>::Apply(const TCallback<void()>& mutator)
{
    auto mutated = NewPromise<void>();
    Subscribe(BIND([mutated, mutator] () mutable {
        mutator.Run();
        mutated.Set();
    }));
    return mutated;
}

inline TFuture<void> TFuture<void>::Apply(const TCallback<TFuture<void>()>& mutator)
{
    auto mutated = NewPromise<void>();

    // TODO(sandello): Make cref here.
    auto inner = BIND([mutated] () mutable {
        mutated.Set();
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([mutator, inner] () mutable {
        mutator.Run().Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

template <class R>
inline TFuture<R> TFuture<void>::Apply(const TCallback<R()>& mutator)
{
    auto mutated = NewPromise<R>();
    // TODO(sandello): Make cref here.
    Subscribe(BIND([mutated, mutator] () mutable {
        mutated.Set(mutator.Run());
    }));
    return mutated;
}

template <class R>
inline TFuture<R> TFuture<void>::Apply(const TCallback<TFuture<R>()>& mutator)
{
    auto mutated = NewPromise<R>();

    // TODO(sandello): Make cref here.
    auto inner = BIND([mutated] (R innerValue) mutable {
        mutated.Set(MoveRV(innerValue));
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([mutator, inner] () mutable {
        mutator.Run().Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

inline TFuture<void>::TFuture(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<void> >& state)
    : Impl(state)
{ }

inline TFuture<void>::TFuture(
    TIntrusivePtr< NYT::NDetail::TPromiseState<void> >&& state)
    : Impl(MoveRV(state))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl == rhs.Impl;
}

template <class T>
inline bool operator!=(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl != rhs.Impl;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TFuture< typename NMpl::TDecay<T>::TType > MakeFuture(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TFuture<U>(New< NYT::NDetail::TPromiseState<U> >(ForwardRV<T>(value)));
}

inline TFuture<void> MakeFuture()
{
    return TFuture<void>(New< NYT::NDetail::TPromiseState<void> >(true));
}

////////////////////////////////////////////////////////////////////////////////
// #TPromise<>

template <class T>
inline TPromise<T>::TPromise()
    : Impl(NULL)
{ }

template <class T>
inline TPromise<T>::TPromise(const TPromise<T>& other)
    : Impl(other.Impl)
{ }

template <class T>
inline TPromise<T>::TPromise(TPromise<T>&& other)
    : Impl(MoveRV(other.Impl))
{ }

template <class T>
inline TPromise<T>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TPromise::Impl : NULL;   
}

template <class T>
inline void TPromise<T>::Reset()
{
    Impl.Reset();
}

template <class T>
inline void TPromise<T>::Swap(TPromise& other)
{
    Impl.Swap(other.Impl);
}

template <class T>
inline TPromise<T>& TPromise<T>::operator=(const TPromise<T>& other)
{
    TPromise(other).Swap(*this);
    return *this;
}

template <class T>
inline TPromise<T>& TPromise<T>::operator=(TPromise<T>&& other)
{
    TPromise(MoveRV(other)).Swap(*this);
    return *this;
}

template <class T>
inline bool TPromise<T>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

template <class T>
inline void TPromise<T>::Set(const T& value)
{
    YASSERT(Impl);
    Impl->Set(value);
}

template <class T>
inline void TPromise<T>::Set(T&& value)
{
    YASSERT(Impl);
    Impl->Set(MoveRV(value));
}

template <class T>
inline const T& TPromise<T>::Get() const
{
    YASSERT(Impl);
    return Impl->Get();
}

template <class T>
inline TNullable<T> TPromise<T>::TryGet() const
{
    YASSERT(Impl);
    return Impl->TryGet();
}

template <class T>
inline void TPromise<T>::Subscribe(const TCallback<void(T)>& listener)
{
    YASSERT(Impl);
    return Impl->Subscribe(listener);
}

template <class T>
inline void TPromise<T>::Subscribe(
    TDuration timeout,
    const TCallback<void(T)>& onValue,
    const TClosure& onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, onValue, onTimeout);
}

template <class T>
inline TFuture<T> TPromise<T>::ToFuture() const
{
    return TFuture<T>(Impl);
}

// XXX(sandello): Kill this method.
template <class T>
inline TPromise<T>::operator TFuture<T>() const
{
    return TFuture<T>(Impl);
}

template <class T>
inline TPromise<T>::TPromise(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state)
    : Impl(state)
{ }

template <class T>
inline TPromise<T>::TPromise(
    TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state)
    : Impl(MoveRV(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TPromise<void>

inline TPromise<void>::TPromise()
    : Impl(NULL)
{ }

inline TPromise<void>::TPromise(const TPromise<void>& other)
    : Impl(other.Impl)
{ }

inline TPromise<void>::TPromise(TPromise<void>&& other)
    : Impl(MoveRV(other.Impl))
{ }

inline TPromise<void>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TPromise::Impl : NULL;   
}

inline bool TPromise<void>::IsNull() const
{
    return Impl.Get() == NULL;
}

inline void TPromise<void>::Reset()
{
    Impl.Reset();
}

inline void TPromise<void>::Swap(TPromise& other)
{
    Impl.Swap(other.Impl);
}

inline TPromise<void>& TPromise<void>::operator=(const TPromise<void>& other)
{
    TPromise(other).Swap(*this);
    return *this;
}

inline TPromise<void>& TPromise<void>::operator=(TPromise<void>&& other)
{
    TPromise(MoveRV(other)).Swap(*this);
    return *this;
}

inline bool TPromise<void>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

inline void TPromise<void>::Set()
{
    YASSERT(Impl);
    Impl->Set();
}

inline void TPromise<void>::Get() const
{
    YASSERT(Impl);
    Impl->Get();
}

inline void TPromise<void>::Subscribe(const TClosure& listener)
{
    YASSERT(Impl);
    return Impl->Subscribe(listener);
}

inline void TPromise<void>::Subscribe(
    TDuration timeout,
    const TClosure& onValue,
    const TClosure& onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, onValue, onTimeout);
}

inline TFuture<void> TPromise<void>::ToFuture() const
{
    return TFuture<void>(Impl);
}

// XXX(sandello): Kill this method.
inline TPromise<void>::operator TFuture<void>() const
{
    return TFuture<void>(Impl);
}

inline TPromise<void>::TPromise(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<void> >& state)
    : Impl(state)
{ }

inline TPromise<void>::TPromise(
    TIntrusivePtr< NYT::NDetail::TPromiseState<void> >&& state)
    : Impl(MoveRV(state))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl == rhs.Impl;
}

template <class T>
inline bool operator!=(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl != rhs.Impl;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TPromise< typename NMpl::TDecay<T>::TType > MakePromise(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TPromise<U>(New< NYT::NDetail::TPromiseState<U> >(ForwardRV<T>(value)));
}

inline TPromise<void> MakePromise()
{
    return TPromise<void>(New< NYT::NDetail::TPromiseState<void> >(true));
}

template <class T>
inline TPromise<T> NewPromise()
{
    return TPromise<T>(New< NYT::NDetail::TPromiseState<T> >());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include "bind.h"
#include "callback.h"

#include <core/concurrency/delayed_executor.h>

#include <core/misc/small_vector.h>

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
    typedef TCallback<void(T)> TResultHandler;
    typedef TSmallVector<TResultHandler, 8> TResultHandlers;

    typedef TClosure TCancelHandler;
    typedef TSmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    TNullable<T> Value_;
    mutable TSpinLock SpinLock_;
    mutable std::unique_ptr<Event> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    bool Canceled_;
    TCancelHandlers CancelHandlers_;

public:
    TPromiseState()
        : Canceled_(false)
    { }

    template <class U>
    explicit TPromiseState(U&& value)
        : Value_(std::forward<U>(value))
    {
        static_assert(
            NMpl::TIsConvertible<U, T>::Value,
            "U have to be convertible to T");
    }

    const T& Get() const
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Value_) {
            return Value_.Get();
        }

        if (!ReadyEvent_) {
            ReadyEvent_.reset(new Event());
        }

        guard.Release();
        ReadyEvent_->Wait();

        return Value_.Get();
    }

    TNullable<T> TryGet() const
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return Value_;
    }

    bool IsSet() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock_);
        return Value_;
    }

    bool IsCanceled() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock_);
        return Canceled_;
    }

    template <class U>
    void Set(U&& value)
    {
        static_assert(
            NMpl::TIsConvertible<U, T>::Value,
            "U have to be convertible to T");

        TResultHandlers handlers;
        Event* event;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Canceled_)
                return;

            YASSERT(!Value_);
            Value_.Assign(std::forward<U>(value));

            event = ~ReadyEvent_;

            ResultHandlers_.swap(handlers);
            CancelHandlers_.clear();
        }

        if (event) {
            event->Signal();
        }

        FOREACH (const auto& handler, handlers) {
            handler.Run(*Value_);
        }
    }

    void Subscribe(TResultHandler onResult)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Value_) {
            guard.Release();
            onResult.Run(Value_.Get());
        } else if (!Canceled_) {
            ResultHandlers_.push_back(std::move(onResult));
        }
    }

    void Subscribe(
        TDuration timeout,
        TResultHandler onResult,
        TClosure onTimeout);

    void OnCanceled(TCancelHandler onCancel)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Value_)
            return;

        if (Canceled_) {
            guard.Release();
            onCancel.Run();
            return;
        }

        CancelHandlers_.push_back(std::move(onCancel));
    }

    void Cancel()
    {
        TCancelHandlers handlers;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Value_ || Canceled_)
                return;

            ResultHandlers_.clear();

            Canceled_ = true;
            CancelHandlers_.swap(handlers);
        }

        FOREACH (auto& handler, handlers) {
            handler.Run();
        }
    }

};

template <class T>
class TPromiseAwaiter
    : public TIntrinsicRefCounted
{
public:
    TPromiseAwaiter(
        TPromiseState<T>* state,
        TDuration timeout,
        TCallback<void(T)> onResult,
        TClosure onTimeout)
        : OnResult_(std::move(onResult))
        , OnTimeout_(std::move(onTimeout))
        , CallbackAlreadyRan_(false)
    {
        YASSERT(state);

        state->Subscribe(
            BIND(&TPromiseAwaiter::OnResult, MakeStrong(this)));
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TPromiseAwaiter::OnTimeout, MakeStrong(this)), timeout);
    }

private:
    TCallback<void(T)> OnResult_;
    TClosure OnTimeout_;

    TAtomic CallbackAlreadyRan_;

    bool AtomicAcquire()
    {
        return AtomicCas(&CallbackAlreadyRan_, true, false);
    }

    void OnResult(T value)
    {
        if (AtomicAcquire()) {
            OnResult_.Run(std::move(value));
        }
    }

    void OnTimeout()
    {
        if (AtomicAcquire()) {
            OnTimeout_.Run();
        }
    }
};

template <class T>
inline void TPromiseState<T>::Subscribe(
    TDuration timeout,
    TResultHandler onResult,
    TClosure onTimeout)
{
    New<TPromiseAwaiter<T>>(
        this,
        timeout,
        std::move(onResult),
        std::move(onTimeout));
}

////////////////////////////////////////////////////////////////////////////////
// #TPromiseState<void>

template <>
class TPromiseState<void>
    : public TIntrinsicRefCounted
{
public:
    typedef TCallback<void()> TResultHandler;
    typedef std::vector<TResultHandler> TResultHandlers;

    typedef TClosure TCancelHandler;
    typedef TSmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    bool HasValue_;
    mutable TSpinLock SpinLock_;
    mutable std::unique_ptr<Event> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    bool Canceled_;
    TCancelHandlers CancelHandlers_;

public:
    TPromiseState(bool hasValue = false)
        : HasValue_(hasValue)
        , Canceled_(false)
    { }

    bool IsSet() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock_);
        return HasValue_;
    }

    bool IsCanceled() const
    {
        // Guard is typically redundant.
        TGuard<TSpinLock> guard(SpinLock_);
        return Canceled_;
    }

    void Set()
    {
        TResultHandlers handlers;

        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Canceled_)
                return;

            YASSERT(!HasValue_);
            HasValue_= true;

            auto* event = ~ReadyEvent_;
            if (event) {
                event->Signal();
            }

            ResultHandlers_.swap(handlers);
            CancelHandlers_.clear();
        }

        FOREACH (auto& handler, handlers) {
            handler.Run();
        }
    }

    void Get() const
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (HasValue_)
            return;

        if (!ReadyEvent_) {
            ReadyEvent_.reset(new Event());
        }

        guard.Release();
        ReadyEvent_->Wait();

        YASSERT(HasValue_);
    }

    void Subscribe(TResultHandler onResult)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (HasValue_) {
            guard.Release();
            onResult.Run();
        } else if (!Canceled_) {
            ResultHandlers_.push_back(std::move(onResult));
        }
    }

    void Subscribe(
        TDuration timeout,
        TResultHandler onResult,
        TClosure onTimeout);

    void OnCanceled(TCancelHandler onCancel)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (HasValue_ || Canceled_)
            return;

        CancelHandlers_.push_back(std::move(onCancel));
    }

    void Cancel()
    {
        TCancelHandlers handlers;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (HasValue_)
                return;

            ResultHandlers_.clear();

            Canceled_ = true;
            CancelHandlers_.swap(handlers);
        }

        FOREACH (auto& handler, handlers) {
            handler.Run();
        }
    }

};

template <>
class TPromiseAwaiter<void>
    : public TIntrinsicRefCounted
{
public:
    TPromiseAwaiter(
        TPromiseState<void>* state,
        TDuration timeout,
        TClosure onResult,
        TClosure onTimeout)
        : OnResult_(onResult)
        , OnTimeout_(onTimeout)
        , CallbackAlreadyRan_(0)
    {
        YASSERT(state);

        state->Subscribe(
            BIND(&TPromiseAwaiter::OnResult, MakeStrong(this)));
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TPromiseAwaiter::OnTimeout, MakeStrong(this)), timeout);
    }

private:
    TClosure OnResult_;
    TClosure OnTimeout_;

    TAtomic CallbackAlreadyRan_;

    bool AtomicAcquire()
    {
        return AtomicCas(&CallbackAlreadyRan_, true, false);
    }

    void OnResult()
    {
        if (AtomicAcquire()) {
            OnResult_.Run();
        }
    }

    void OnTimeout()
    {
        if (AtomicAcquire()) {
            OnTimeout_.Run();
        }
    }
};

inline void TPromiseState<void>::Subscribe(
    TDuration timeout,
    TResultHandler onResult,
    TClosure onTimeout)
{
    New<TPromiseAwaiter<void>>(
        this,
        timeout,
        std::move(onResult),
        std::move(onTimeout));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////
// #TFuture<>

template <class T>
inline TFuture<T>::TFuture()
{ }

template <class T>
inline TFuture<T>::TFuture(TNull)
{ }

template <class T>
inline TFuture<T>::TFuture(const TFuture<T>& other)
    : Impl(other.Impl)
{ }

template <class T>
inline TFuture<T>::TFuture(TFuture<T>&& other)
    : Impl(std::move(other.Impl))
{ }

template <class T>
inline TFuture<T>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TFuture::Impl : nullptr;
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
    TFuture(std::move(other)).Swap(*this);
    return *this;
}

template <class T>
inline bool TFuture<T>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

template <class T>
inline bool TFuture<T>::IsCanceled() const
{
    YASSERT(Impl);
    return Impl->IsCanceled();
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
inline void TFuture<T>::Subscribe(TCallback<void(T)> onResult)
{
    YASSERT(Impl);
    return Impl->Subscribe(std::move(onResult));
}

template <class T>
inline void TFuture<T>::Subscribe(
    TDuration timeout,
    TCallback<void(T)> onResult,
    TClosure onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

template <class T>
inline void TFuture<T>::Cancel()
{
    YASSERT(Impl);
    return Impl->Cancel();
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(TCallback<void(T)> mutator)
{
    auto mutated = NewPromise();
    // TODO(sandello): Make cref here.
    Subscribe(BIND([=] (T value) mutable {
        mutator.Run(value);
        mutated.Set();
    }));
    return mutated;
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(TCallback<TFuture<void>(T)> mutator)
{
    auto mutated = NewPromise();

    // TODO(sandello): Make cref here.
    auto inner = BIND([=] () mutable {
        mutated.Set();
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([=] (T outerValue) mutable {
        mutator.Run(outerValue).Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(TCallback<R(T)> mutator)
{
    auto mutated = NewPromise<R>();
    // TODO(sandello): Make cref here.
    Subscribe(BIND([=] (T value) mutable {
        mutated.Set(mutator.Run(value));
    }));
    return mutated;
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(T)> mutator)
{
    auto mutated = NewPromise<R>();

    // TODO(sandello): Make cref here.
    auto inner = BIND([=] (R innerValue) mutable {
        mutated.Set(std::move(innerValue));
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([=] (T outerValue) mutable {
        mutator.Run(outerValue).Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

template <class T>
TFuture<void> TFuture<T>::IgnoreResult()
{
    auto voidPromise = NewPromise();
    Subscribe(BIND([=] (T) mutable {
        voidPromise.Set();
    }));
    return voidPromise;
}

template <class T>
inline TFuture<T>::TFuture(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<T> >& state)
    : Impl(state)
{ }

template <class T>
inline TFuture<T>::TFuture(
    TIntrusivePtr< NYT::NDetail::TPromiseState<T> >&& state)
    : Impl(std::move(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TFuture<void>

inline TFuture<void>::TFuture()
    : Impl(nullptr)
{ }

inline TFuture<void>::TFuture(TNull)
    : Impl(nullptr)
{ }

inline TFuture<void>::TFuture(const TFuture<void>& other)
    : Impl(other.Impl)
{ }

inline TFuture<void>::TFuture(TFuture<void>&& other)
    : Impl(std::move(other.Impl))
{ }

inline TFuture<void>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TFuture::Impl : nullptr;
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
    TFuture(std::move(other)).Swap(*this);
    return *this;
}

inline bool TFuture<void>::IsSet() const
{
    YASSERT(Impl);
    return Impl->IsSet();
}

inline bool TFuture<void>::IsCanceled() const
{
    YASSERT(Impl);
    return Impl->IsCanceled();
}

inline void TFuture<void>::Get() const
{
    YASSERT(Impl);
    Impl->Get();
}

inline void TFuture<void>::Subscribe(TClosure onResult)
{
    YASSERT(Impl);
    return Impl->Subscribe(std::move(onResult));
}

inline void TFuture<void>::Subscribe(
    TDuration timeout,
    TClosure onResult,
    TClosure onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

inline void TFuture<void>::Cancel()
{
    YASSERT(Impl);
    return Impl->Cancel();
}

inline TFuture<void> TFuture<void>::Apply(TCallback<void()> mutator)
{
    auto mutated = NewPromise();
    Subscribe(BIND([=] () mutable {
        mutator.Run();
        mutated.Set();
    }));
    return mutated;
}

inline TFuture<void> TFuture<void>::Apply(TCallback<TFuture<void>()> mutator)
{
    auto mutated = NewPromise();

    // TODO(sandello): Make cref here.
    auto inner = BIND([=] () mutable {
        mutated.Set();
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([=] () mutable {
        mutator.Run().Subscribe(inner);
    });

    Subscribe(outer);
    return mutated;
}

template <class R>
inline TFuture<R> TFuture<void>::Apply(TCallback<R()> mutator)
{
    auto mutated = NewPromise<R>();
    // TODO(sandello): Make cref here.
    Subscribe(BIND([=] () mutable {
        mutated.Set(mutator.Run());
    }));
    return mutated;
}

template <class R>
inline TFuture<R> TFuture<void>::Apply(TCallback<TFuture<R>()> mutator)
{
    auto mutated = NewPromise<R>();

    // TODO(sandello): Make cref here.
    auto inner = BIND([=] (R innerValue) mutable {
        mutated.Set(std::move(innerValue));
    });
    // TODO(sandello): Make cref here.
    auto outer = BIND([=] () mutable {
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
    : Impl(std::move(state))
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
    return TFuture<U>(New< NYT::NDetail::TPromiseState<U> >(std::forward<T>(value)));
}

inline TFuture<void> MakeFuture()
{
    return TFuture<void>(New< NYT::NDetail::TPromiseState<void> >(true));
}

////////////////////////////////////////////////////////////////////////////////
// #TPromise<>

template <class T>
inline TPromise<T>::TPromise()
    : Impl(nullptr)
{ }

template <class T>
inline TPromise<T>::TPromise(TNull)
    : Impl(nullptr)
{ }

template <class T>
inline TPromise<T>::TPromise(const TPromise<T>& other)
    : Impl(other.Impl)
{ }

template <class T>
inline TPromise<T>::TPromise(TPromise<T>&& other)
    : Impl(std::move(other.Impl))
{ }

template <class T>
inline TPromise<T>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TPromise::Impl : nullptr;
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
    TPromise(std::move(other)).Swap(*this);
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
    Impl->Set(std::move(value));
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
inline void TPromise<T>::Subscribe(TCallback<void(T)> onResult)
{
    YASSERT(Impl);
    Impl->Subscribe(std::move(onResult));
}

template <class T>
inline void TPromise<T>::Subscribe(
    TDuration timeout,
    TCallback<void(T)> onResult,
    TClosure onTimeout)
{
    YASSERT(Impl);
    Impl->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

template <class T>
inline void TPromise<T>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl);
    Impl->OnCanceled(onCancel);
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
    : Impl(std::move(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TPromise<void>

inline TPromise<void>::TPromise()
    : Impl(nullptr)
{ }

inline TPromise<void>::TPromise(TNull)
    : Impl(nullptr)
{ }

inline TPromise<void>::TPromise(const TPromise<void>& other)
    : Impl(other.Impl)
{ }

inline TPromise<void>::TPromise(TPromise<void>&& other)
    : Impl(std::move(other.Impl))
{ }

inline TPromise<void>::operator TUnspecifiedBoolType() const
{
    return Impl ? &TPromise::Impl : nullptr;
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
    TPromise(std::move(other)).Swap(*this);
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

inline void TPromise<void>::Subscribe(TClosure onResult)
{
    YASSERT(Impl);
    return Impl->Subscribe(std::move(onResult));
}

inline void TPromise<void>::Subscribe(
    TDuration timeout,
    TClosure onResult,
    TClosure onTimeout)
{
    YASSERT(Impl);
    return Impl->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

inline void TPromise<void>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl);
    Impl->OnCanceled(onCancel);
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
    : Impl(std::move(state))
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
    return TPromise<U>(New< NYT::NDetail::TPromiseState<U> >(std::forward<T>(value)));
}

template <class T>
inline TPromise<T> NewPromise()
{
    return TPromise<T>(New< NYT::NDetail::TPromiseState<T> >());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFutureCancelationGuard<T>::TFutureCancelationGuard(TFuture<T> future)
    : Future(std::move(future))
{ }

template <class T>
TFutureCancelationGuard<T>::~TFutureCancelationGuard()
{
    if (Future) {
        Future.Cancel();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

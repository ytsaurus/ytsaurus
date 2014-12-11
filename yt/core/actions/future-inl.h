#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include "bind.h"
#include "callback.h"
#include "invoker.h"
#include "future.h"
#include "invoker_util.h"

#include <core/concurrency/delayed_executor.h>

#include <core/misc/small_vector.h>
#include <core/misc/error.h>

#include <util/system/event.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Implemented in concurrency library.
namespace NConcurrency {

namespace NDetail {
TClosure GetCurrentFiberCanceler();
} // namespace NDetail

struct IScheduler;
IScheduler* GetCurrentScheduler();
IScheduler* TryGetCurrentScheduler();

} // namespace NConcurrency

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////
// #TPromiseState<>

template <class T>
class TPromiseState
    : public TIntrinsicRefCounted
{
public:
    typedef TCallback<void(const T&)> TResultHandler;
    typedef SmallVector<TResultHandler, 8> TResultHandlers;

    typedef TClosure TCancelHandler;
    typedef SmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    mutable TSpinLock SpinLock_;
    std::atomic<bool> Canceled_;
    std::atomic<bool> Set_;
    TNullable<T> Value_;
    mutable std::unique_ptr<Event> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    TCancelHandlers CancelHandlers_;


    template <class U, bool MustSet>
    bool DoSet(U&& value)
    {
        static_assert(
            NMpl::TIsConvertible<U, T>::Value,
            "U have to be convertible to T");

        // Calling subscribers may release the last reference to this.
        auto this_ = MakeStrong(this);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Canceled_) {
                return false;
            }
            if (MustSet) {
                YCHECK(!Set_);
            } else {
                if (Set_) {
                    return false;
                }
            }
            Value_.Assign(std::forward<U>(value));
            Set_ = true;
        }

        if (ReadyEvent_) {
            ReadyEvent_->Signal();
        }

        for (const auto& handler : ResultHandlers_) {
            handler.Run(*Value_);
        }

        ResultHandlers_.clear();
        CancelHandlers_.clear();

        return true;
    }

public:
    TPromiseState()
    {
        // TODO(babenko): VS compat
        Set_ = false;
        Canceled_ = false;
    }

    template <class U>
    explicit TPromiseState(U&& value)
        : Value_(std::forward<U>(value))
    {
        // TODO(babenko): VS compat
        Set_ = true;
        Canceled_ = false;
        static_assert(
            NMpl::TIsConvertible<U, T>::Value,
            "U have to be convertible to T");
    }

    ~TPromiseState()
    {
        DoCancel();
    }

    const T& Get() const
    {
        // Fast path.
        if (Set_) {
            return *Value_;
        }

        // Slow path.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Set_) {
                return *Value_;
            }
            if (!ReadyEvent_) {
                ReadyEvent_.reset(new Event());
            }
        }

        ReadyEvent_->Wait();

        return *Value_;
    }

    TNullable<T> TryGet() const
    {
        return Set_ ? Value_ : Null;
    }

    bool IsSet() const
    {
        return Set_;
    }

    bool IsCanceled() const
    {
        return Canceled_;
    }

    template <class U>
    void Set(U&& value)
    {
        DoSet<U, true>(std::forward<U>(value));
    }

    template <class U>
    bool TrySet(U&& value)
    {
        return DoSet<U, false>(std::forward<U>(value));
    }

    void Subscribe(TResultHandler handler)
    {
        // Fast path.
        if (Set_) {
            handler.Run(*Value_);
            return;
        }

        // Slow path.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Set_) {
                guard.Release();
                handler.Run(*Value_);
            } else if (!Canceled_) {
                ResultHandlers_.push_back(std::move(handler));
            }
        }
    }

    void OnCanceled(TCancelHandler handler)
    {
        // Fast path.
        if (Canceled_) {
            handler.Run();
            return;
        }

        // Slow path.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Canceled_) {
                guard.Release();
                handler.Run();
            } else if (!Set_) {
                CancelHandlers_.push_back(std::move(handler));
            }
        }
    }

    bool Cancel()
    {
        // Calling subscribers may release the last reference to this.
        auto this_ = MakeStrong(this);
        return DoCancel();
    }

private:
    bool DoCancel()
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Set_ || Canceled_) {
                return false;
            }
            Canceled_ = true;
        }

        for (auto& handler : CancelHandlers_) {
            handler.Run();
        }

        ResultHandlers_.clear();
        CancelHandlers_.clear();

        return true;
    }

};

template <class T>
struct TPromiseSetter
{
    static void Do(TPromise<T> promise, T value)
    {
        promise.Set(std::move(value));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void RegisterFiberCancelation(TPromise<T>& promise)
{
    if (NConcurrency::TryGetCurrentScheduler()) {
        auto invoker = GetCurrentInvoker();
        auto canceler = NConcurrency::NDetail::GetCurrentFiberCanceler();
        promise.OnCanceled(BIND(
            IgnoreResult(&IInvoker::Invoke),
            std::move(invoker),
            std::move(canceler)));
    }
}

template <class U, class... TArgs>
struct TAsyncViaHelperBase
{
    typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
    typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
    typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

    typedef TCallback<U(TArgs...)> TSourceCallback;
    typedef TCallback<typename NYT::NDetail::TFutureHelper<U>::TFutureType(TArgs...)> TTargetCallback;


    static void Canceler(FR future)
    {
        future.Cancel();
    }
};

template <bool WrappedInFuture, class TSignature>
struct TAsyncViaHelper;

template <bool W, class U, class... TArgs>
struct TAsyncViaHelper<W, U(TArgs...)>
    : public TAsyncViaHelperBase<U, TArgs...>
{
    typedef TAsyncViaHelperBase<U, TArgs...> TBase;

    typedef typename TBase::R R;
    typedef typename TBase::FR FR;
    typedef typename TBase::PR PR;

    typedef typename TBase::TSourceCallback TSourceCallback;
    typedef typename TBase::TTargetCallback TTargetCallback;


    static void Inner(const TSourceCallback& this_, PR promise, TArgs... args)
    {
        RegisterFiberCancelation(promise);
        promise.Set(this_.Run(std::forward<TArgs>(args)...));
    }

    static FR Outer(const TSourceCallback& this_, IInvokerPtr invoker, TArgs... args)
    {
        auto promise = NewPromise<R>();
        auto future = promise.ToFuture();
        auto canceler = BIND(&TBase::Canceler, future);
        auto inner = BIND(&Inner, this_, promise, std::forward<TArgs>(args)...);
        GuardedInvoke(std::move(invoker), std::move(inner), std::move(canceler));
        return future;
    }

    static TTargetCallback Do(const TSourceCallback& this_, IInvokerPtr invoker)
    {
        return BIND(&Outer, this_, std::move(invoker));
    }
};

template <bool W, class... TArgs>
struct TAsyncViaHelper<W, void(TArgs...)>
    : public TAsyncViaHelperBase<void, TArgs...>
{
    typedef TAsyncViaHelperBase<void, TArgs...> TBase;

    typedef typename TBase::R R;
    typedef typename TBase::FR FR;
    typedef typename TBase::PR PR;

    typedef typename TBase::TSourceCallback TSourceCallback;
    typedef typename TBase::TTargetCallback TTargetCallback;
    

    static void Inner(const TSourceCallback& this_, PR promise, TArgs... args)
    {
        RegisterFiberCancelation(promise);
        this_.Run(std::forward<TArgs>(args)...);
        promise.Set();
    }

    static FR Outer(const TSourceCallback& this_, IInvokerPtr invoker, TArgs... args)
    {
        auto promise = NewPromise<R>();
        auto future = promise.ToFuture();
        auto canceler = BIND(&TBase::Canceler, future);
        auto inner = BIND(&Inner, this_, promise, std::forward<TArgs>(args)...);
        GuardedInvoke(std::move(invoker), std::move(inner), std::move(canceler));
        return future;
    }

    static TTargetCallback Do(const TSourceCallback& this_, IInvokerPtr invoker)
    {
        return BIND(&Outer, this_, std::move(invoker));
    }
};

template <class U, class... TArgs>
struct TAsyncViaHelper<true, U(TArgs...)>
    : public TAsyncViaHelperBase<U, TArgs...>
{
    typedef TAsyncViaHelperBase<U, TArgs...> TBase;
    typedef typename TBase::R R;
    typedef typename TBase::FR FR;
    typedef typename TBase::PR PR;
    typedef typename TBase::TSourceCallback TSourceCallback;
    typedef typename TBase::TTargetCallback TTargetCallback;

    static void Inner(const TSourceCallback& this_, PR promise, TArgs... args)
    {
        RegisterFiberCancelation(promise);
        this_
            .Run(std::forward<TArgs>(args)...)
            .Subscribe(BIND(&TPromiseSetter<R>::Do, promise));
    }

    static FR Outer(const TSourceCallback& this_, IInvokerPtr invoker, TArgs... args)
    {
        auto promise = NewPromise<R>();
        auto future = promise.ToFuture();
        auto canceler = BIND(&TBase::Canceler, future);
        auto inner = BIND(&Inner, this_, promise, std::forward<TArgs>(args)...);
        GuardedInvoke(std::move(invoker), std::move(inner), std::move(canceler));
        return future;
    }

    static TTargetCallback Do(const TSourceCallback& this_, IInvokerPtr invoker)
    {
        return BIND(&Outer, this_, std::move(invoker));
    }
};

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
inline TFuture<T>::operator bool() const
{
    return Impl_ != nullptr;
}

template <class T>
inline void TFuture<T>::Reset()
{
    Impl_.Reset();
}

template <class T>
inline void TFuture<T>::Swap(TFuture& other)
{
    Impl_.Swap(other.Impl_);
}

template <class T>
inline bool TFuture<T>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
inline bool TFuture<T>::IsCanceled() const
{
    YASSERT(Impl_);
    return Impl_->IsCanceled();
}

template <class T>
inline const T& TFuture<T>::Get() const
{
    YASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
inline TNullable<T> TFuture<T>::TryGet() const
{
    YASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
inline void TFuture<T>::Subscribe(TCallback<void(const T&)> handler)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(handler));
}

template <class T>
inline void TFuture<T>::OnCanceled(TClosure handler)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(handler));
}

template <class T>
inline bool TFuture<T>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(TCallback<void(const T&)> mutator)
{
    auto mutated = NewPromise();

    Subscribe(BIND([=] (const T& value) mutable {
        mutator.Run(value);
        mutated.Set();
    }));

    auto this_ = *this;
    mutated.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return mutated;
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(TCallback<void(T)> mutator)
{
    return Apply(TCallback<void(const T&)>(mutator));
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(TCallback<TFuture<void>(const T&)> mutator)
{
    auto mutated = NewPromise();

    auto inner = BIND([=] () mutable {
        mutated.Set();
    });
    auto outer = BIND([=] (const T& outerValue) mutable {
        mutator.Run(outerValue).Subscribe(inner);
    });
    Subscribe(outer);

    auto this_ = *this;
    mutated.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return mutated;
}

template <class T>
inline TFuture<void> TFuture<T>::Apply(TCallback<TFuture<void>(T)> mutator)
{
    return Apply(TCallback<TFuture<void>(const T&)>(mutator));
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(TCallback<R(const T&)> mutator)
{
    auto mutated = NewPromise<R>();

    Subscribe(BIND([=] (const T& value) mutable {
        mutated.Set(mutator.Run(value));
    }));

    auto this_ = *this;
    mutated.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return mutated;
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(TCallback<R(T)> mutator)
{
    return Apply(TCallback<R(const T&)>(mutator));
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(const T&)> mutator)
{
    auto mutated = NewPromise<R>();

    auto inner = BIND([=] (const R& innerValue) mutable {
        mutated.Set(innerValue);
    });
    auto outer = BIND([=] (const T& outerValue) mutable {
        mutator.Run(outerValue).Subscribe(inner);
    });
    Subscribe(outer);

    auto this_ = *this;
    mutated.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return mutated;
}

template <class T>
template <class R>
inline TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(T)> mutator)
{
    return Apply(TCallback<TFuture<R>(const T&)>(mutator));
}

template <class T>
TFuture<void> TFuture<T>::IgnoreResult()
{
    auto promise = NewPromise<void>();

    Subscribe(BIND([=] (const T&) mutable {
        promise.Set();
    }));

    auto this_ = *this;
    promise.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return promise;
}

template <class T>
TFuture<void> TFuture<T>::Finally()
{
    auto promise = NewPromise<void>();
    Subscribe(BIND([=] (const T&) mutable { promise.Set(); }));
    OnCanceled(BIND([=] () mutable { promise.Set(); }));
    return promise;
}

template <class T>
TFuture<typename TErrorTraits<T>::TWrapped> TFuture<T>::WithTimeout(TDuration timeout)
{
    auto promise = NewPromise<typename TErrorTraits<T>::TWrapped>();

    Subscribe(BIND([=] (const T& value) mutable {
        promise.TrySet(value);
    }));

    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () mutable {
            promise.TrySet(TError(NYT::EErrorCode::Timeout, "Future has timed out"));
        }),
        timeout);

    auto this_ = *this;
    promise.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return promise;
}

template <class T>
inline TFuture<T>::TFuture(const TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl)
    : Impl_(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TFuture<void>

template <class R>
inline TFuture<R> TFuture<void>::Apply(TCallback<R()> mutator)
{
    auto mutated = NewPromise<R>();

    Subscribe(BIND([=] () mutable {
        mutated.Set(mutator.Run());
    }));

    auto this_ = *this;
    mutated.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return mutated;
}

template <class R>
inline TFuture<R> TFuture<void>::Apply(TCallback<TFuture<R>()> mutator)
{
    auto mutated = NewPromise<R>();

    auto inner = BIND([=] (const R& innerValue) mutable {
        mutated.Set(innerValue);
    });
    auto outer = BIND([=] () mutable {
        mutator.Run().Subscribe(inner);
    });
    Subscribe(outer);

    auto this_ = *this;
    mutated.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return mutated;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl_ == rhs.Impl_;
}

template <class T>
inline bool operator!=(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl_ != rhs.Impl_;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TFuture<typename NMpl::TDecay<T>::TType> MakeFuture(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TFuture<U>(New<NYT::NDetail::TPromiseState<U>>(std::forward<T>(value)));
}

////////////////////////////////////////////////////////////////////////////////
// #TPromise<>

template <class T>
inline TPromise<T>::TPromise()
    : Impl_(nullptr)
{ }

template <class T>
inline TPromise<T>::TPromise(TNull)
    : Impl_(nullptr)
{ }

template <class T>
inline TPromise<T>::operator bool() const
{
    return Impl_ != nullptr;
}

template <class T>
inline void TPromise<T>::Reset()
{
    Impl_.Reset();
}

template <class T>
inline void TPromise<T>::Swap(TPromise& other)
{
    Impl_.Swap(other.Impl_);
}

template <class T>
inline bool TPromise<T>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
inline void TPromise<T>::Set(const T& value)
{
    YASSERT(Impl_);
    Impl_->Set(value);
}

template <class T>
inline void TPromise<T>::Set(T&& value)
{
    YASSERT(Impl_);
    Impl_->Set(std::move(value));
}

template <class T>
template <class U>
inline void TPromise<T>::SetFrom(TFuture<U> another)
{
    auto this_ = *this;
    another.Subscribe(BIND([this_] (const U& value) mutable {
        this_.Set(value);
    }));
    OnCanceled(BIND([another] () mutable {
        another.Cancel();
    }));
}

template <class T>
inline bool TPromise<T>::TrySet(const T& value)
{
    YASSERT(Impl_);
    return Impl_->TrySet(value);
}

template <class T>
inline bool TPromise<T>::TrySet(T&& value)
{
    YASSERT(Impl_);
    return Impl_->TrySet(std::move(value));
}

template <class T>
template <class U>
inline void TPromise<T>::TrySetFrom(TFuture<U> another)
{
    auto this_ = *this;
    another.Subscribe(BIND([this_] (const U& value) mutable {
        this_.TrySet(value);
    }));
    OnCanceled(BIND([another] () mutable {
        another.Cancel();
    }));
}

template <class T>
inline const T& TPromise<T>::Get() const
{
    YASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
inline TNullable<T> TPromise<T>::TryGet() const
{
    YASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
inline void TPromise<T>::Subscribe(TCallback<void(const T&)> handler)
{
    YASSERT(Impl_);
    Impl_->Subscribe(std::move(handler));
}

template <class T>
inline void TPromise<T>::OnCanceled(TClosure handler)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(handler));
}

template <class T>
inline bool TPromise<T>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

template <class T>
inline TFuture<T> TPromise<T>::ToFuture() const
{
    return TFuture<T>(Impl_);
}

// XXX(sandello): Kill this method.
template <class T>
inline TPromise<T>::operator TFuture<T>() const
{
    return TFuture<T>(Impl_);
}

template <class T>
inline TPromise<T>::TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl)
    : Impl_(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class U>
void TPromise<void>::SetFrom(TFuture<U> another)
{
    SetFrom(another.IgnoreResult());
}

template <class U>
void TPromise<void>::TrySetFrom(TFuture<U> another)
{
    TrySetFrom(another.IgnoreResult());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl_ == rhs.Impl_;
}

template <class T>
inline bool operator!=(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl_ != rhs.Impl_;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TPromise<typename NMpl::TDecay<T>::TType> MakePromise(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TPromise<U>(New<NYT::NDetail::TPromiseState<U>>(std::forward<T>(value)));
}

template <class T>
inline TPromise<T> NewPromise()
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFutureCancelationGuard<T>::TFutureCancelationGuard(TFuture<T> future)
    : Future_(std::move(future))
{ }

template <class T>
TFutureCancelationGuard<T>::TFutureCancelationGuard(TFutureCancelationGuard&& other)
    : Future_(std::move(other.Future_))
{ }

template <class T>
TFutureCancelationGuard<T>::~TFutureCancelationGuard()
{
    Release();
}

template <class T>
TFutureCancelationGuard<T>& TFutureCancelationGuard<T>::operator=(TFutureCancelationGuard<T>&& other)
{
    if (this != &other) {
        Future_ = std::move(other.Future_);
    }
    return *this;
}

template <class T>
void swap(TFutureCancelationGuard<T>& lhs, TFutureCancelationGuard<T>& rhs)
{
    std::swap(lhs.Future_, rhs.Future_);
}

template <class T>
void TFutureCancelationGuard<T>::Release()
{
    if (Future_) {
        Future_.Cancel();
    }
}

template <class T>
TFutureCancelationGuard<T>::operator bool() const
{
    return static_cast<bool>(Future_);
}

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
TCallback<R(TArgs...)>
TCallback<R(TArgs...)>::Via(IInvokerPtr invoker)
{
    static_assert(
        NMpl::TIsVoid<R>::Value,
        "Via() can only be used with void return type.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (TArgs... args) {
        invoker->Invoke(BIND(this_, std::forward<TArgs>(args)...));
    });
}

template <class U, class... TArgs>
TCallback<typename NYT::NDetail::TFutureHelper<U>::TFutureType(TArgs...)>
TCallback<U(TArgs...)>::AsyncVia(IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(TArgs...)
    >::Do(*this, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

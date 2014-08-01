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

#include <util/system/event.h>

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
    typedef TCallback<void(T)> TResultHandler;
    typedef SmallVector<TResultHandler, 8> TResultHandlers;

    typedef TClosure TCancelHandler;
    typedef SmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    TNullable<T> Value_;
    mutable TSpinLock SpinLock_;
    mutable std::unique_ptr<Event> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    bool Canceled_;
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
                YCHECK(!Value_);
            } else {
                if (Value_) {
                    return false;
                }
            }

            Value_.Assign(std::forward<U>(value));
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
        : Canceled_(false)
    { }

    template <class U>
    explicit TPromiseState(U&& value)
        : Value_(std::forward<U>(value))
        , Canceled_(false)
    {
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
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Value_) {
                return Value_.Get();
            }

            if (!ReadyEvent_) {
                ReadyEvent_.reset(new Event());
            }
        }

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
        return Value_.HasValue();
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
        DoSet<U, true>(std::forward<U>(value));
    }

    template <class U>
    bool TrySet(U&& value)
    {
        return DoSet<U, false>(std::forward<U>(value));
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

        if (Canceled_) {
            guard.Release();
            onCancel.Run();
        } else if (!Value_) {
            CancelHandlers_.push_back(std::move(onCancel));
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
            if (Value_ || Canceled_) {
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
    typedef SmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    bool HasValue_;
    mutable TSpinLock SpinLock_;
    mutable std::unique_ptr<Event> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    bool Canceled_;
    TCancelHandlers CancelHandlers_;

    template <bool MustSet>
    bool DoSet()
    {
        // Calling subscribers may release the last reference to this.
        auto this_ = MakeStrong(this);

        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Canceled_) {
                return false;
            }

            if (MustSet) {
                YCHECK(!HasValue_);
            } else {
                if (HasValue_) {
                    return false;
                }
            }

            HasValue_ = true;
        }

        if (ReadyEvent_) {
            ReadyEvent_->Signal();
        }

        for (auto& handler : ResultHandlers_) {
            handler.Run();
        }

        ResultHandlers_.clear();
        CancelHandlers_.clear();

        return true;
    }

public:
    TPromiseState(bool hasValue = false)
        : HasValue_(hasValue)
        , Canceled_(false)
    { }

    ~TPromiseState()
    {
        DoCancel();
    }

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
        DoSet<true>();
    }

    bool TrySet()
    {
        return DoSet<false>();
    }

    void Get() const
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (HasValue_)
                return;

            if (!ReadyEvent_) {
                ReadyEvent_.reset(new Event());
            }
        }

        ReadyEvent_->Wait();
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

        if (HasValue_)
            return;

        if (Canceled_) {

        }

        CancelHandlers_.push_back(std::move(onCancel));
    }

    bool Cancel()
    {
        // Calling subscribers may release the last reference to this.
        auto this_ =  MakeStrong(this);
        return DoCancel();
    }

private:
    bool DoCancel()
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (HasValue_) {
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

template <class T>
struct TPromiseSetter
{
    static void Do(TPromise<T> promise, T value)
    {
        promise.Set(std::move(value));
    }
};

template <>
struct TPromiseSetter<void>
{
    static void Do(TPromise<void> promise)
    {
        promise.Set();
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

template <bool WrappedInFuture, class TSignature>
struct TAsyncViaHelper;

template <bool W, class U, class... TArgs>
struct TAsyncViaHelper<W, U(TArgs...)>
{
    typedef TCallback<U(TArgs...)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (TArgs...)> TTargetCallback;

    static TTargetCallback Do(const TSourceCallback& this_, IInvokerPtr invoker)
    {
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, TArgs... args) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(std::forward<TArgs>(args)...));
        });

        auto canceler = BIND([] (FR future) {
            future.Cancel();
        });

        return BIND([=] (TArgs... args) -> FR {
            auto promise = NewPromise<R>();
            auto future = promise.ToFuture();
            GuardedInvoke(
                invoker,
                BIND(inner, promise, std::forward<TArgs>(args)...),
                BIND(canceler, future));
            return future;
        });
    }
};

template <bool W, class... TArgs>
struct TAsyncViaHelper<W, void(TArgs...)>
{
    typedef TCallback<void(TArgs...)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (TArgs...)> TTargetCallback;

    static TTargetCallback Do(const TSourceCallback& this_, IInvokerPtr invoker)
    {
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, TArgs... args) {
            RegisterFiberCancelation(promise);
            this_.Run(std::forward<TArgs>(args)...);
            promise.Set();
        });

        auto canceler = BIND([] (FR future) {
            future.Cancel();
        });

        return BIND([=] (TArgs... args) -> FR {
            auto promise = NewPromise<R>();
            auto future = promise.ToFuture();
            GuardedInvoke(
                invoker,
                BIND(inner, promise, std::forward<TArgs>(args)... ),
                BIND(canceler, future));
            return future;
        });
    }
};

template <class U, class... TArgs>
struct TAsyncViaHelper<true, U(TArgs...)>
{
    typedef TCallback<U(TArgs...)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (TArgs...)> TTargetCallback;

    static TTargetCallback Do(const TSourceCallback& this_, IInvokerPtr invoker)
    {
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, TArgs... args) {
            RegisterFiberCancelation(promise);
            this_
                .Run(std::forward<TArgs>(args)...)
                .Subscribe(BIND(&TPromiseSetter<R>::Do, promise));
        });

        auto canceler = BIND([] (FR future) {
            future.Cancel();
        });

        return BIND([=] (TArgs... args) -> FR {
            auto promise = NewPromise<R>();
            auto future = promise.ToFuture();
            GuardedInvoke(
                invoker,
                BIND(inner, promise, std::forward<TArgs>(args)...),
                BIND(canceler, future));
            return future;
        });
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
inline void TFuture<T>::Subscribe(TCallback<void(T)> onResult)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(onResult));
}

template <class T>
inline void TFuture<T>::Subscribe(
    TDuration timeout,
    TCallback<void(T)> onResult,
    TClosure onTimeout)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

template <class T>
inline void TFuture<T>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(onCancel));
}

template <class T>
inline bool TFuture<T>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
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

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
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

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
    }));

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

    OnCanceled(BIND([=] () mutable {
       mutated.Cancel();
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

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
    }));

    return mutated;
}

template <class T>
TFuture<void> TFuture<T>::IgnoreResult()
{
    auto promise = NewPromise();
    Subscribe(BIND([=] (T) mutable {
        promise.Set();
    }));
    OnCanceled(BIND([=] () mutable {
        promise.Cancel();
    }));
    return promise;
}

template <class T>
TFuture<void> TFuture<T>::Finally()
{
    auto promise = NewPromise();
    Subscribe(BIND([=] (T) mutable { promise.Set(); }));
    OnCanceled(BIND([=] () mutable { promise.Set(); }));
    return promise;
}

template <class T>
inline TFuture<T>::TFuture(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<T>>& state)
    : Impl_(state)
{ }

template <class T>
inline TFuture<T>::TFuture(
    TIntrusivePtr< NYT::NDetail::TPromiseState<T>>&& state)
    : Impl_(std::move(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TFuture<void>

inline TFuture<void>::TFuture()
    : Impl_(nullptr)
{ }

inline TFuture<void>::TFuture(TNull)
    : Impl_(nullptr)
{ }

inline TFuture<void>::operator bool() const
{
    return Impl_ != nullptr;
}

inline void TFuture<void>::Reset()
{
    Impl_.Reset();
}

inline void TFuture<void>::Swap(TFuture& other)
{
    Impl_.Swap(other.Impl_);
}

inline bool TFuture<void>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

inline bool TFuture<void>::IsCanceled() const
{
    YASSERT(Impl_);
    return Impl_->IsCanceled();
}

inline void TFuture<void>::Get() const
{
    YASSERT(Impl_);
    Impl_->Get();
}

inline void TFuture<void>::Subscribe(TClosure onResult)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(onResult));
}

inline void TFuture<void>::Subscribe(
    TDuration timeout,
    TClosure onResult,
    TClosure onTimeout)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

inline void TFuture<void>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(onCancel));
}

inline bool TFuture<void>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

inline TFuture<void> TFuture<void>::Apply(TCallback<void()> mutator)
{
    auto mutated = NewPromise();

    Subscribe(BIND([=] () mutable {
        mutator.Run();
        mutated.Set();
    }));

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
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

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
    }));

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

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
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

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
    }));

    return mutated;
}

inline TFuture<void> TFuture<void>::Finally()
{
    auto promise = NewPromise();
    Subscribe(BIND([=] () mutable { promise.Set(); }));
    OnCanceled(BIND([=] () mutable { promise.Set(); }));
    return promise;
}

inline TFuture<void>::TFuture(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<void>>& state)
    : Impl_(state)
{ }

inline TFuture<void>::TFuture(
    TIntrusivePtr< NYT::NDetail::TPromiseState<void>>&& state)
    : Impl_(std::move(state))
{ }

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
inline TFuture< typename NMpl::TDecay<T>::TType> MakeFuture(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TFuture<U>(New< NYT::NDetail::TPromiseState<U>>(std::forward<T>(value)));
}

inline TFuture<void> MakeFuture()
{
    return TFuture<void>(New< NYT::NDetail::TPromiseState<void>>(true));
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
inline void TPromise<T>::Subscribe(TCallback<void(T)> onResult)
{
    YASSERT(Impl_);
    Impl_->Subscribe(std::move(onResult));
}

template <class T>
inline void TPromise<T>::Subscribe(
    TDuration timeout,
    TCallback<void(T)> onResult,
    TClosure onTimeout)
{
    YASSERT(Impl_);
    Impl_->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

template <class T>
inline void TPromise<T>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(onCancel));
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
inline TPromise<T>::TPromise(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<T>>& state)
    : Impl_(state)
{ }

template <class T>
inline TPromise<T>::TPromise(
    TIntrusivePtr< NYT::NDetail::TPromiseState<T>>&& state)
    : Impl_(std::move(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TPromise<void>

inline TPromise<void>::TPromise()
    : Impl_(nullptr)
{ }

inline TPromise<void>::TPromise(TNull)
    : Impl_(nullptr)
{ }

inline TPromise<void>::operator bool() const
{
    return Impl_ != nullptr;
}

inline void TPromise<void>::Reset()
{
    Impl_.Reset();
}

inline void TPromise<void>::Swap(TPromise& other)
{
    Impl_.Swap(other.Impl_);
}

inline bool TPromise<void>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

inline void TPromise<void>::Set()
{
    YASSERT(Impl_);
    Impl_->Set();
}

inline bool TPromise<void>::TrySet()
{
    YASSERT(Impl_);
    return Impl_->TrySet();
}

inline void TPromise<void>::Get() const
{
    YASSERT(Impl_);
    Impl_->Get();
}

inline void TPromise<void>::Subscribe(TClosure onResult)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(onResult));
}

inline void TPromise<void>::Subscribe(
    TDuration timeout,
    TClosure onResult,
    TClosure onTimeout)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(timeout, std::move(onResult), std::move(onTimeout));
}

inline void TPromise<void>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(onCancel));
}

inline bool TPromise<void>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

inline TFuture<void> TPromise<void>::ToFuture() const
{
    return TFuture<void>(Impl_);
}

// XXX(sandello): Kill this method.
inline TPromise<void>::operator TFuture<void>() const
{
    return TFuture<void>(Impl_);
}

inline TPromise<void>::TPromise(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<void>>& state)
    : Impl_(state)
{ }

inline TPromise<void>::TPromise(
    TIntrusivePtr< NYT::NDetail::TPromiseState<void>>&& state)
    : Impl_(std::move(state))
{ }

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
inline TPromise< typename NMpl::TDecay<T>::TType> MakePromise(T&& value)
{
    typedef typename NMpl::TDecay<T>::TType U;
    return TPromise<U>(New< NYT::NDetail::TPromiseState<U>>(std::forward<T>(value)));
}

template <class T>
inline TPromise<T> NewPromise()
{
    return TPromise<T>(New< NYT::NDetail::TPromiseState<T>>());
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

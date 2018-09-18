#pragma once
#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include "bind.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/event_count.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/small_vector.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declarations

// invoker_util.h.
IInvokerPtr GetFinalizerInvoker();

namespace NConcurrency {

// scheduler.h
TClosure GetCurrentFiberCanceler();

} // namespace NConcurrency

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFutureState
    : public TRefCountedBase
{
public:
    ~TFutureState() noexcept
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        FinalizeTracking();
#endif
    }

    typedef TCallback<void(const TErrorOr<T>&)> TResultHandler;
    typedef SmallVector<TResultHandler, 8> TResultHandlers;

    typedef TClosure TCancelHandler;
    typedef SmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    const bool WellKnown_;

    //! Number of promises.
    std::atomic<int> StrongRefCount_;
    //! Number of futures plus one if there's at least one promise.
    std::atomic<int> WeakRefCount_;

    //! Protects the following section of members.
    mutable TSpinLock SpinLock_;
    std::atomic<bool> Canceled_;
    std::atomic<bool> Set_;
    std::atomic<bool> AbandonedUnset_ = {false};
    TNullable<TErrorOr<T>> Value_;
    mutable std::unique_ptr<NConcurrency::TEvent> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    TCancelHandlers CancelHandlers_;

    template <class F, class... As>
    auto RunNoExcept(F&& functor, As&&... args) noexcept -> decltype(functor(std::forward<As>(args)...))
    {
        return functor(std::forward<As>(args)...);
    }

    template <class U, bool MustSet>
    bool DoSet(U&& value) noexcept
    {
        // Calling subscribers may release the last reference to this.
        TIntrusivePtr<TFutureState> this_(this);

        NConcurrency::TEvent* readyEvent = nullptr;
        bool canceled;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            Y_ASSERT(!AbandonedUnset_);
            if (MustSet) {
                YCHECK(!Set_);
            } else {
                if (Set_) {
                    return false;
                }
            }
            // TODO(sandello): What about exceptions here?
            Value_.Assign(TErrorOr<T>(std::forward<U>(value)));
            Set_ = true;
            canceled = Canceled_;
            readyEvent = ReadyEvent_.get();
        }

        if (readyEvent) {
            readyEvent->NotifyAll();
        }

        for (const auto& handler : ResultHandlers_) {
            RunNoExcept(handler, *Value_);
        }
        ResultHandlers_.clear();

        if (!canceled) {
            CancelHandlers_.clear();
        }

        return true;
    }


    void Dispose()
    {
        // Check for fast path.
        if (Set_) {
            // Just kill the fake weak reference.
            UnrefFuture();
            return;
        }

        // Another fast path: no subscribers.
        {
            auto guard = Guard(SpinLock_);
            if (ResultHandlers_.empty() && CancelHandlers_.empty()) {
                Y_ASSERT(!AbandonedUnset_);
                AbandonedUnset_ = true;
                // Cannot access this after UnrefFuture; in particular, cannot touch SpinLock_ in guard's dtor.
                guard.Release();
                UnrefFuture();
                return;
            }
        }

        // Slow path: notify the subscribers in a dedicated thread.
        GetFinalizerInvoker()->Invoke(BIND([=] () {
            // Set the promise if the value is still missing.
            TrySet(MakeAbandonedError());
            // Kill the fake weak reference.
            UnrefFuture();
        }));
    }

    void Destroy()
    {
        delete this;
    }

    static TError MakeAbandonedError()
    {
        return TError(NYT::EErrorCode::Canceled, "Promise abandoned");
    }

    void InstallAbandonedError()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (AbandonedUnset_ && !Set_) {
            Value_.Assign(MakeAbandonedError());
            Set_ = true;
        }
    }

    void InstallAbandonedError() const
    {
        const_cast<TFutureState*>(this)->InstallAbandonedError();
    }

protected:
    TFutureState(int strongRefCount, int weakRefCount)
        : WellKnown_(false)
        , StrongRefCount_(strongRefCount)
        , WeakRefCount_(weakRefCount)
        , Canceled_(false)
        , Set_(false)
    { }

    template <class U>
    TFutureState(bool wellKnown, int strongRefCount, int weakRefCount, U&& value)
        : WellKnown_(wellKnown)
        , StrongRefCount_(strongRefCount)
        , WeakRefCount_(weakRefCount)
        , Canceled_ (false)
        , Set_(true)
        , Value_(std::forward<U>(value))
    { }

public:
    void RefFuture()
    {
        if (WellKnown_) {
            return;
        }
        auto oldWeakCount = WeakRefCount_++;
        Y_ASSERT(oldWeakCount > 0);
    }

    void UnrefFuture()
    {
        if (WellKnown_) {
            return;
        }
        auto oldWeakCount = WeakRefCount_--;
        Y_ASSERT(oldWeakCount > 0);
        if (oldWeakCount == 1) {
            Destroy();
        }
    }

    void RefPromise()
    {
        Y_ASSERT(!WellKnown_);
        auto oldStrongCount = StrongRefCount_++;
        Y_ASSERT(oldStrongCount > 0 && WeakRefCount_ > 0);
    }

    void UnrefPromise()
    {
        Y_ASSERT(!WellKnown_);
        auto oldStrongCount = StrongRefCount_--;
        Y_ASSERT(oldStrongCount > 0);
        if (oldStrongCount == 1) {
            Dispose();
        }
    }


    const TErrorOr<T>& Get() const
    {
        // Fast path.
        if (Set_) {
            return *Value_;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                return *Value_;
            }
            if (!ReadyEvent_) {
                ReadyEvent_.reset(new NConcurrency::TEvent());
            }
        }

        ReadyEvent_->Wait();

        return *Value_;
    }

    bool TimedWait(TDuration timeout) const
    {
        // Fast path.
        if (Set_ || AbandonedUnset_) {
            return true;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                return true;
            }
            if (!ReadyEvent_) {
                ReadyEvent_.reset(new NConcurrency::TEvent());
            }
        }

        return ReadyEvent_->Wait(timeout.ToDeadLine());
    }

    TNullable<TErrorOr<T>> TryGet() const
    {
        // Fast path.
        if (!Set_ && !AbandonedUnset_) {
            return Null;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            return Set_ ? Value_ : Null;
        }
    }

    bool IsSet() const
    {
        return Set_ || AbandonedUnset_;
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
        // Fast path.
        if (Set_) {
            return false;
        }
        return DoSet<U, false>(std::forward<U>(value));
    }

    void Subscribe(TResultHandler handler)
    {
        // Fast path.
        if (Set_) {
            RunNoExcept(handler, *Value_);
            return;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                guard.Release();
                RunNoExcept(handler, *Value_);
            } else {
                ResultHandlers_.push_back(std::move(handler));
            }
        }
    }

    void OnCanceled(TCancelHandler handler)
    {
        // Fast path.
        if (Set_) {
            return;
        }
        if (Canceled_) {
            RunNoExcept(handler);
            return;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Canceled_) {
                guard.Release();
                RunNoExcept(handler);
            } else if (!Set_) {
                CancelHandlers_.push_back(std::move(handler));
            }
        }
    }

    bool Cancel()
    {
        return DoCancel();
    }

private:
    bool DoCancel() noexcept
    {
        // Calling subscribers may release the last reference to this.
        TIntrusivePtr<TFutureState> this_(this);

        {
            auto guard = Guard(SpinLock_);
            if (Set_ || AbandonedUnset_ || Canceled_) {
                return false;
            }
            Canceled_ = true;
        }

        for (auto& handler : CancelHandlers_) {
            RunNoExcept(handler);
        }
        CancelHandlers_.clear();

        return true;
    }
};

template <class T>
void Ref(TFutureState<T>* state)
{
    state->RefFuture();
}

template <class T>
void Unref(TFutureState<T>* state)
{
    state->UnrefFuture();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPromiseState
    : public TFutureState<T>
{
public:
    explicit TPromiseState(int strongRefCount, int weakRefCount)
        : TFutureState<T>(strongRefCount, weakRefCount)
    { }

    template <class U>
    TPromiseState(bool wellKnown, int strongRefCount, int weakRefCount, U&& value)
        : TFutureState<T>(wellKnown, strongRefCount, weakRefCount, std::forward<U>(value))
    { }
};

template <class T>
void Ref(TPromiseState<T>* state)
{
    state->RefPromise();
}

template <class T>
void Unref(TPromiseState<T>* state)
{
    state->UnrefPromise();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class S>
struct TPromiseSetter;

template <class T, class F>
void InterceptExceptions(TPromise<T>& promise, F func)
{
    try {
        func();
    } catch (const TErrorException& ex) {
        promise.Set(ex.Error());
    } catch (const std::exception& ex) {
        promise.Set(TError(ex));
    }
}

template <class R, class T, class... TArgs>
struct TPromiseSetter<T, R(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<T>& promise, const TCallback<T(TArgs...)>& callback, TCallArgs&&... args)
    {
        InterceptExceptions(
            promise,
            [&] {
                promise.Set(callback.Run(std::forward<TCallArgs>(args)...));
            });
    }
};

template <class R, class T, class... TArgs>
struct TPromiseSetter<T, TErrorOr<R>(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<T>& promise, const TCallback<TErrorOr<T>(TArgs...)>& callback, TCallArgs&&... args)
    {
        InterceptExceptions(
            promise,
            [&] {
                promise.Set(callback.Run(std::forward<TCallArgs>(args)...));
            });
    }
};

template <class... TArgs>
struct TPromiseSetter<void, void(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<void>& promise, const TCallback<void(TArgs...)>& callback, TCallArgs&&... args)
    {
        InterceptExceptions(
            promise,
            [&] {
                callback.Run(std::forward<TCallArgs>(args)...);
                promise.Set();
            });
    }
};

template <class T, class... TArgs>
struct TPromiseSetter<T, TFuture<T>(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<T>& promise, const TCallback<TFuture<T>(TArgs...)>& callback, TCallArgs&&... args)
    {
        InterceptExceptions(
            promise,
            [&] {
                promise.SetFrom(callback.Run(std::forward<TCallArgs>(args)...));
            });
    }
};

template <class T, class... TArgs>
struct TPromiseSetter<T, TErrorOr<TFuture<T>>(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<T>& promise, const TCallback<TFuture<T>(TArgs...)>& callback, TCallArgs&&... args)
    {
        InterceptExceptions(
            promise,
            [&] {
                auto result = callback.Run(std::forward<TCallArgs>(args)...);
                if (result.IsOK()) {
                    promise.SetFrom(std::move(result));
                } else {
                    promise.Set(TError(result));
                }
            });
    }
};

template <class R, class T>
void ApplyHelperHandler(TPromise<T>& promise, const TCallback<R()>& callback, const TError& value)
{
    if (value.IsOK()) {
        TPromiseSetter<T, R()>::Do(promise, callback);
    } else {
        promise.Set(TError(value));
    }
}

template <class R, class T, class U>
void ApplyHelperHandler(TPromise<T>& promise, const TCallback<R(const U&)>& callback, const TErrorOr<U>& value)
{
    if (value.IsOK()) {
        TPromiseSetter<T, R(const U&)>::Do(promise, callback, value.Value());
    } else {
        promise.Set(TError(value));
    }
}

template <class R, class T, class U>
void ApplyHelperHandler(TPromise<T>& promise, const TCallback<R(const TErrorOr<U>&)>& callback, const TErrorOr<U>& value)
{
    TPromiseSetter<T, R(const TErrorOr<U>&)>::Do(promise, callback, value);
}

template <class R, class T, class S>
TFuture<R> ApplyHelper(TFutureBase<T> this_, const TCallback<S>& callback)
{
    Y_ASSERT(this_);

    auto promise = NewPromise<R>();

    this_.Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
        ApplyHelperHandler(promise, callback, value);
    }));

    promise.OnCanceled(BIND([=] () mutable {
        this_.Cancel();
    }));

    return promise;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPromise<T> NewPromise()
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(1, 1));
}

template <class T>
TPromise<T> MakePromise(TErrorOr<T> value)
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(false, 1, 1, std::move(value)));
}

template <class T>
TPromise<T> MakePromise(T value)
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(false, 1, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeFuture(TErrorOr<T> value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(false, 0, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeFuture(T value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(false, 0, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeWellKnownFuture(TErrorOr<T> value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(true, -1, -1, std::move(value)));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return lhs.Impl_ == rhs.Impl_;
}

template <class T>
bool operator!=(const TFuture<T>& lhs, const TFuture<T>& rhs)
{
    return !(lhs == rhs);
}

template <class T>
void swap(TFuture<T>& lhs, TFuture<T>& rhs)
{
    using std::swap;
    swap(lhs.Impl_, rhs.Impl_);
}

template <class T>
bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return lhs.Impl_ == rhs.Impl_;
}

template <class T>
bool operator!=(const TPromise<T>& lhs, const TPromise<T>& rhs)
{
    return *(lhs == rhs);
}

template <class T>
void swap(TPromise<T>& lhs, TPromise<T>& rhs)
{
    using std::swap;
    swap(lhs.Impl_, rhs.Impl_);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFutureBase<T>::operator bool() const
{
    return Impl_.operator bool();
}

template <class T>
void TFutureBase<T>::Reset()
{
    Impl_.Reset();
}

template <class T>
bool TFutureBase<T>::IsSet() const
{
    Y_ASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
const TErrorOr<T>& TFutureBase<T>::Get() const
{
    Y_ASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
bool TFutureBase<T>::TimedWait(TDuration timeout) const
{
    Y_ASSERT(Impl_);
    return Impl_->TimedWait(timeout);
}

template <class T>
TNullable<TErrorOr<T>> TFutureBase<T>::TryGet() const
{
    Y_ASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
void TFutureBase<T>::Subscribe(TCallback<void(const TErrorOr<T>&)> handler)
{
    Y_ASSERT(Impl_);
    return Impl_->Subscribe(std::move(handler));
}

template <class T>
bool TFutureBase<T>::Cancel()
{
    Y_ASSERT(Impl_);
    return Impl_->Cancel();
}

template <class T>
TFuture<T> TFutureBase<T>::ToUncancelable()
{
    if (!Impl_) {
        return TFuture<T>();
    }

    auto promise = NewPromise<T>();

    this->Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
        promise.Set(value);
    }));

    return promise;
}

template <class T>
TFuture<T> TFutureBase<T>::WithTimeout(TDuration timeout)
{
    Y_ASSERT(Impl_);

    if (IsSet()) {
        return TFuture<T>(Impl_);
    }

    auto this_ = *this;
    auto promise = NewPromise<T>();

    auto cookie = NConcurrency::TDelayedExecutor::Submit(
        BIND([=] (bool aborted) mutable {
            TError error;
            if (aborted) {
                error = TError(NYT::EErrorCode::Canceled, "Operation aborted");
            } else {
                error = TError(NYT::EErrorCode::Timeout, "Operation timed out")
                    << TErrorAttribute("timeout", timeout);
            }
            promise.TrySet(error);
            this_.Cancel();
        }),
        timeout);

    Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
        NConcurrency::TDelayedExecutor::CancelAndClear(cookie);
        promise.TrySet(value);
    }));

    promise.OnCanceled(BIND([this_, cookie] () mutable {
        NConcurrency::TDelayedExecutor::CancelAndClear(cookie);
        this_.Cancel();
    }));

    return promise;
}

template <class T>
TFuture<T> TFutureBase<T>::WithTimeout(TNullable<TDuration> timeout)
{
    return timeout ? WithTimeout(*timeout) : TFuture<T>(Impl_);
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<R(const TErrorOr<T>&)> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<TErrorOr<R>(const TErrorOr<T>&)> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<TFuture<R>(const TErrorOr<T>&)> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<TErrorOr<TFuture<R>>(const TErrorOr<T>&)> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class U>
TFuture<U> TFutureBase<T>::As()
{
    if (!Impl_) {
        return TFuture<U>();
    }

    auto promise = NewPromise<U>();

    this->Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
        promise.Set(TErrorOr<U>(value));
    }));

    auto this_ = *this;
    promise.OnCanceled(BIND([this_] () mutable {
        this_.Cancel();
    }));

    return promise;
}

template <class T>
TFutureBase<T>::TFutureBase(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl)
    : Impl_(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T>::TFuture(TNull)
{ }

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<R(const T&)> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<R(T)> callback)
{
    return this->Apply(TCallback<R(const T&)>(callback));
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(const T&)> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(T)> callback)
{
    return this->Apply(TCallback<TFuture<R>(const T&)>(callback));
}

template <class T>
TFuture<T>::TFuture(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl)
    : TFutureBase<T>(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

inline TFuture<void>::TFuture(TNull)
{ }

template <class R>
TFuture<R> TFuture<void>::Apply(TCallback<R()> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class R>
TFuture<R> TFuture<void>::Apply(TCallback<TFuture<R>()> callback)
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

inline TFuture<void>::TFuture(TIntrusivePtr<NYT::NDetail::TFutureState<void>> impl)
    : TFutureBase<void>(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPromiseBase<T>::operator bool() const
{
    return Impl_.operator bool();
}

template <class T>
void TPromiseBase<T>::Reset()
{
    Impl_.Reset();
}

template <class T>
bool TPromiseBase<T>::IsSet() const
{
    Y_ASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
void TPromiseBase<T>::Set(const TErrorOr<T>& value)
{
    Y_ASSERT(Impl_);
    Impl_->Set(value);
}

template <class T>
void TPromiseBase<T>::Set(TErrorOr<T>&& value)
{
    Y_ASSERT(Impl_);
    Impl_->Set(std::move(value));
}

template <class T>
template <class U>
void TPromiseBase<T>::SetFrom(TFuture<U> another)
{
    Y_ASSERT(Impl_);

    auto this_ = *this;

    another.Subscribe(BIND([this_] (const TErrorOr<U>& value) mutable {
        this_.Set(value);
    }));

    OnCanceled(BIND([another] () mutable {
        another.Cancel();
    }));
}

template <class T>
bool TPromiseBase<T>::TrySet(const TErrorOr<T>& value)
{
    Y_ASSERT(Impl_);
    return Impl_->TrySet(value);
}

template <class T>
bool TPromiseBase<T>::TrySet(TErrorOr<T>&& value)
{
    Y_ASSERT(Impl_);
    return Impl_->TrySet(std::move(value));
}

template <class T>
template <class U>
inline void TPromiseBase<T>::TrySetFrom(TFuture<U> another)
{
    Y_ASSERT(Impl_);

    auto this_ = *this;

    another.Subscribe(BIND([this_] (const TErrorOr<U>& value) mutable {
        this_.TrySet(value);
    }));

    OnCanceled(BIND([another] () mutable {
        another.Cancel();
    }));
}

template <class T>
const TErrorOr<T>& TPromiseBase<T>::Get() const
{
    Y_ASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
TNullable<TErrorOr<T>> TPromiseBase<T>::TryGet() const
{
    Y_ASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
bool TPromiseBase<T>::IsCanceled() const
{
    return Impl_->IsCanceled();
}

template <class T>
void TPromiseBase<T>::OnCanceled(TClosure handler)
{
    Y_ASSERT(Impl_);
    Impl_->OnCanceled(std::move(handler));
}

template <class T>
TFuture<T> TPromiseBase<T>::ToFuture() const
{
    return TFuture<T>(Impl_);
}

template <class T>
TPromiseBase<T>::operator TFuture<T>() const
{
    return TFuture<T>(Impl_);
}

template <class T>
TPromiseBase<T>::TPromiseBase(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl)
    : Impl_(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPromise<T>::TPromise(TNull)
{ }

template <class T>
void TPromise<T>::Set(const T& value)
{
    Y_ASSERT(this->Impl_);
    this->Impl_->Set(value);
}

template <class T>
void TPromise<T>::Set(T&& value)
{
    Y_ASSERT(this->Impl_);
    this->Impl_->Set(std::move(value));
}

template <class T>
void TPromise<T>::Set(const TError& error)
{
    Set(TErrorOr<T>(error));
}

template <class T>
void TPromise<T>::Set(TError&& error)
{
    Set(TErrorOr<T>(std::move(error)));
}

template <class T>
bool TPromise<T>::TrySet(const T& value)
{
    Y_ASSERT(this->Impl_);
    return this->Impl_->TrySet(value);
}

template <class T>
bool TPromise<T>::TrySet(T&& value)
{
    Y_ASSERT(this->Impl_);
    return this->Impl_->TrySet(std::move(value));
}

template <class T>
bool TPromise<T>::TrySet(const TError& error)
{
    return TrySet(TErrorOr<T>(error));
}

template <class T>
bool TPromise<T>::TrySet(TError&& error)
{
    return TrySet(TErrorOr<T>(std::move(error)));
}

template <class T>
TPromise<T>::TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl)
    : TPromiseBase<T>(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

inline TPromise<void>::TPromise(TNull)
{ }

inline void TPromise<void>::Set()
{
    Y_ASSERT(this->Impl_);
    this->Impl_->Set(TError());
}

inline bool TPromise<void>::TrySet()
{
    Y_ASSERT(this->Impl_);
    return this->Impl_->TrySet(TError());
}

inline TPromise<void>::TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<void>> impl)
    : TPromiseBase<void>(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TSignature>
struct TAsyncViaHelper;

template <class R, class... TArgs>
struct TAsyncViaHelper<R(TArgs...)>
{
    typedef typename TFutureTraits<R>::TUnderlying TUnderlying;
    typedef TCallback<R(TArgs...)> TSourceCallback;
    typedef TCallback<TFuture<TUnderlying>(TArgs...)> TTargetCallback;

    // TODO(babenko): consider taking promise by non-const ref
    // TODO(babenko): consider moving args
    static void Inner(const TSourceCallback& this_, TPromise<TUnderlying> promise, TArgs... args)
    {
        if (promise.IsCanceled()) {
            promise.Set(TError(
                NYT::EErrorCode::Canceled,
                "Computation was canceled before being started"));
            return;
        }

        auto canceler = NConcurrency::GetCurrentFiberCanceler();
        if (canceler) {
            promise.OnCanceled(std::move(canceler));
        }

        NYT::NDetail::TPromiseSetter<TUnderlying, R(TArgs...)>::Do(promise, this_, args...);
    }

    // TODO(babenko): consider moving args
    static TFuture<TUnderlying> Outer(const TSourceCallback& this_, const IInvokerPtr& invoker, TArgs... args)
    {
        auto promise = NewPromise<TUnderlying>();
        invoker->Invoke(BIND(&Inner, this_, promise, args...));
        return promise;
    }

    static TTargetCallback Do(TSourceCallback this_, IInvokerPtr invoker)
    {
        return BIND(&Outer, std::move(this_), std::move(invoker));
    }
};

} // namespace NDetail

template <class R, class... TArgs>
TCallback<typename TFutureTraits<R>::TWrapped(TArgs...)>
TCallback<R(TArgs...)>::AsyncVia(IInvokerPtr invoker) const
{
    return NYT::NDetail::TAsyncViaHelper<R(TArgs...)>::Do(*this, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFutureHolder<T>::TFutureHolder()
{ }

template <class T>
TFutureHolder<T>::TFutureHolder(TNull)
{ }

template <class T>
TFutureHolder<T>::TFutureHolder(TFuture<T> future)
    : Future_(std::move(future))
{ }

template <class T>
TFutureHolder<T>::~TFutureHolder()
{
    if (Future_) {
        Future_.Cancel();
    }
}

template <class T>
TFutureHolder<T>::operator bool() const
{
    return static_cast<bool>(Future_);
}

template <class T>
TFuture<T>& TFutureHolder<T>::Get()
{
    return Future_;
}

template <class T>
const TFuture<T>& TFutureHolder<T>::Get() const
{
    return Future_;
}

template <class T>
const TFuture<T>& TFutureHolder<T>::operator*() const // noexcept
{
    return Future_;
}

template <class T>
TFuture<T>& TFutureHolder<T>::operator*() // noexcept
{
    return Future_;
}

template <class T>
const TFuture<T>* TFutureHolder<T>::operator->() const // noexcept
{
    return &Future_;
}

template <class T>
TFuture<T>* TFutureHolder<T>::operator->() // noexcept
{
    return &Future_;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
class TFutureCombinerVectorResultHolder
{
public:
    using TResult = std::vector<T>;

public:
    explicit TFutureCombinerVectorResultHolder(int size)
        : Result_(size)
    { }

    void SetItem(int index, const TErrorOr<T>& value)
    {
        Result_[index] = value.Value();
    }

    void SetPromise(TPromise<TResult>& promise)
    {
        promise.TrySet(std::move(Result_));
    }

private:
    TResult Result_;

};

template <>
class TFutureCombinerVectorResultHolder<void>
{
public:
    using TResult = void;

public:
    explicit TFutureCombinerVectorResultHolder(int /*size*/)
    { }

    void SetItem(int /*index*/, const TError& /*value*/)
    { }

    void SetPromise(TPromise<TResult>& promise)
    {
        promise.TrySet();
    }
};

template <class K, class T>
class TFutureCombinerHashMapResultHolder
{
public:
    using TResult = THashMap<K, T>;

public:
    TFutureCombinerHashMapResultHolder(std::vector<K> keys)
        : Keys_(std::move(keys))
    { }

    void SetItem(int index, const TErrorOr<T>& value)
    {
        Result_.emplace(Keys_[index], value.Value());
    }

    void SetPromise(TPromise<TResult>& promise)
    {
        promise.TrySet(std::move(Result_));
    }

private:
    const std::vector<K> Keys_;
    TResult Result_;
};

template <class K>
class TFutureCombinerHashMapResultHolder<K, void>
{
public:
    using TResult = void;

public:
    TFutureCombinerHashMapResultHolder(std::vector<K> /*keys*/)
    { }

    void SetItem(int /*index*/, const TError& /*value*/)
    { }

    void SetPromise(TPromise<TResult>& promise)
    {
        promise.TrySet();
    }
};

template <class TItem, class TResult>
class TFutureCombinerBase
    : public TRefCounted
{
public:
    explicit TFutureCombinerBase(std::vector<TFuture<TItem>> futures)
        : Futures_(std::move(futures))
    { }

    TFuture<TResult> Run()
    {
        if (ShouldCompletePrematurely()) {
            SetPromisePrematurely(Promise_);
        } else {
            for (size_t index = 0; index < Futures_.size(); ++index) {
                Futures_[index].Subscribe(BIND(&TFutureCombinerBase::OnFutureSet, MakeStrong(this), index));
            }
            Promise_.OnCanceled(BIND(&TFutureCombinerBase::CancelFutures, MakeWeak(this)));
        }
        return Promise_;
    }

protected:
    std::vector<TFuture<TItem>> Futures_;
    TPromise<TResult> Promise_ = NewPromise<TResult>();

    void CancelFutures()
    {
        for (size_t index = 0; index < Futures_.size(); ++index) {
            Futures_[index].Cancel();
        }
    }

    virtual void OnFutureSet(int futureIndex, const TErrorOr<TItem>& result) = 0;
    virtual bool ShouldCompletePrematurely() = 0;

private:
    template <class T>
    static void SetPromisePrematurely(TPromise<T>& promise)
    {
        promise.Set(T());
    }

    static void SetPromisePrematurely(TPromise<void>& promise)
    {
        promise.Set();
    }
};

template <class T, class TResultHolder>
class TFutureCombiner
    : public TFutureCombinerBase<T, typename TResultHolder::TResult>
{
public:
    template <typename... Args>
    TFutureCombiner(std::vector<TFuture<T>> futures, Args... args)
        : TFutureCombinerBase<T, typename TResultHolder::TResult>(std::move(futures))
        , ResultHolder_(args...)
        , PendingResponseCount_(this->Futures_.size())
    { }

private:
    TResultHolder ResultHolder_;
    std::atomic<int> PendingResponseCount_;

    virtual void OnFutureSet(int futureIndex, const TErrorOr<T>& result) override
    {
        if (!result.IsOK()) {
            this->Promise_.TrySet(TError(result));
            this->CancelFutures();
            return;
        }

        ResultHolder_.SetItem(futureIndex, result);

        if (--PendingResponseCount_ == 0) {
            ResultHolder_.SetPromise(this->Promise_);
        }
    }

    virtual bool ShouldCompletePrematurely() override
    {
        return this->Futures_.empty();
    }
};

template <class T>
class TQuorumFutureCombiner
    : public TFutureCombinerBase<T, typename TFutureCombineTraits<T>::TCombinedVector>
{
public:
    TQuorumFutureCombiner(std::vector<TFuture<T>> futures, int quorum)
        : TFutureCombinerBase<T, typename TFutureCombineTraits<T>::TCombinedVector>(std::move(futures))
        , Quorum_(quorum)
        , ResultHolder_(quorum)
        , PendingResponseCount_(quorum)
    { }

private:
    const int Quorum_;

    TFutureCombinerVectorResultHolder<T> ResultHolder_;
    std::atomic<int> PendingResponseCount_;
    std::atomic<int> CurrentResponseIndex_ = {0};


    virtual void OnFutureSet(int /*futureIndex*/, const TErrorOr<T>& result) override
    {
        if (!result.IsOK()) {
            this->Promise_.TrySet(TError(result));
            this->CancelFutures();
            return;
        }

        int responseIndex = CurrentResponseIndex_++;
        if (responseIndex < Quorum_) {
            ResultHolder_.SetItem(responseIndex, result);
        }

        if (--PendingResponseCount_ == 0) {
            ResultHolder_.SetPromise(this->Promise_);
        }
    }

    virtual bool ShouldCompletePrematurely() override
    {
        return this->Futures_.empty() || Quorum_ == 0;
    }
};

template <class T>
class TAllFutureCombiner
    : public TFutureCombinerBase<T, std::vector<TErrorOr<T>>>
{
public:
    explicit TAllFutureCombiner(std::vector<TFuture<T>> futures)
        : TFutureCombinerBase<T, std::vector<TErrorOr<T>>>(std::move(futures))
        , Results_(this->Futures_.size())
        , PendingResponseCount_(this->Futures_.size())
    { }

private:
    std::vector<TErrorOr<T>> Results_;
    std::atomic<int> PendingResponseCount_;


    virtual void OnFutureSet(int futureIndex, const TErrorOr<T>& result) override
    {
        Results_[futureIndex] = result;
        if (--PendingResponseCount_ == 0) {
            this->Promise_.Set(std::move(Results_));
        }
    }

    virtual bool ShouldCompletePrematurely() override
    {
        return this->Futures_.empty();
    }
};

} // namespace NDetail

template <class T>
TFuture<typename TFutureCombineTraits<T>::TCombinedVector> Combine(
    std::vector<TFuture<T>> futures)
{
    auto size = futures.size();
    return New<NDetail::TFutureCombiner<T, NDetail::TFutureCombinerVectorResultHolder<T>>>(std::move(futures), size)
        ->Run();
}

template <class K, class T>
TFuture<typename TFutureCombineTraits<T>::template TCombinedHashMap<K>> Combine(
    const THashMap<K, TFuture<T>>& futures)
{
    const size_t size = futures.size();
    std::vector<K> keys;
    keys.reserve(size);
    std::vector<TFuture<T>> values;
    values.reserve(size);

    for (const auto& item : futures) {
        keys.emplace_back(item.first);
        values.emplace_back(item.second);
    }

    return New<NDetail::TFutureCombiner<T, NDetail::TFutureCombinerHashMapResultHolder<K,T>>>(std::move(values), std::move(keys))
        ->Run();
}

template <class T>
TFuture<typename TFutureCombineTraits<T>::TCombinedVector> CombineQuorum(
    std::vector<TFuture<T>> futures,
    int quorum)
{
    YCHECK(quorum >= 0);
    return New<NDetail::TQuorumFutureCombiner<T>>(std::move(futures), quorum)
        ->Run();
}

template <class T>
TFuture<typename TFutureCombineTraits<T>::TCombinedVector> Combine(
    std::vector<TFutureHolder<T>> holders)
{
    std::vector<TFuture<T>> futures;
    futures.reserve(holders.size());
    for (auto& holder : holders) {
        futures.push_back(holder.Get());
    }
    return Combine(std::move(futures));
}

template <class T>
TFuture<std::vector<TErrorOr<T>>> CombineAll(
    std::vector<TFuture<T>> futures)
{
    return New<NDetail::TAllFutureCombiner<T>>(std::move(futures))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
class TBoundedConcurrencyRunner
    : public TIntrinsicRefCounted
{
public:
    TBoundedConcurrencyRunner(
        std::vector<TCallback<TFuture<T>()>> callbacks,
        int concurrencyLimit)
        : Callbacks_(std::move(callbacks))
        , ConcurrencyLimit_(concurrencyLimit)
        , Results_(Callbacks_.size())
    { }

    TFuture<std::vector<TErrorOr<T>>> Run()
    {
        if (Callbacks_.empty()) {
            return MakeFuture(std::vector<TErrorOr<T>>());
        }
        int startImmediatelyCount = std::min(ConcurrencyLimit_, static_cast<int>(Callbacks_.size()));
        CurrentIndex_ = startImmediatelyCount;
        for (int index = 0; index < startImmediatelyCount; ++index) {
            RunCallback(index);
        }
        return Promise_;
    }

private:
    const std::vector<TCallback<TFuture<T>()>> Callbacks_;
    const int ConcurrencyLimit_;

    std::vector<TErrorOr<T>> Results_;
    TPromise<std::vector<TErrorOr<T>>> Promise_ = NewPromise<std::vector<TErrorOr<T>>>();
    std::atomic<int> CurrentIndex_;
    std::atomic<int> FinishedCount_ = {0};


    void RunCallback(int index)
    {
        Callbacks_[index].Run().Subscribe(
            BIND(&TBoundedConcurrencyRunner::OnResult, MakeStrong(this), index));
    }

    void OnResult(int index, const TErrorOr<T>& result)
    {
        Results_[index] = result;

        int newIndex = CurrentIndex_++;
        if (newIndex < Callbacks_.size()) {
            RunCallback(newIndex);
        }

        if (++FinishedCount_ == Callbacks_.size()) {
            Promise_.Set(Results_);
        }
    }
};

} // namespace NDetail

template <class T>
TFuture<std::vector<TErrorOr<T>>> RunWithBoundedConcurrency(
    std::vector<TCallback<TFuture<T>()>> callbacks,
    int concurrencyLimit)
{
    YCHECK(concurrencyLimit >= 0);
    return New<NDetail::TBoundedConcurrencyRunner<T>>(std::move(callbacks), concurrencyLimit)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

//! A hasher for TFuture.
template <class T>
struct THash<NYT::TFuture<T>>
{
    size_t operator () (const NYT::TFuture<T>& future) const
    {
        return THash<NYT::TIntrusivePtr<NYT::NDetail::TFutureState<T>>>()(future.Impl_);
    }
};

//! A hasher for TPromise.
template <class T>
struct THash<NYT::TPromise<T>>
{
    size_t operator () (const NYT::TPromise<T>& promise) const
    {
        return THash<NYT::TIntrusivePtr<NYT::NDetail::TPromiseState<T>>>()(promise.Impl_);
    }
};

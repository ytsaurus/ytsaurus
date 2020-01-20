#pragma once
#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
// For the sake of sane code completion.
#include "future.h"
#endif
#undef FUTURE_INL_H_

#include "bind.h"
#include "invoker_util.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/event_count.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/small_vector.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declarations

namespace NConcurrency {

// scheduler.h
TCallback<void(const TError&)> GetCurrentFiberCanceler();

} // namespace NConcurrency

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

inline TError MakeAbandonedError()
{
    return TError(NYT::EErrorCode::Canceled, "Promise abandoned");
}

inline TError MakeCanceledError(const TError& error)
{
    return TError(NYT::EErrorCode::Canceled, "Operation canceled")
        << error;
}

////////////////////////////////////////////////////////////////////////////////

class TFutureStateBase
    : public TRefCountedBase
{
public:
    using TVoidResultHandler = TClosure;
    using TVoidResultHandlers = SmallVector<TVoidResultHandler, 8>;

    using TCancelHandler = TCallback<void(const TError&)>;
    using TCancelHandlers = SmallVector<TCancelHandler, 8>;

    virtual ~TFutureStateBase() noexcept
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        FinalizeTracking();
#endif
    }

    void RefFuture()
    {
        if (WellKnown_) {
            return;
        }
        auto oldWeakCount = WeakRefCount_++;
        YT_ASSERT(oldWeakCount > 0);
    }

    void UnrefFuture()
    {
        if (WellKnown_) {
            return;
        }
        auto oldWeakCount = WeakRefCount_--;
        YT_ASSERT(oldWeakCount > 0);
        if (oldWeakCount == 1) {
            delete this;
        }
    }

    void RefPromise()
    {
        YT_ASSERT(!WellKnown_);
        auto oldStrongCount = StrongRefCount_++;
        YT_ASSERT(oldStrongCount > 0 && WeakRefCount_ > 0);
    }

    void UnrefPromise()
    {
        YT_ASSERT(!WellKnown_);
        auto oldStrongCount = StrongRefCount_--;
        YT_ASSERT(oldStrongCount > 0);
        if (oldStrongCount == 1) {
            Dispose();
        }
    }

    void Subscribe(TVoidResultHandler handler);

    bool Cancel(const TError& error) noexcept;

    void OnCanceled(TCancelHandler handler);

    bool IsSet() const
    {
        return Set_ || AbandonedUnset_;
    }

    bool IsCanceled() const
    {
        return Canceled_;
    }

    bool TimedWait(TDuration timeout) const;

protected:
    const bool WellKnown_;

    //! Number of promises.
    std::atomic<int> StrongRefCount_;
    //! Number of futures plus one if there's at least one promise.
    std::atomic<int> WeakRefCount_;

    //! Protects the following section of members.
    mutable TSpinLock SpinLock_;
    std::atomic<bool> Canceled_;
    TError CancelationError_;
    std::atomic<bool> Set_;
    std::atomic<bool> AbandonedUnset_ = {false};

    bool HasHandlers_ = false;
    TVoidResultHandlers VoidResultHandlers_;
    TCancelHandlers CancelHandlers_;

    mutable std::unique_ptr<NConcurrency::TEvent> ReadyEvent_;

    TFutureStateBase(int strongRefCount, int weakRefCount)
        : WellKnown_(false)
        , StrongRefCount_(strongRefCount)
        , WeakRefCount_(weakRefCount)
        , Canceled_(false)
        , Set_(false)
    { }

    TFutureStateBase(bool wellKnown, int strongRefCount, int weakRefCount)
        : WellKnown_(wellKnown)
        , StrongRefCount_(strongRefCount)
        , WeakRefCount_(weakRefCount)
        , Canceled_ (false)
        , Set_(true)
    { }


    template <class F, class... As>
    static auto RunNoExcept(F&& functor, As&&... args) noexcept -> decltype(functor(std::forward<As>(args)...))
    {
        return functor(std::forward<As>(args)...);
    }


    virtual void DoInstallAbandonedError() = 0;
    virtual void DoTrySetAbandonedError() = 0;
    virtual bool DoTrySetCanceledError(const TError& error) = 0;

    void InstallAbandonedError();
    void InstallAbandonedError() const;

private:
    void Dispose();
};

Y_FORCE_INLINE void Ref(TFutureStateBase* state)
{
    state->RefFuture();
}

Y_FORCE_INLINE void Unref(TFutureStateBase* state)
{
    state->UnrefFuture();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFutureState
    : public TFutureStateBase
{
public:
    using TResultHandler = TCallback<void(const TErrorOr<T>&)>;
    using TResultHandlers = SmallVector<TResultHandler, 8>;

    using TUniqueResultHandler = TCallback<void(TErrorOr<T>&&)>;

private:
    std::optional<TErrorOr<T>> Value_;
#ifndef NDEBUG
    std::atomic_flag ValueMovedOut_ = ATOMIC_FLAG_INIT;
#endif

    TResultHandlers ResultHandlers_;
    TUniqueResultHandler UniqueResultHandler_;


    template <class U, bool MustSet>
    bool DoSet(U&& value) noexcept
    {
        // Calling subscribers may release the last reference to this.
        TIntrusivePtr<TFutureState> this_(this);

        NConcurrency::TEvent* readyEvent = nullptr;
        bool canceled;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YT_ASSERT(!AbandonedUnset_);
            if (MustSet && !Canceled_) {
                YT_VERIFY(!Set_);
            } else if (Set_) {
                return false;
            }
            // TODO(sandello): What about exceptions here?
            Value_.emplace(std::forward<U>(value));
            Set_ = true;
            canceled = Canceled_;
            readyEvent = ReadyEvent_.get();
        }

        if (readyEvent) {
            readyEvent->NotifyAll();
        }

        for (const auto& handler : VoidResultHandlers_) {
            RunNoExcept(handler);
        }
        VoidResultHandlers_.clear();

        for (const auto& handler : ResultHandlers_) {
            RunNoExcept(handler, *Value_);
        }
        ResultHandlers_.clear();

        if (UniqueResultHandler_) {
            RunNoExcept(UniqueResultHandler_, MoveValueOut());
            UniqueResultHandler_ = {};
        }

        if (!canceled) {
            CancelHandlers_.clear();
        }

        return true;
    }

    TErrorOr<T> MoveValueOut()
    {
#ifndef NDEBUG
        YT_ASSERT(!ValueMovedOut_.test_and_set());
#endif
        auto result = std::move(*Value_);
        Value_.reset();
        return result;
    }

    virtual void DoInstallAbandonedError() override
    {
        Value_ = MakeAbandonedError();
        Set_ = true;
    }

    virtual void DoTrySetAbandonedError() override
    {
        TrySet(MakeAbandonedError());
    }

    virtual bool DoTrySetCanceledError(const TError& error) override
    {
        return TrySet(MakeCanceledError(error));
    }

protected:
    TFutureState(int strongRefCount, int weakRefCount)
        : TFutureStateBase(strongRefCount, weakRefCount)
    { }

    template <class U>
    TFutureState(bool wellKnown, int strongRefCount, int weakRefCount, U&& value)
        : TFutureStateBase(wellKnown, strongRefCount, weakRefCount)
        , Value_(std::forward<U>(value))
    { }

public:
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

    TErrorOr<T> GetUnique()
    {
        // Fast path.
        if (Set_) {
            return MoveValueOut();
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                return MoveValueOut();
            }
            if (!ReadyEvent_) {
                ReadyEvent_.reset(new NConcurrency::TEvent());
            }
        }

        ReadyEvent_->Wait();

        return MoveValueOut();
    }

    std::optional<TErrorOr<T>> TryGet() const
    {
        // Fast path.
        if (!Set_ && !AbandonedUnset_) {
            return std::nullopt;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (!Set_) {
                return std::nullopt;
            }
            return Value_;
        }
    }

    std::optional<TErrorOr<T>> TryGetUnique()
    {
        // Fast path.
        if (!Set_ && !AbandonedUnset_) {
            return std::nullopt;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (!Set_) {
                return std::nullopt;
            }
            return MoveValueOut();
        }
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
                HasHandlers_ = true;
            }
        }
    }

    void SubscribeUnique(TUniqueResultHandler handler)
    {
        // Fast path.
        if (Set_) {
            RunNoExcept(handler, MoveValueOut());
            return;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                guard.Release();
                RunNoExcept(handler, MoveValueOut());
            } else {
                YT_ASSERT(!UniqueResultHandler_);
                YT_ASSERT(ResultHandlers_.empty());
                UniqueResultHandler_ = std::move(handler);
                HasHandlers_ = true;
            }
        }
    }
};

template <class T>
Y_FORCE_INLINE void Ref(TFutureState<T>* state)
{
    state->RefFuture();
}

template <class T>
Y_FORCE_INLINE void Unref(TFutureState<T>* state)
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
Y_FORCE_INLINE void Ref(TPromiseState<T>* state)
{
    state->RefPromise();
}

template <class T>
Y_FORCE_INLINE void Unref(TPromiseState<T>* state)
{
    state->UnrefPromise();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class S>
struct TPromiseSetter;

template <class T, class F>
void InterceptExceptions(const TPromise<T>& promise, const F& func)
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
    static void Do(const TPromise<T>& promise, const TCallback<T(TArgs...)>& callback, TCallArgs&&... args)
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
    static void Do(const TPromise<T>& promise, const TCallback<TErrorOr<T>(TArgs...)>& callback, TCallArgs&&... args)
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
    static void Do(const TPromise<void>& promise, const TCallback<void(TArgs...)>& callback, TCallArgs&&... args)
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
    static void Do(const TPromise<T>& promise, const TCallback<TFuture<T>(TArgs...)>& callback, TCallArgs&&... args)
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
    static void Do(const TPromise<T>& promise, const TCallback<TFuture<T>(TArgs...)>& callback, TCallArgs&&... args)
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
void ApplyHelperHandler(const TPromise<T>& promise, const TCallback<R()>& callback, const TError& value)
{
    if (value.IsOK()) {
        TPromiseSetter<T, R()>::Do(promise, callback);
    } else {
        promise.Set(TError(value));
    }
}

template <class R, class T, class U>
void ApplyHelperHandler(const TPromise<T>& promise, const TCallback<R(const U&)>& callback, const TErrorOr<U>& value)
{
    if (value.IsOK()) {
        TPromiseSetter<T, R(const U&)>::Do(promise, callback, value.Value());
    } else {
        promise.Set(TError(value));
    }
}

template <class R, class T, class U>
void ApplyHelperHandler(const TPromise<T>& promise, const TCallback<R(const TErrorOr<U>&)>& callback, const TErrorOr<U>& value)
{
    TPromiseSetter<T, R(const TErrorOr<U>&)>::Do(promise, callback, value);
}

template <class R, class T, class S>
TFuture<R> ApplyHelper(TFutureBase<T> this_, TCallback<S> callback)
{
    YT_ASSERT(this_);

    auto promise = NewPromise<R>();

    this_.Subscribe(BIND([=, callback = std::move(callback)] (const TErrorOr<T>& value) {
        ApplyHelperHandler(promise, callback, value);
    }));

    promise.OnCanceled(BIND([=] (const TError& error) {
        this_.Cancel(error);
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

inline bool operator==(const TAwaitable& lhs, const TAwaitable& rhs)
{
    return lhs.Impl_ == rhs.Impl_;
}

inline bool operator!=(const TAwaitable& lhs, const TAwaitable& rhs)
{
    return !(lhs == rhs);
}

inline void swap(TAwaitable& lhs, TAwaitable& rhs)
{
    using std::swap;
    swap(lhs.Impl_, rhs.Impl_);
}

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

inline TAwaitable::operator bool() const
{
    return Impl_.operator bool();
}

inline void TAwaitable::Reset()
{
    Impl_.Reset();
}

inline void TAwaitable::Subscribe(TClosure handler) const
{
    YT_ASSERT(Impl_);
    return Impl_->Subscribe(std::move(handler));
}

inline bool TAwaitable::Cancel(const TError& error) const
{
    YT_ASSERT(Impl_);
    return Impl_->Cancel(error);
}

inline TAwaitable::TAwaitable(TIntrusivePtr<NYT::NDetail::TFutureStateBase> impl)
    : Impl_(std::move(impl))
{ }

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
    YT_ASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
const TErrorOr<T>& TFutureBase<T>::Get() const
{
    YT_ASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
TErrorOr<T> TFutureBase<T>::GetUnique() const
{
    YT_ASSERT(Impl_);
    return Impl_->GetUnique();
}

template <class T>
bool TFutureBase<T>::TimedWait(TDuration timeout) const
{
    YT_ASSERT(Impl_);
    return Impl_->TimedWait(timeout);
}

template <class T>
std::optional<TErrorOr<T>> TFutureBase<T>::TryGet() const
{
    YT_ASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
std::optional<TErrorOr<T>> TFutureBase<T>::TryGetUnique() const
{
    YT_ASSERT(Impl_);
    return Impl_->TryGetUnique();
}

template <class T>
void TFutureBase<T>::Subscribe(TCallback<void(const TErrorOr<T>&)> handler) const
{
    YT_ASSERT(Impl_);
    return Impl_->Subscribe(std::move(handler));
}

template <class T>
void TFutureBase<T>::SubscribeUnique(TCallback<void(TErrorOr<T>&&)> handler) const
{
    YT_ASSERT(Impl_);
    return Impl_->SubscribeUnique(std::move(handler));
}

template <class T>
bool TFutureBase<T>::Cancel(const TError& error) const
{
    YT_ASSERT(Impl_);
    return Impl_->Cancel(error);
}

template <class T>
TFuture<T> TFutureBase<T>::ToUncancelable() const
{
    if (!Impl_) {
        return TFuture<T>();
    }

    auto promise = NewPromise<T>();

    this->Subscribe(BIND([=] (const TErrorOr<T>& value) {
        promise.Set(value);
    }));

    return promise;
}

template <class T>
TFuture<T> TFutureBase<T>::ToImmediatelyCancelable() const
{
    if (!Impl_) {
        return TFuture<T>();
    }

    auto promise = NewPromise<T>();

    this->Subscribe(BIND([=] (const TErrorOr<T>& value) {
        promise.TrySet(value);
    }));

    promise.OnCanceled(BIND([=, underlying = Impl_] (const TError& error) {
        underlying->Cancel(error);
        promise.TrySet(NDetail::MakeCanceledError(error));
    }));

    return promise;
}

template <class T>
TFuture<T> TFutureBase<T>::WithTimeout(TDuration timeout) const
{
    YT_ASSERT(Impl_);

    if (IsSet()) {
        return TFuture<T>(Impl_);
    }

    auto this_ = *this;
    auto promise = NewPromise<T>();

    auto cookie = NConcurrency::TDelayedExecutor::Submit(
        BIND([=] (bool aborted) {
            TError error;
            if (aborted) {
                error = TError(NYT::EErrorCode::Canceled, "Operation aborted");
            } else {
                error = TError(NYT::EErrorCode::Timeout, "Operation timed out")
                    << TErrorAttribute("timeout", timeout);
            }
            promise.TrySet(error);
            this_.Cancel(error);
        }),
        timeout);

    Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
        NConcurrency::TDelayedExecutor::CancelAndClear(cookie);
        promise.TrySet(value);
    }));

    promise.OnCanceled(BIND([this_, cookie] (const TError& error) mutable {
        NConcurrency::TDelayedExecutor::CancelAndClear(cookie);
        this_.Cancel(error);
    }));

    return promise;
}

template <class T>
TFuture<T> TFutureBase<T>::WithTimeout(std::optional<TDuration> timeout) const
{
    return timeout ? WithTimeout(*timeout) : TFuture<T>(Impl_);
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<R(const TErrorOr<T>&)> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, std::move(callback));
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<TErrorOr<R>(const TErrorOr<T>&)> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, std::move(callback));
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<TFuture<R>(const TErrorOr<T>&)> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, std::move(callback));
}

template <class T>
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<TErrorOr<TFuture<R>>(const TErrorOr<T>&)> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, std::move(callback));
}

template <class T>
template <class U>
TFuture<U> TFutureBase<T>::As() const
{
    if (!Impl_) {
        return TFuture<U>();
    }

    auto promise = NewPromise<U>();

    this->Subscribe(BIND([=] (const TErrorOr<T>& value) {
        promise.Set(TErrorOr<U>(value));
    }));

    auto this_ = *this;
    promise.OnCanceled(BIND([this_] (const TError& error) {
        this_.Cancel(error);
    }));

    return promise;
}

template <class T>
TAwaitable TFutureBase<T>::AsAwaitable() const
{
    return TAwaitable(Impl_);
}

template <class T>
TFutureBase<T>::TFutureBase(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl)
    : Impl_(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T>::TFuture(std::nullopt_t)
{ }

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<R(const T&)> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<R(T)> callback) const
{
    return this->Apply(TCallback<R(const T&)>(callback));
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(const T&)> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class T>
template <class R>
TFuture<R> TFuture<T>::Apply(TCallback<TFuture<R>(T)> callback) const
{
    return this->Apply(TCallback<TFuture<R>(const T&)>(callback));
}

template <class T>
TFuture<T>::TFuture(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl)
    : TFutureBase<T>(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

inline TFuture<void>::TFuture(std::nullopt_t)
{ }

template <class R>
TFuture<R> TFuture<void>::Apply(TCallback<R()> callback) const
{
    return NYT::NDetail::ApplyHelper<R>(*this, callback);
}

template <class R>
TFuture<R> TFuture<void>::Apply(TCallback<TFuture<R>()> callback) const
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
    YT_ASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
void TPromiseBase<T>::Set(const TErrorOr<T>& value) const
{
    YT_ASSERT(Impl_);
    Impl_->Set(value);
}

template <class T>
void TPromiseBase<T>::Set(TErrorOr<T>&& value) const
{
    YT_ASSERT(Impl_);
    Impl_->Set(std::move(value));
}

template <class T>
template <class U>
void TPromiseBase<T>::SetFrom(const TFuture<U>& another) const
{
    YT_ASSERT(Impl_);

    auto this_ = *this;

    another.Subscribe(BIND([this_] (const TErrorOr<U>& value)   {
        this_.Set(value);
    }));

    OnCanceled(BIND([another] (const TError& error) {
        another.Cancel(error);
    }));
}

template <class T>
bool TPromiseBase<T>::TrySet(const TErrorOr<T>& value) const
{
    YT_ASSERT(Impl_);
    return Impl_->TrySet(value);
}

template <class T>
bool TPromiseBase<T>::TrySet(TErrorOr<T>&& value) const
{
    YT_ASSERT(Impl_);
    return Impl_->TrySet(std::move(value));
}

template <class T>
template <class U>
inline void TPromiseBase<T>::TrySetFrom(TFuture<U> another) const
{
    YT_ASSERT(Impl_);

    auto this_ = *this;

    another.Subscribe(BIND([this_] (const TErrorOr<U>& value) {
        this_.TrySet(value);
    }));

    OnCanceled(BIND([another] (const TError& error) {
        another.Cancel(error);
    }));
}

template <class T>
const TErrorOr<T>& TPromiseBase<T>::Get() const
{
    YT_ASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
std::optional<TErrorOr<T>> TPromiseBase<T>::TryGet() const
{
    YT_ASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
bool TPromiseBase<T>::IsCanceled() const
{
    return Impl_->IsCanceled();
}

template <class T>
void TPromiseBase<T>::OnCanceled(TCallback<void(const TError&)> handler) const
{
    YT_ASSERT(Impl_);
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
TPromise<T>::TPromise(std::nullopt_t)
{ }

template <class T>
void TPromise<T>::Set(const T& value) const
{
    YT_ASSERT(this->Impl_);
    this->Impl_->Set(value);
}

template <class T>
void TPromise<T>::Set(T&& value) const
{
    YT_ASSERT(this->Impl_);
    this->Impl_->Set(std::move(value));
}

template <class T>
void TPromise<T>::Set(const TError& error) const
{
    Set(TErrorOr<T>(error));
}

template <class T>
void TPromise<T>::Set(TError&& error) const
{
    Set(TErrorOr<T>(std::move(error)));
}

template <class T>
bool TPromise<T>::TrySet(const T& value) const
{
    YT_ASSERT(this->Impl_);
    return this->Impl_->TrySet(value);
}

template <class T>
bool TPromise<T>::TrySet(T&& value) const
{
    YT_ASSERT(this->Impl_);
    return this->Impl_->TrySet(std::move(value));
}

template <class T>
bool TPromise<T>::TrySet(const TError& error) const
{
    return TrySet(TErrorOr<T>(error));
}

template <class T>
bool TPromise<T>::TrySet(TError&& error) const
{
    return TrySet(TErrorOr<T>(std::move(error)));
}

template <class T>
TPromise<T>::TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl)
    : TPromiseBase<T>(std::move(impl))
{ }

////////////////////////////////////////////////////////////////////////////////

inline TPromise<void>::TPromise(std::nullopt_t)
{ }

inline void TPromise<void>::Set() const
{
    YT_ASSERT(this->Impl_);
    this->Impl_->Set(TError());
}

inline bool TPromise<void>::TrySet() const
{
    YT_ASSERT(this->Impl_);
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
    using TUnderlying = typename TFutureTraits<R>::TUnderlying;
    using TSourceCallback = TCallback<R(TArgs...)>;
    using TTargetCallback = TCallback<TFuture<TUnderlying>(TArgs...)>;

    static void Inner(
        const TSourceCallback& this_,
        const TPromise<TUnderlying>& promise,
        TArgs... args)
    {
        if (promise.IsCanceled()) {
            promise.Set(TError(
                NYT::EErrorCode::Canceled,
                "Computation was canceled before it was started"));
            return;
        }

        auto canceler = NConcurrency::GetCurrentFiberCanceler();
        if (canceler) {
            promise.OnCanceled(std::move(canceler));
        }

        NYT::NDetail::TPromiseSetter<TUnderlying, R(TArgs...)>::Do(promise, this_, args...);
    }

    static TFuture<TUnderlying> Outer(
        const TSourceCallback& this_,
        const IInvokerPtr& invoker,
        TArgs... args)
    {
        auto promise = NewPromise<TUnderlying>();
        invoker->Invoke(BIND(&Inner, this_, promise, args...));
        return promise;
    }

    static TFuture<TUnderlying> OuterGuarded(
        const TSourceCallback& this_,
        const IInvokerPtr& invoker,
        TError cancellationError,
        TArgs... args)
    {
        auto promise = NewPromise<TUnderlying>();
        GuardedInvoke(
            invoker,
            BIND(&Inner, this_, promise, args...),
            BIND([promise, cancellationError = std::move(cancellationError)] {
                promise.Set(std::move(cancellationError));
            }));
        return promise;
    }

    static TTargetCallback Do(
        TSourceCallback this_,
        IInvokerPtr invoker)
    {
        return BIND(&Outer, std::move(this_), std::move(invoker));
    }

    static TTargetCallback DoGuarded(
        TSourceCallback this_,
        IInvokerPtr invoker,
        TError cancellationError)
    {
        return BIND(&OuterGuarded, std::move(this_), std::move(invoker), std::move(cancellationError));
    }
};

} // namespace NDetail

template <class R, class... TArgs>
TCallback<typename TFutureTraits<R>::TWrapped(TArgs...)>
TCallback<R(TArgs...)>::AsyncVia(IInvokerPtr invoker) const
{
    return NYT::NDetail::TAsyncViaHelper<R(TArgs...)>::Do(*this, std::move(invoker));
}

template <class R, class... TArgs>
TCallback<typename TFutureTraits<R>::TWrapped(TArgs...)>
TCallback<R(TArgs...)>::AsyncViaGuarded(IInvokerPtr invoker, TError cancellationError) const
{
    return NYT::NDetail::TAsyncViaHelper<R(TArgs...)>::DoGuarded(*this, std::move(invoker), std::move(cancellationError));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFutureHolder<T>::TFutureHolder(std::nullopt_t)
{ }

template <class T>
TFutureHolder<T>::TFutureHolder(TFuture<T> future)
    : Future_(std::move(future))
{ }

template <class T>
TFutureHolder<T>::~TFutureHolder()
{
    if (Future_) {
        Future_.Cancel(TError("Future holder destroyed"));
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

    void SetPromise(const TPromise<TResult>& promise)
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

    void SetPromise(const TPromise<TResult>& promise)
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

    void SetPromise(const TPromise<TResult>& promise)
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

    void SetPromise(const TPromise<TResult>& promise)
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
    std::atomic_flag Canceled_ = ATOMIC_FLAG_INIT;

    void CancelFutures(const TError& error)
    {
        if (Canceled_.test_and_set()) {
            return;
        }
        for (size_t index = 0; index < Futures_.size(); ++index) {
            Futures_[index].Cancel(error);
        }
    }

    virtual void OnFutureSet(int futureIndex, const TErrorOr<TItem>& result) = 0;
    virtual bool ShouldCompletePrematurely() = 0;

private:
    template <class T>
    static void SetPromisePrematurely(const TPromise<T>& promise)
    {
        promise.Set(T());
    }

    static void SetPromisePrematurely(const TPromise<void>& promise)
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
            TError error(result);
            this->Promise_.TrySet(error);
            this->CancelFutures(error);
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
            TError error(result);
            this->Promise_.TrySet(error);
            this->CancelFutures(error);
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
    if constexpr(std::is_same_v<T, void>) {
        if (size == 0) {
            return VoidFuture;
        }
        if (size == 1) {
            return std::move(futures[0]);
        }
    }
    return New<NDetail::TFutureCombiner<T, NDetail::TFutureCombinerVectorResultHolder<T>>>(std::move(futures), size)
        ->Run();
}

template <class K, class T>
TFuture<typename TFutureCombineTraits<T>::template TCombinedHashMap<K>> Combine(
    const THashMap<K, TFuture<T>>& futures)
{
    auto size = futures.size();
    if constexpr(std::is_same_v<T, void>) {
        if (size == 0) {
            return VoidFuture;
        }
        if (size == 1) {
            return THashMap<K, TFuture<void>>{*futures.begin()};
        }
    }

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
    YT_VERIFY(quorum >= 0);
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
    YT_VERIFY(concurrencyLimit >= 0);
    return New<NDetail::TBoundedConcurrencyRunner<T>>(std::move(callbacks), concurrencyLimit)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

//! A hasher for TAwaitable.
template <>
struct THash<NYT::TAwaitable>
{
    inline size_t operator () (const NYT::TAwaitable& awaitable) const
    {
        return THash<NYT::TIntrusivePtr<NYT::NDetail::TFutureStateBase>>()(awaitable.Impl_);
    }
};

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

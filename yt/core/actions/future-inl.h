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
#include <type_traits>

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

template <class F, class... As>
auto RunNoExcept(F&& functor, As&&... args) noexcept -> decltype(functor(std::forward<As>(args)...))
{
    return functor(std::forward<As>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, TFutureCallbackCookie MinCookie, TFutureCallbackCookie MaxCookie>
class TFutureCallbackList
{
public:
    static bool IsValidCookie(TFutureCallbackCookie cookie)
    {
        return cookie >= MinCookie && cookie <= MaxCookie;
    }

    TFutureCallbackCookie Add(T callback)
    {
        YT_ASSERT(callback);
        TFutureCallbackCookie cookie;
        if (SpareCookies_.empty()) {
            cookie = static_cast<TFutureCallbackCookie>(Callbacks_.size());
            Callbacks_.push_back(std::move(callback));
        } else {
            cookie = SpareCookies_.back();
            SpareCookies_.pop_back();
            YT_ASSERT(!Callbacks_[cookie]);
            Callbacks_[cookie] = std::move(callback);
        }
        cookie += MinCookie;
        YT_ASSERT(cookie <= MaxCookie);
        return cookie;
    }

    bool TryRemove(TFutureCallbackCookie cookie, TGuard<TSpinLock>* guard)
    {
        if (!IsValidCookie(cookie)) {
            return false;
        }
        cookie -= MinCookie;
        YT_ASSERT(cookie >= 0 && cookie < static_cast<int>(Callbacks_.size()));
        YT_ASSERT(Callbacks_[cookie]);
        SpareCookies_.push_back(cookie);
        auto callback = std::move(Callbacks_[cookie]);
        // Make sure callback is not being destroyed under spinlock.
        guard->Release();
        return true;
    }

    template <class... As>
    void RunAndClear(As&&... args)
    {
        for (const auto& callback : Callbacks_) {
            if (callback) {
                RunNoExcept(callback, std::forward<As>(args)...);
            }
        }
        Callbacks_.clear();
    }

    bool IsEmpty() const
    {
        return Callbacks_.empty();
    }

private:
    static constexpr int TypicalCount = 8;
    SmallVector<T, TypicalCount> Callbacks_;
    SmallVector<TFutureCallbackCookie, TypicalCount> SpareCookies_;
};

////////////////////////////////////////////////////////////////////////////////

class TCancelableStateBase
    : public TRefCountedBase
{
public:
    TCancelableStateBase(bool wellKnown, int cancelableRefCount)
        : WellKnown_(wellKnown)
        , CancelableRefCount_(cancelableRefCount)
    { }

    virtual ~TCancelableStateBase() noexcept = default;

    virtual bool Cancel(const TError& error) noexcept = 0;

    void RefCancelable()
    {
        if (WellKnown_) {
            return;
        }
        auto oldCount = CancelableRefCount_++;
        YT_ASSERT(oldCount > 0);
    }

    void UnrefCancelable()
    {
        if (WellKnown_) {
            return;
        }
        auto oldCount = CancelableRefCount_--;
        YT_ASSERT(oldCount > 0);
        if (oldCount == 1) {
            OnLastCancelableRefLost();
        }
    }

protected:
    const bool WellKnown_;

    //! Number of cancelables plus one if FutureRefCount_ > 0.
    std::atomic<int> CancelableRefCount_;

private:
    void OnLastCancelableRefLost()
    {
        delete this;
    }
};

Y_FORCE_INLINE void Ref(TCancelableStateBase* state)
{
    state->RefCancelable();
}

Y_FORCE_INLINE void Unref(TCancelableStateBase* state)
{
    state->UnrefCancelable();
}

////////////////////////////////////////////////////////////////////////////////

template <>
class TFutureState<void>
    : public TCancelableStateBase
{
public:
    using TVoidResultHandler = TCallback<void(const TError&)>;
    using TVoidResultHandlers = TFutureCallbackList<TVoidResultHandler, 0, (1ULL << 30) - 1>;

    using TCancelHandler = TCallback<void(const TError&)>;
    using TCancelHandlers = SmallVector<TCancelHandler, 8>;

    void RefFuture()
    {
        if (WellKnown_) {
            return;
        }
        auto oldCount = FutureRefCount_++;
        YT_ASSERT(oldCount > 0);
    }

    bool TryRefFuture()
    {
        if (WellKnown_) {
            return true;
        }
        auto oldCount = FutureRefCount_.load();
        while (true) {
            if (oldCount == 0) {
                return false;
            }
            auto newCount = oldCount + 1;
            if (FutureRefCount_.compare_exchange_weak(oldCount, newCount)) {
                return true;
            }
        }
    }

    void UnrefFuture()
    {
        if (WellKnown_) {
            return;
        }
        auto oldCount = FutureRefCount_--;
        YT_ASSERT(oldCount > 0);
        if (oldCount == 1) {
            OnLastFutureRefLost();
        }
    }

    void RefPromise()
    {
        YT_ASSERT(!WellKnown_);
        auto oldCount = PromiseRefCount_++;
        YT_ASSERT(oldCount > 0 && FutureRefCount_ > 0);
    }

    void UnrefPromise()
    {
        YT_ASSERT(!WellKnown_);
        auto oldCount = PromiseRefCount_--;
        YT_ASSERT(oldCount > 0);
        if (oldCount == 1) {
            OnLastPromiseRefLost();
        }
    }

    const TError& Get() const
    {
        WaitUntilSet();
        return ResultError_;
    }

    TError GetUnique()
    {
        return Get();
    }

    std::optional<TError> TryGet() const
    {
        if (!CheckIfSet()) {
            return std::nullopt;
        }
        return ResultError_;
    }

    std::optional<TError> TryGetUnique()
    {
        return TryGet();
    }

    void Set(const TError& error)
    {
        DoTrySet<true>(error);
    }

    bool TrySet(const TError& error)
    {
        // Fast path.
        if (Set_) {
            return false;
        }

        // Slow path.
        return DoTrySet<false>(error);
    }

    TFutureCallbackCookie Subscribe(TVoidResultHandler handler);
    void Unsubscribe(TFutureCallbackCookie cookie);

    virtual bool Cancel(const TError& error) noexcept override;

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
    bool TimedWait(TInstant deadline) const;

protected:
    //! Number of promises.
    std::atomic<int> PromiseRefCount_;
    //! Number of futures plus one if PromiseRefCount_ > 0.
    std::atomic<int> FutureRefCount_;

    //! Protects the following section of members.
    mutable TSpinLock SpinLock_;
    std::atomic<bool> Canceled_ = false;
    TError CancelationError_;
    std::atomic<bool> Set_;
    std::atomic<bool> AbandonedUnset_ = false;
    TError ResultError_;
    bool HasHandlers_ = false;
    TVoidResultHandlers VoidResultHandlers_;
    TCancelHandlers CancelHandlers_;
    mutable std::unique_ptr<NConcurrency::TEvent> ReadyEvent_;

    TFutureState(int promiseRefCount, int futureRefCount, int cancelableRefCount)
        : TCancelableStateBase(false, cancelableRefCount)
        , PromiseRefCount_(promiseRefCount)
        , FutureRefCount_(futureRefCount)
        , Set_(false)
    { }

    TFutureState(bool wellKnown, int promiseRefCount, int futureRefCount, int cancelableRefCount, TError&& error)
        : TCancelableStateBase(wellKnown, cancelableRefCount)
        , PromiseRefCount_(promiseRefCount)
        , FutureRefCount_(futureRefCount)
        , Set_(true)
        , ResultError_(std::move(error))
    { }

    void InstallAbandonedError();
    void InstallAbandonedError() const;

    virtual void ResetResult();
    virtual void SetResultError(const TError& error);
    virtual bool TrySetError(const TError& error);

    template <bool MustSet, class F>
    bool DoRunSetter(F setter)
    {
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
            RunNoExcept(setter);
            Set_ = true;
            canceled = Canceled_;
            readyEvent = ReadyEvent_.get();
        }

        if (readyEvent) {
            readyEvent->NotifyAll();
        }

        if (!canceled) {
            CancelHandlers_.clear();
        }

        VoidResultHandlers_.RunAndClear(ResultError_);

        return true;
    }

    template <bool MustSet>
    bool DoTrySet(const TError& error)
    {
        // Calling subscribers may release the last reference to this.
        TIntrusivePtr<TFutureState<void>> this_(this);

        return DoRunSetter<MustSet>([&] {
            SetResultError(error);
        });
    }

    virtual bool DoUnsubscribe(TFutureCallbackCookie cookie, TGuard<TSpinLock>* guard);

    void WaitUntilSet() const;
    bool CheckIfSet() const;

private:
    void OnLastFutureRefLost();
    void OnLastPromiseRefLost();
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFutureState
    : public TFutureState<void>
{
public:
    using TResultHandler = TCallback<void(const TErrorOr<T>&)>;
    using TResultHandlers = TFutureCallbackList<TResultHandler, (1ULL << 30), (1ULL << 31) - 1>;

    using TUniqueResultHandler = TCallback<void(TErrorOr<T>&&)>;

private:
    std::optional<TErrorOr<T>> Result_;
#ifndef NDEBUG
    mutable std::atomic<bool> ResultMovedOut_ = false;
#endif

    TResultHandlers ResultHandlers_;
    TUniqueResultHandler UniqueResultHandler_;


    template <bool MustSet, class U>
    bool DoTrySet(U&& value) noexcept
    {
        // Calling subscribers may release the last reference to this.
        TIntrusivePtr<TFutureState<void>> this_(this);

        if (!DoRunSetter<MustSet>([&] {
            Result_.emplace(std::forward<U>(value));
            if (!Result_->IsOK()) {
                ResultError_ = *Result_;
            }
        }))
        {
            return false;
        }

        ResultHandlers_.RunAndClear(GetResult());

        if (UniqueResultHandler_) {
            RunNoExcept(UniqueResultHandler_, GetUniqueResult());
            UniqueResultHandler_ = {};
        }

        return true;
    }


    const TErrorOr<T>& GetResult() const
    {
#ifndef NDEBUG
        YT_ASSERT(!ResultMovedOut_);
#endif
        YT_ASSERT(Result_);
        return *Result_;
    }

    const std::optional<TErrorOr<T>>& GetOptionalResult() const
    {
#ifndef NDEBUG
        YT_ASSERT(!ResultMovedOut_);
#endif
        return Result_;
    }

    TErrorOr<T> GetUniqueResult()
    {
#ifndef NDEBUG
        YT_ASSERT(!ResultMovedOut_.exchange(true));
#endif
        auto result = std::move(*Result_);
        Result_.reset();
        return result;
    }


    virtual bool TrySetError(const TError& error) override
    {
        return TrySet(error);
    }

    virtual void ResetResult() override
    {
        Result_.reset();
    }

    virtual void SetResultError(const TError& error) override
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);
        TFutureState<void>::SetResultError(error);
        Result_ = error;
    }

    virtual bool DoUnsubscribe(TFutureCallbackCookie cookie, TGuard<TSpinLock>* guard) override
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);
        return
            ResultHandlers_.TryRemove(cookie, guard) ||
            TFutureState<void>::DoUnsubscribe(cookie, guard);  
    }

protected:
    TFutureState(int promiseRefCount, int futureRefCount, int cancelableRefCount)
        : TFutureState<void>(promiseRefCount, futureRefCount, cancelableRefCount)
    { }

    TFutureState(bool wellKnown, int promiseRefCount, int futureRefCount, int cancelableRefCount, TErrorOr<T>&& value)
        : TFutureState<void>(wellKnown, promiseRefCount, futureRefCount, cancelableRefCount, TError(static_cast<const TError&>(value)))
        , Result_(value)
    { }

    TFutureState(bool wellKnown, int promiseRefCount, int futureRefCount, int cancelableRefCount, T&& value)
        : TFutureState<void>(wellKnown, promiseRefCount, futureRefCount, cancelableRefCount, TError())
        , Result_(std::move(value))
    { }

public:
    const TErrorOr<T>& Get() const
    {
        WaitUntilSet();
        return GetResult();
    }

    TErrorOr<T> GetUnique()
    {
        // Fast path.
        if (Set_) {
            return GetUniqueResult();
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                return GetUniqueResult();
            }
            if (!ReadyEvent_) {
                ReadyEvent_.reset(new NConcurrency::TEvent());
            }
        }

        ReadyEvent_->Wait();

        return GetUniqueResult();
    }

    std::optional<TErrorOr<T>> TryGet() const
    {
        if (!CheckIfSet()) {
            return std::nullopt;
        }
        return GetOptionalResult();
    }

    std::optional<TErrorOr<T>> TryGetUnique()
    {
        if (!CheckIfSet()) {
            return std::nullopt;
        }
        return GetUniqueResult();
    }

    template <class U>
    void Set(U&& value)
    {
        DoTrySet<true>(std::forward<U>(value));
    }

    template <class U>
    bool TrySet(U&& value)
    {
        // Fast path.
        if (Set_) {
            return false;
        }

        // Slow path.
        return DoTrySet<false>(std::forward<U>(value));
    }

    TFutureCallbackCookie Subscribe(TResultHandler handler)
    {
        // Fast path.
        if (Set_) {
            RunNoExcept(handler, GetResult());
            return NullFutureCallbackCookie;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                guard.Release();
                RunNoExcept(handler, GetResult());
                return NullFutureCallbackCookie;
            } else {
                HasHandlers_ = true;
                return ResultHandlers_.Add(std::move(handler));
            }
        }
    }

    void SubscribeUnique(TUniqueResultHandler handler)
    {
        // Fast path.
        if (Set_) {
            RunNoExcept(handler, GetUniqueResult());
            return;
        }

        // Slow path.
        {
            auto guard = Guard(SpinLock_);
            InstallAbandonedError();
            if (Set_) {
                guard.Release();
                RunNoExcept(handler, GetUniqueResult());
            } else {
                YT_ASSERT(!UniqueResultHandler_);
                YT_ASSERT(ResultHandlers_.IsEmpty());
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
    TPromiseState(int promiseRefCount, int futureRefCount, int cancelableRefCount)
        : TFutureState<T>(promiseRefCount, futureRefCount, cancelableRefCount)
    { }

    template <class U>
    TPromiseState(bool wellKnown, int promiseRefCount, int futureRefCount, int cancelableRefCount, U&& value)
        : TFutureState<T>(wellKnown, promiseRefCount, futureRefCount, cancelableRefCount, std::forward<U>(value))
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

    promise.OnCanceled(BIND([cancelable = this_.AsCancelable()] (const TError& error) {
        cancelable.Cancel(error);
    }));

    return promise;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPromise<T> NewPromise()
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(1, 1, 1));
}

template <class T>
TPromise<T> MakePromise(TErrorOr<T> value)
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(false, 1, 1, 1, std::move(value)));
}

template <class T>
TPromise<T> MakePromise(T value)
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(false, 1, 1, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeFuture(TErrorOr<T> value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(false, 0, 1, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeFuture(T value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(false, 0, 1, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeWellKnownFuture(TErrorOr<T> value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(true, -1, -1, -1, std::move(value)));
}

////////////////////////////////////////////////////////////////////////////////

inline bool operator==(const TCancelable& lhs, const TCancelable& rhs)
{
    return lhs.Impl_ == rhs.Impl_;
}

inline bool operator!=(const TCancelable& lhs, const TCancelable& rhs)
{
    return !(lhs == rhs);
}

inline void swap(TCancelable& lhs, TCancelable& rhs)
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

inline TCancelable::operator bool() const
{
    return Impl_.operator bool();
}

inline void TCancelable::Reset()
{
    Impl_.Reset();
}

inline bool TCancelable::Cancel(const TError& error) const
{
    YT_ASSERT(Impl_);
    return Impl_->Cancel(error);
}

inline TCancelable::TCancelable(TIntrusivePtr<NYT::NDetail::TCancelableStateBase> impl)
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
bool TFutureBase<T>::TimedWait(TInstant deadline) const
{
    YT_ASSERT(Impl_);
    return Impl_->TimedWait(deadline);
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
TFutureCallbackCookie TFutureBase<T>::Subscribe(TCallback<void(const TErrorOr<T>&)> handler) const
{
    YT_ASSERT(Impl_);
    return Impl_->Subscribe(std::move(handler));
}

template <class T>
void TFutureBase<T>::Unsubscribe(TFutureCallbackCookie cookie) const
{
    YT_ASSERT(Impl_);
    Impl_->Unsubscribe(cookie);
}

template <class T>
void TFutureBase<T>::SubscribeUnique(TCallback<void(TErrorOr<T>&&)> handler) const
{
    YT_ASSERT(Impl_);
    Impl_->SubscribeUnique(std::move(handler));
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

    static const auto NoopHandler = BIND([] (const TError&) { });
    promise.OnCanceled(NoopHandler);

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

    promise.OnCanceled(BIND([=, cancelable = AsCancelable()] (const TError& error) {
        cancelable.Cancel(error);
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

    auto promise = NewPromise<T>();

    auto cookie = NConcurrency::TDelayedExecutor::Submit(
        BIND([=, cancelable = AsCancelable()] (bool aborted) {
            TError error;
            if (aborted) {
                error = TError(NYT::EErrorCode::Canceled, "Operation aborted");
            } else {
                error = TError(NYT::EErrorCode::Timeout, "Operation timed out")
                    << TErrorAttribute("timeout", timeout);
            }
            promise.TrySet(error);
            cancelable.Cancel(error);
        }),
        timeout);

    Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
        NConcurrency::TDelayedExecutor::CancelAndClear(cookie);
        promise.TrySet(value);
    }));

    promise.OnCanceled(BIND([=, cancelable = AsCancelable()] (const TError& error) mutable {
        NConcurrency::TDelayedExecutor::CancelAndClear(cookie);
        cancelable.Cancel(error);
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
    if constexpr (std::is_same_v<U, void>) {
        return TFuture<void>(Impl_);
    }

    if (!Impl_) {
        return TFuture<U>();
    }

    auto promise = NewPromise<U>();

    Subscribe(BIND([=] (const TErrorOr<T>& value) {
        promise.Set(TErrorOr<U>(value));
    }));

    promise.OnCanceled(BIND([cancelable = AsCancelable()] (const TError& error) {
        cancelable.Cancel(error);
    }));

    return promise;
}

template <class T>
TFuture<void> TFutureBase<T>::AsVoid() const
{
    return TFuture<void>(Impl_);
}

template <class T>
TCancelable TFutureBase<T>::AsCancelable() const
{
    return TCancelable(Impl_);
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

    OnCanceled(BIND([anotherCancelable = another.AsCancelable()] (const TError& error) {
        anotherCancelable.Cancel(error);
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

    OnCanceled(BIND([anotherCancelable = another.AsCancelable()] (const TError& error) {
        anotherCancelable.Cancel(error);
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
class TFutureCombinerResultHolder
{
public:
    using TResult = std::vector<T>;

    explicit TFutureCombinerResultHolder(int size)
        : Result_(size)
    { }

    bool TrySetResult(int index, const TErrorOr<T>& errorOrValue)
    {
        if (errorOrValue.IsOK()) {
            Result_[index] = errorOrValue.Value();
            return true;
        } else {
            return false;
        }
    }

    bool TrySetPromise(const TPromise<TResult>& promise)
    {
        return promise.TrySet(std::move(Result_));
    }

private:
    TResult Result_;
};

template <class T>
class TFutureCombinerResultHolder<TErrorOr<T>>
{
public:
    using TResult = std::vector<TErrorOr<T>>;

    explicit TFutureCombinerResultHolder(int size)
        : Result_(size)
    { }

    bool TrySetResult(int index, const TErrorOr<T>& errorOrValue)
    {
        Result_[index] = errorOrValue;
        return true;
    }

    bool TrySetPromise(const TPromise<TResult>& promise)
    {
        return promise.TrySet(std::move(Result_));
    }

private:
    TResult Result_;
};

template <>
class TFutureCombinerResultHolder<void>
{
public:
    using TResult = void;

    explicit TFutureCombinerResultHolder(int /*size*/)
    { }

    bool TrySetResult(int /*index*/, const TError& error)
    {
        return error.IsOK();
    }

    bool TrySetPromise(const TPromise<TResult>& promise)
    {
        return promise.TrySet();
    }
};

template <class T>
class TFutureCombinerBase
    : public TRefCounted
{
protected:
    const std::vector<TFuture<T>> Futures_;
    
    explicit TFutureCombinerBase(std::vector<TFuture<T>> futures)
        : Futures_(std::move(futures))
    { }

    void CancelFutures(const TError& error)
    {
        for (const auto& future : Futures_) {
            future.Cancel(error);
        }
    }

    bool TryAcquireFuturesCancelLatch()
    {
        return !FuturesCancelLatch_.exchange(true);
    }

    void OnCanceled(const TError& error)
    {
        if (TryAcquireFuturesCancelLatch()) {
            CancelFutures(error);
        }
    }

private:
    std::atomic<bool> FuturesCancelLatch_ = false;
};

template <class T>
class TFutureCombinerWithSubscriptionBase
    : public TFutureCombinerBase<T>
{
protected:
    using TFutureCombinerBase<T>::TFutureCombinerBase;

    void RegisterSubscriptionCookies(std::vector<TFutureCallbackCookie>&& cookies)
    {
        SubscriptionCookies_ = std::move(cookies);
        YT_ASSERT(this->Futures_.size() == SubscriptionCookies_.size());
        MaybeUnsubscribeFromFutures();
    }

    void OnCombinerFinished()
    {
        MaybeUnsubscribeFromFutures();
    }

private:
    std::vector<TFutureCallbackCookie> SubscriptionCookies_;
    std::atomic<int> SubscriptionLatch_ = 0;

    void MaybeUnsubscribeFromFutures()
    {
        if (++SubscriptionLatch_ != 2) {
            return;
        }
        for (size_t index = 0; index < this->Futures_.size(); ++index) {
            this->Futures_[index].Unsubscribe(SubscriptionCookies_[index]);
        }
    }
};

template <class T>
class TAnyFutureCombiner
    : public TFutureCombinerWithSubscriptionBase<T>
{
public:
    TAnyFutureCombiner(
        std::vector<TFuture<T>> futures,
        bool skipErrors,
        TFutureCombinerOptions options)
        : TFutureCombinerWithSubscriptionBase<T>(std::move(futures))
        , SkipErrors_(skipErrors)
        , Options_(options)
    { }

    TFuture<T> Run()
    {
        if (this->Futures_.empty()) {
            return MakeFuture<T>(TError(
                NYT::EErrorCode::FutureCombinerFailure,
                "Any-of combiner failure: empty input"));
        }

        std::vector<TFutureCallbackCookie> subscriptionCookies;
        subscriptionCookies.reserve(this->Futures_.size());
        for (const auto& future : this->Futures_) {
            subscriptionCookies.push_back(future.Subscribe(BIND(&TAnyFutureCombiner::OnFutureSet, MakeStrong(this))));
        }
        this->RegisterSubscriptionCookies(std::move(subscriptionCookies));

        if (Options_.PropagateCancelationToInput) {
            Promise_.OnCanceled(BIND(&TAnyFutureCombiner::OnCanceled, MakeWeak(this)));
        }

        return Promise_;
    }

private:
    const bool SkipErrors_;
    const TFutureCombinerOptions Options_;
    const TPromise<T> Promise_ = NewPromise<T>();

    TSpinLock ErrorsLock_;
    std::vector<TError> Errors_;

    void OnFutureSet(const TErrorOr<T>& result)
    {
        if (SkipErrors_ && !result.IsOK()) {
            RegisterError(result);
            return;
        }

        if (Promise_.TrySet(result)) {
            this->OnCombinerFinished();
        }

        if (Options_.CancelInputOnShortcut &&
            this->Futures_.size() > 1 &&
            this->TryAcquireFuturesCancelLatch())
        {
            this->CancelFutures(TError(
                NYT::EErrorCode::FutureCombinerShortcut,
                "Any-of combiner shortcut: some response received"));
        }
    }

    void RegisterError(const TError& error)
    {
        auto guard = Guard(ErrorsLock_);

        Errors_.push_back(error);

        if (Errors_.size() < this->Futures_.size()) {
            return;
        }

        auto combinerError = TError(
            NYT::EErrorCode::FutureCombinerFailure,
            "Any-of combiner failure: all responses have failed")
            << Errors_;

        guard.Release();

        if (Promise_.TrySet(combinerError)) {
            this->OnCombinerFinished();
        }
    }
};

template <class T, class TResultHolder>
class TAllFutureCombiner
    : public TFutureCombinerBase<T>
{
public:
    TAllFutureCombiner(
        std::vector<TFuture<T>> futures,
        TFutureCombinerOptions options)
        : TFutureCombinerBase<T>(std::move(futures))
        , Options_(options)
        , ResultHolder_(this->Futures_.size())
    { }

    TFuture<typename TResultHolder::TResult> Run()
    {
        if (this->Futures_.empty()) {
            return MakeFuture<typename TResultHolder::TResult>({});
        }

        for (int index = 0; index < static_cast<int>(this->Futures_.size()); ++index) {
            this->Futures_[index].Subscribe(BIND(&TAllFutureCombiner::OnFutureSet, MakeStrong(this), index));
        }

        if (Options_.PropagateCancelationToInput) {
            Promise_.OnCanceled(BIND(&TAllFutureCombiner::OnCanceled, MakeWeak(this)));
        }

        return Promise_;
    }

private:
    const TFutureCombinerOptions Options_;
    const TPromise<typename TResultHolder::TResult> Promise_ = NewPromise<typename TResultHolder::TResult>();

    TResultHolder ResultHolder_;

    std::atomic<int> ResponseCount_ = 0;

    void OnFutureSet(int index, const TErrorOr<T>& result)
    {
        if (!ResultHolder_.TrySetResult(index, result)) {
            TError error(result);
            Promise_.TrySet(error);

            if (Options_.CancelInputOnShortcut && 
                this->Futures_.size() > 1 &&
                this->TryAcquireFuturesCancelLatch())
            {
                this->CancelFutures(TError(
                    NYT::EErrorCode::FutureCombinerShortcut,
                    "All-of combiner shortcut: some response failed")
                    << error);
            }

            return;
        }

        if (++ResponseCount_ == static_cast<int>(this->Futures_.size())) {
            ResultHolder_.TrySetPromise(Promise_);
        }
    }
};

template <class T, class TResultHolder>
class TAnyNFutureCombiner
    : public TFutureCombinerWithSubscriptionBase<T>
{
public:
    TAnyNFutureCombiner(
        std::vector<TFuture<T>> futures,
        int n,
        bool skipErrors,
        TFutureCombinerOptions options)
        : TFutureCombinerWithSubscriptionBase<T>(std::move(futures))
        , Options_(options)
        , N_(n)
        , SkipErrors_(skipErrors)
        , ResultHolder_(n)
    {
        YT_VERIFY(N_ >= 0);
    }

    TFuture<typename TResultHolder::TResult> Run()
    {
        if (N_ == 0) {
            if (Options_.CancelInputOnShortcut && !this->Futures_.empty()) {
                this->CancelFutures(TError(
                    NYT::EErrorCode::FutureCombinerShortcut,
                    "Any-N-of combiner shortcut: no responses needed"));
            }

            return MakeFuture<typename TResultHolder::TResult>({});
        }

        if (static_cast<int>(this->Futures_.size()) < N_) {
            if (Options_.CancelInputOnShortcut) {
                this->CancelFutures(TError(
                    NYT::EErrorCode::FutureCombinerShortcut,
                    "Any-N-of combiner shortcut: too few inputs given"));
            }

            return MakeFuture<typename TResultHolder::TResult>(TError(
                NYT::EErrorCode::FutureCombinerFailure,
                "Any-N-of combiner failure: %v responses needed, %v inputs given",
                N_,
                this->Futures_.size()));
        }

        std::vector<TFutureCallbackCookie> subscriptionCookies;
        subscriptionCookies.reserve(this->Futures_.size());
        for (int index = 0; index < static_cast<int>(this->Futures_.size()); ++index) {
            subscriptionCookies.push_back(this->Futures_[index].Subscribe(
                BIND(&TAnyNFutureCombiner::OnFutureSet, MakeStrong(this), index)));
        }
        this->RegisterSubscriptionCookies(std::move(subscriptionCookies));

        if (Options_.PropagateCancelationToInput) {
            Promise_.OnCanceled(BIND(&TAnyNFutureCombiner::OnCanceled, MakeWeak(this)));
        }

        return Promise_;
    }

private:
    const TFutureCombinerOptions Options_;
    const int N_;
    const bool SkipErrors_;
    const TPromise<typename TResultHolder::TResult> Promise_ = NewPromise<typename TResultHolder::TResult>();

    TResultHolder ResultHolder_;

    std::atomic<int> ResponseCount_ = 0;

    TSpinLock ErrorsLock_;
    std::vector<TError> Errors_;

    void OnFutureSet(int /*index*/, const TErrorOr<T>& result)
    {
        if (SkipErrors_ && !result.IsOK()) {
            RegisterError(result);
            return;
        }

        int responseIndex = ResponseCount_++;
        if (responseIndex >= N_) {
            return;
        }

        if (!ResultHolder_.TrySetResult(responseIndex, result)) {
            TError error(result);
            if (Promise_.TrySet(error)) {
                this->OnCombinerFinished();
            }

            if (Options_.CancelInputOnShortcut &&
                this->Futures_.size() > 1 &&
                this->TryAcquireFuturesCancelLatch())
            {
                this->CancelFutures(TError(
                    NYT::EErrorCode::FutureCombinerShortcut,
                    "Any-N-of combiner shortcut: some input failed"));
            }
            return;
        }

        if (responseIndex == N_ - 1) {
            if (ResultHolder_.TrySetPromise(Promise_)) {
                this->OnCombinerFinished();
            }

            if (Options_.CancelInputOnShortcut &&
               responseIndex < static_cast<int>(this->Futures_.size()) - 1 &&
               this->TryAcquireFuturesCancelLatch())
            {
                this->CancelFutures(TError(
                    NYT::EErrorCode::FutureCombinerShortcut,
                    "Any-N-of combiner shortcut: enough responses received"));
            }
        }
    }

    void RegisterError(const TError& error)
    {
        auto guard = Guard(ErrorsLock_);

        Errors_.push_back(error);

        auto totalCount = static_cast<int>(this->Futures_.size());
        auto failedCount = static_cast<int>(Errors_.size());
        if (totalCount - failedCount >= N_) {
            return;
        }

        auto combinerError = TError(
            NYT::EErrorCode::FutureCombinerFailure,
            "Any-N-of combiner failure: %v responses needed, %v failed, %v inputs given",
            N_,
            failedCount,
            totalCount)
            << Errors_;

        guard.Release();

        if (Promise_.TrySet(combinerError)) {
            this->OnCombinerFinished();
        }

        if (Options_.CancelInputOnShortcut &&
           this->TryAcquireFuturesCancelLatch())
        {
            this->CancelFutures(TError(
                NYT::EErrorCode::FutureCombinerShortcut,
                "Any-N-of combiner shortcut: one of responses failed")
                << error);
        }
    }
};

} // namespace NDetail

template <class T>
TFuture<T> AnySucceded(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options)
{
    if (futures.size() == 1) {
        return std::move(futures[0]);
    }
    return New<NDetail::TAnyFutureCombiner<T>>(std::move(futures), true, options)
        ->Run();
}

template <class T>
TFuture<T> AnySet(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options)
{
    return New<NDetail::TAnyFutureCombiner<T>>(std::move(futures), false, options)
        ->Run();
}

template <class T>
TFuture<typename TFutureCombinerTraits<T>::TCombinedVector> AllSucceeded(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options)
{
    auto size = futures.size();
    if constexpr (std::is_same_v<T, void>) {
        if (size == 0) {
            return VoidFuture;
        }
        if (size == 1) {
            return std::move(futures[0]);
        }
    }
    using TResultHolder = NDetail::TFutureCombinerResultHolder<T>;
    return New<NDetail::TAllFutureCombiner<T, TResultHolder>>(std::move(futures), options)
        ->Run();
}

template <class T>
TFuture<std::vector<TErrorOr<T>>> AllSet(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options)
{
    using TResultHolder = NDetail::TFutureCombinerResultHolder<TErrorOr<T>>;
    return New<NDetail::TAllFutureCombiner<T, TResultHolder>>(std::move(futures), options)
        ->Run();
}

template <class T>
TFuture<typename TFutureCombinerTraits<T>::TCombinedVector> AnyNSucceeded(
    std::vector<TFuture<T>> futures,
    int n,
    TFutureCombinerOptions options)
{
    auto size = futures.size();
    if constexpr (std::is_same_v<T, void>) {
        if (size == 1 && n == 1) {
            return std::move(futures[0]);
        }
    }
    using TResultHolder = NDetail::TFutureCombinerResultHolder<T>;
    return New<NDetail::TAnyNFutureCombiner<T, TResultHolder>>(std::move(futures), n, true, options)
        ->Run();
}

template <class T>
TFuture<std::vector<TErrorOr<T>>> AnyNSet(
    std::vector<TFuture<T>> futures,
    int n,
    TFutureCombinerOptions options)
{
    using TResultHolder = NDetail::TFutureCombinerResultHolder<TErrorOr<T>>;
    return New<NDetail::TAnyNFutureCombiner<T, TResultHolder>>(std::move(futures), n, false, options)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////
// COMPAT(babenko)

template <class T>
TFuture<typename TFutureCombinerTraits<T>::TCombinedVector> Combine(std::vector<TFuture<T>> futures)
{
    return AllSucceeded(std::move(futures));
}

template <class T>
TFuture<typename TFutureCombinerTraits<T>::TCombinedVector> CombineQuorum(std::vector<TFuture<T>> futures, int quorum)
{
    return AnyNSucceeded(std::move(futures), quorum);
}

template <class T>
TFuture<std::vector<TErrorOr<T>>> CombineAll(std::vector<TFuture<T>> futures)
{
    return AllSet(std::move(futures));
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
    const TPromise<std::vector<TErrorOr<T>>> Promise_ = NewPromise<std::vector<TErrorOr<T>>>();

    std::vector<TErrorOr<T>> Results_;
    std::atomic<int> CurrentIndex_;
    std::atomic<int> FinishedCount_ = 0;


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

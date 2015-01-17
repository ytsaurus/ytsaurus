#ifndef FUTURE_INL_H_
#error "Direct inclusion of this file is not allowed, include future.h"
#endif
#undef FUTURE_INL_H_

#include "bind.h"

#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/event_count.h>

#include <core/misc/small_vector.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declrations

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
    typedef TCallback<void(const TErrorOr<T>&)> TResultHandler;
    typedef SmallVector<TResultHandler, 8> TResultHandlers;

    typedef TClosure TCancelHandler;
    typedef SmallVector<TCancelHandler, 8> TCancelHandlers;

private:
    class THolder
        : private ::TNonCopyable
    {
    public:
        explicit THolder(TFutureState* state)
            : State_(state)
        {
            State_->RefFuture();
        }

        ~THolder()
        {
            State_->UnrefFuture();
        }

    private:
        TFutureState* const State_;

    };

    //! Number of promises.
    std::atomic<int> StrongRefCount_;
    //! Number of futures plus one if there's at least one promise.
    std::atomic<int> WeakRefCount_;

    //! Protects the following section of members.
    mutable TSpinLock SpinLock_;
    std::atomic<bool> Canceled_;
    std::atomic<bool> Set_;
    TNullable<TErrorOr<T>> Value_;
    mutable std::unique_ptr<NConcurrency::TEvent> ReadyEvent_;
    TResultHandlers ResultHandlers_;
    TCancelHandlers CancelHandlers_;


    template <class U, bool MustSet>
    bool DoSet(U&& value)
    {
        // Calling subscribers may release the last reference to this.
        THolder holder(this);

        NConcurrency::TEvent* readyEvent = nullptr;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (MustSet) {
                YCHECK(!Set_);
            } else {
                if (Set_) {
                    return false;
                }
            }
            Value_.Assign(std::forward<U>(value));
            Set_ = true;
            readyEvent = ReadyEvent_.get();
        }

        if (readyEvent) {
            readyEvent->NotifyAll();
        }

        for (const auto& handler : ResultHandlers_) {
            handler.Run(*Value_);
        }

        ResultHandlers_.clear();
        CancelHandlers_.clear();

        return true;
    }


    void Dispose()
    {
        // Check for fast path.
        if (Set_) {
            // Just kill the fake weak reference.
            UnrefFuture();
        } else {
            GetFinalizerInvoker()->Invoke(BIND([=] () {
                // Set the promise if the value is still missing.
                TrySet(TError(NYT::EErrorCode::Canceled, "Promise abandoned"));
                // Kill the fake weak reference.
                UnrefFuture();
            }));
        }
    }

    void Destroy()
    {
        delete this;
    }

protected:
    TFutureState(int strongRefCount, int weakRefCount)
    {
        // TODO(babenko): VS compat
        StrongRefCount_ = strongRefCount;
        WeakRefCount_ = weakRefCount;
        Set_ = false;
        Canceled_ = false;
    }

    template <class U>
    TFutureState(int strongRefCount, int weakRefCount, U&& value)
        : Value_(std::forward<U>(value))
    {
        // TODO(babenko): VS compat
        StrongRefCount_ = strongRefCount;
        WeakRefCount_ = weakRefCount;
        Set_ = true;
        Canceled_ = false;
    }

public:
    void RefFuture()
    {
        auto oldWeakCount = WeakRefCount_++;
        YASSERT(oldWeakCount > 0);
    }

    void UnrefFuture()
    {
        auto oldWeakCount = WeakRefCount_--;
        YASSERT(oldWeakCount > 0);
        if (oldWeakCount == 1) {
            Destroy();
        }
    }

    void RefPromise()
    {
        auto oldStrongCount = StrongRefCount_++;
        YASSERT(oldStrongCount > 0 && WeakRefCount_ > 0);
    }

    void UnrefPromise()
    {
        auto oldStrongCount = StrongRefCount_--;
        YASSERT(oldStrongCount > 0);

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
            TGuard<TSpinLock> guard(SpinLock_);
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

    TNullable<TErrorOr<T>> TryGet() const
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
        // Calling subscribers may release the last reference to this.
        THolder holder(this);

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
    TPromiseState(int strongRefCount, int weakRefCount, U&& value)
        : TFutureState<T>(strongRefCount, weakRefCount, std::forward<U>(value))
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

template <class R, class T, class... TArgs>
struct TPromiseSetter<T, R(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<T>& promise, const TCallback<T(TArgs...)>& callback, TCallArgs&&... args)
    {
        try {
            promise.Set(callback.Run(std::forward<TCallArgs>(args)...));
        } catch (const std::exception& ex) {
            promise.Set(TError(ex));
        }
    }
};

template <class... TArgs>
struct TPromiseSetter<void, void(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<void>& promise, const TCallback<void(TArgs...)>& callback, TCallArgs&&... args)
    {
        try {
            callback.Run(std::forward<TCallArgs>(args)...);
            promise.Set();
        } catch (const std::exception& ex) {
            promise.Set(TError(ex));
        }
    }
};

template <class T, class... TArgs>
struct TPromiseSetter<T, TFuture<T>(TArgs...)>
{
    template <class... TCallArgs>
    static void Do(TPromise<T>& promise, const TCallback<TFuture<T>(TArgs...)>& callback, TCallArgs&&... args)
    {
        try {
            promise.SetFrom(callback.Run(std::forward<TCallArgs>(args)...));
        } catch (const std::exception& ex) {
            promise.Set(TError(ex));
        }
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
    YASSERT(this_);

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
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(1, 1, std::move(value)));
}

template <class T>
TPromise<T> MakePromise(T value)
{
    return TPromise<T>(New<NYT::NDetail::TPromiseState<T>>(1, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeFuture(TErrorOr<T> value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(0, 1, std::move(value)));
}

template <class T>
TFuture<T> MakeFuture(T value)
{
    return TFuture<T>(New<NYT::NDetail::TPromiseState<T>>(0, 1, std::move(value)));
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
    return Impl_ != nullptr;
}

template <class T>
void TFutureBase<T>::Reset()
{
    Impl_.Reset();
}

template <class T>
bool TFutureBase<T>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
bool TFutureBase<T>::IsCanceled() const
{
    YASSERT(Impl_);
    return Impl_->IsCanceled();
}

template <class T>
const TErrorOr<T>& TFutureBase<T>::Get() const
{
    YASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
TNullable<TErrorOr<T>> TFutureBase<T>::TryGet() const
{
    YASSERT(Impl_);
    return Impl_->TryGet();
}

template <class T>
void TFutureBase<T>::Subscribe(TCallback<void(const TErrorOr<T>&)> handler)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(handler));
}

template <class T>
bool TFutureBase<T>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

template <class T>
TFuture<T> TFutureBase<T>::WithTimeout(TDuration timeout)
{
    YASSERT(Impl_);

    auto promise = NewPromise<T>();

    Subscribe(BIND([=] (const TErrorOr<T>& value) mutable {
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
template <class R>
TFuture<R> TFutureBase<T>::Apply(TCallback<R(const TErrorOr<T>&)> callback)
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
    return Impl_ != nullptr;
}

template <class T>
void TPromiseBase<T>::Reset()
{
    Impl_.Reset();
}

template <class T>
bool TPromiseBase<T>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

template <class T>
void TPromiseBase<T>::Set(const TErrorOr<T>& value)
{
    YASSERT(Impl_);
    Impl_->Set(value);
}

template <class T>
void TPromiseBase<T>::Set(TErrorOr<T>&& value)
{
    YASSERT(Impl_);
    Impl_->Set(std::move(value));
}

template <class T>
template <class U>
void TPromiseBase<T>::SetFrom(TFuture<U> another)
{
    YASSERT(Impl_);

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
    YASSERT(Impl_);
    return Impl_->TrySet(value);
}

template <class T>
bool TPromiseBase<T>::TrySet(TErrorOr<T>&& value)
{
    YASSERT(Impl_);
    return Impl_->TrySet(std::move(value));
}

template <class T>
template <class U>
inline void TPromiseBase<T>::TrySetFrom(TFuture<U> another)
{
    YASSERT(Impl_);

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
    YASSERT(Impl_);
    return Impl_->Get();
}

template <class T>
TNullable<TErrorOr<T>> TPromiseBase<T>::TryGet() const
{
    YASSERT(Impl_);
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
    YASSERT(Impl_);
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
    YASSERT(this->Impl_);
    this->Impl_->Set(value);
}

template <class T>
void TPromise<T>::Set(T&& value)
{
    YASSERT(this->Impl_);
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
    YASSERT(this->Impl_);
    return this->Impl_->TrySet(value);
}

template <class T>
bool TPromise<T>::TrySet(T&& value)
{
    YASSERT(this->Impl_);
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
    YASSERT(this->Impl_);
    this->Impl_->Set(TError());
}

inline bool TPromise<void>::TrySet()
{
    YASSERT(this->Impl_);
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
TCallback<R(TArgs...)>::AsyncVia(IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<R(TArgs...)>::Do(*this, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {


template <class T>
class TFutureCombinerResultHolder
{
public:
    explicit TFutureCombinerResultHolder(int size)
        : Result_(size)
    { }

    void SetItem(int index, const TErrorOr<T>& value)
    {
        Result_[index] = value.Value();
    }

    void SetPromise(TPromise<std::vector<T>>& promise)
    {
        promise.Set(std::move(Result_));
    }

private:
    std::vector<T> Result_;

};

template <>
class TFutureCombinerResultHolder<void>
{
public:
    explicit TFutureCombinerResultHolder(int /*size*/)
    { }

    void SetItem(int /*index*/, const TError& /*value*/)
    { }

    void SetPromise(TPromise<void>& promise)
    {
        promise.Set();
    }
};

template <class T>
class TFutureCombiner
    : public TRefCounted
{
public:
    TFutureCombiner(std::vector<TFuture<T>> items)
        : Items_(std::move(items))
        , ResultHolder_(Items_.size())
        , PendingResponseCount_(Items_.size())
        , ErrorResponseCount_(0)
    { }

    TFuture<typename TFutureCombineTraits<T>::TCombined> Run()
    {
        if (Items_.empty()) {
            ResultHolder_.SetPromise(Promise_);
        } else {
            for (int index = 0; index < Items_.size(); ++index) {
                Items_[index].Subscribe(BIND(&TFutureCombiner::OnSet, MakeStrong(this), index));
            }
            Promise_.OnCanceled(BIND(&TFutureCombiner::OnCanceled, MakeWeak(this)));
        }
        return Promise_;
    }

private:
    std::vector<TFuture<T>> Items_;

    TPromise<typename TFutureCombineTraits<T>::TCombined> Promise_ = NewPromise<typename TFutureCombineTraits<T>::TCombined>();
    TFutureCombinerResultHolder<T> ResultHolder_;
    std::atomic<int> PendingResponseCount_;
    std::atomic<int> ErrorResponseCount_;
    TError Error_;


    void OnCanceled()
    {
        for (int index = 0; index < Items_.size(); ++index) {
            Items_[index].Cancel();
        }
    }

    void OnSet(int index, const TErrorOr<T>& result)
    {
        if (result.IsOK()) {
            ResultHolder_.SetItem(index, result);
        } else {
            if (++ErrorResponseCount_ == 1) {
                Error_ = result;
            }
            OnCanceled();
        }
        if (--PendingResponseCount_ == 0) {
            if (Error_.IsOK()) {
                ResultHolder_.SetPromise(Promise_);
            } else {
                Promise_.Set(Error_);
            }
        }
    }
};

} // namespace NDetail

template <class T>
TFuture<typename TFutureCombineTraits<T>::TCombined>
Combine(std::vector<TFuture<T>> items)
{
    return New<NDetail::TFutureCombiner<T>>(std::move(items))->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

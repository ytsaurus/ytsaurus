#include "stdafx.h"
#include "future.h"

#include <core/concurrency/delayed_executor.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// #TPromiseState<void>

namespace NDetail {

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
    mutable TSpinLock SpinLock_;
    std::atomic<bool> Canceled_;
    std::atomic<bool> Set_;
    mutable std::unique_ptr<Event> ReadyEvent_;
    TResultHandlers ResultHandlers_;
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
                YCHECK(!Set_);
            } else {
                if (Set_) {
                    return false;
                }
            }
            Set_ = true;
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
    TPromiseState(bool set = false)
    {
        // TODO(babenko): VS compat
        Set_ = set;
        Canceled_ = false;
    }

    ~TPromiseState()
    {
        DoCancel();
    }

    bool IsSet() const
    {
        return Set_;
    }

    bool IsCanceled() const
    {
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
        // Fast path.
        if (Set_)
            return;

        // Slow path.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Set_)
                return;
            if (!ReadyEvent_) {
                ReadyEvent_.reset(new Event());
            }
        }

        ReadyEvent_->Wait();
    }

    void Subscribe(TResultHandler onResult)
    {
        // Fast path.
        if (Set_) {
            onResult.Run();
            return;
        }

        // Slow path.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Set_) {
                guard.Release();
                onResult.Run();
            } else if (!Canceled_) {
                ResultHandlers_.push_back(std::move(onResult));
            }
        }
    }

    void OnCanceled(TCancelHandler onCancel)
    {
        // Fast path.
        if (Canceled_) {
            onCancel.Run();
            return;
        }

        // Slow path.
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Canceled_) {
                guard.Release();
                onCancel.Run();
            } else if (!Set_) {
                CancelHandlers_.push_back(std::move(onCancel));
            }
        }
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

            if (Set_) {
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

DEFINE_REFCOUNTED_TYPE(TPromiseState<void>)

template <>
struct TPromiseSetter<void>
{
    static void Do(TPromise<void> promise)
    {
        promise.Set();
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////
// #TPromise<void>

TPromise<void>::TPromise()
    : Impl_(nullptr)
{ }

TPromise<void>::TPromise(TNull)
    : Impl_(nullptr)
{ }

TPromise<void>::operator bool() const
{
    return Impl_ != nullptr;
}

void TPromise<void>::Reset()
{
    Impl_.Reset();
}

void TPromise<void>::Swap(TPromise& other)
{
    Impl_.Swap(other.Impl_);
}

bool TPromise<void>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

void TPromise<void>::Set()
{
    YASSERT(Impl_);
    Impl_->Set();
}

bool TPromise<void>::TrySet()
{
    YASSERT(Impl_);
    return Impl_->TrySet();
}

void TPromise<void>::Get() const
{
    YASSERT(Impl_);
    Impl_->Get();
}

void TPromise<void>::Subscribe(TClosure onResult)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(onResult));
}

void TPromise<void>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(onCancel));
}

bool TPromise<void>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

TFuture<void> TPromise<void>::ToFuture() const
{
    return TFuture<void>(Impl_);
}

// XXX(sandello): Kill this method.
TPromise<void>::operator TFuture<void>() const
{
    return TFuture<void>(Impl_);
}

TPromise<void>::TPromise(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<void>>& state)
    : Impl_(state)
{ }

TPromise<void>::TPromise(
    TIntrusivePtr< NYT::NDetail::TPromiseState<void>>&& state)
    : Impl_(std::move(state))
{ }

////////////////////////////////////////////////////////////////////////////////
// #TFuture<void>

TFuture<void>::TFuture()
    : Impl_(nullptr)
{ }

TFuture<void>::TFuture(TNull)
    : Impl_(nullptr)
{ }

TFuture<void>::operator bool() const
{
    return Impl_ != nullptr;
}

void TFuture<void>::Reset()
{
    Impl_.Reset();
}

void TFuture<void>::Swap(TFuture& other)
{
    Impl_.Swap(other.Impl_);
}

bool TFuture<void>::IsSet() const
{
    YASSERT(Impl_);
    return Impl_->IsSet();
}

bool TFuture<void>::IsCanceled() const
{
    YASSERT(Impl_);
    return Impl_->IsCanceled();
}

void TFuture<void>::Get() const
{
    YASSERT(Impl_);
    Impl_->Get();
}

void TFuture<void>::Subscribe(TClosure onResult)
{
    YASSERT(Impl_);
    return Impl_->Subscribe(std::move(onResult));
}

void TFuture<void>::OnCanceled(TClosure onCancel)
{
    YASSERT(Impl_);
    Impl_->OnCanceled(std::move(onCancel));
}

bool TFuture<void>::Cancel()
{
    YASSERT(Impl_);
    return Impl_->Cancel();
}

TFuture<void> TFuture<void>::Apply(TCallback<void()> mutator)
{
    auto mutated = NewPromise<void>();

    Subscribe(BIND([=] () mutable {
        mutator.Run();
        mutated.Set();
    }));

    OnCanceled(BIND([=] () mutable {
        mutated.Cancel();
    }));

    return mutated;
}

TFuture<void> TFuture<void>::Apply(TCallback<TFuture<void>()> mutator)
{
    auto mutated = NewPromise<void>();

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

TFuture<void> TFuture<void>::Finally()
{
    auto promise = NewPromise<void>();
    Subscribe(BIND([=] () mutable { promise.Set(); }));
    OnCanceled(BIND([=] () mutable { promise.Set(); }));
    return promise;
}

TFuture<TError> TFuture<void>::WithTimeout(TDuration timeout)
{
    auto promise = NewPromise<TError>();
    Subscribe(BIND([=] () mutable {
        promise.TrySet(TError());
    }));
    OnCanceled(BIND([=] () mutable {
        promise.Cancel();
    }));
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () mutable {
            promise.TrySet(TError(NYT::EErrorCode::Timeout, "Future has timed out"));
        }),
        timeout);
    return promise;
}

TFuture<void>::TFuture(
    const TIntrusivePtr< NYT::NDetail::TPromiseState<void>>& state)
    : Impl_(state)
{ }

TFuture<void>::TFuture(
    TIntrusivePtr< NYT::NDetail::TPromiseState<void>>&& state)
    : Impl_(std::move(state))
{ }

///////////////////////////////////////////////////////////////////////////////

TFuture<void> VoidFuture  = MakeFuture();
TFuture<bool> TrueFuture = MakeFuture<bool>(true);
TFuture<bool> FalseFuture = MakeFuture<bool>(false);

///////////////////////////////////////////////////////////////////////////////

template <>
TPromise<void> NewPromise<void>()
{
    return TPromise<void>(New< NYT::NDetail::TPromiseState<void> >(false));
}

TPromise<void> NewPromise()
{
    return NewPromise<void>();
}

TPromise<void> MakePromise()
{
    return TPromise<void>(New< NYT::NDetail::TPromiseState<void> >(true));
}

TFuture<void> MakeFuture()
{
    return TFuture<void>(New< NYT::NDetail::TPromiseState<void>>(true));
}

TFuture<void> MakeDelayed(TDuration delay)
{
    auto promise = NewPromise<void>();
    NConcurrency::TDelayedExecutor::Submit(
        BIND([=] () mutable { promise.Set(); }),
        delay);
    return promise;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

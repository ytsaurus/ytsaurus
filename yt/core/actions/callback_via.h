#pragma once

#include "invoker.h"
#include "future.h"
#include "invoker_util.h"

namespace NYT {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

// Implemented in fibers/fiber.cpp
namespace NConcurrency {
namespace NDetail {

TClosure GetCurrentFiberCanceler();

} // namespace NDetail
} // namespace NConcurrency

namespace NDetail {

template <bool WrappedInFuture, class TSignature>
struct TAsyncViaHelper;

template <class T>
void RegisterFiberCancelation(TPromise<T> promise)
{
    auto canceler = NConcurrency::NDetail::GetCurrentFiberCanceler();
    auto invoker = GetCurrentInvoker();
    promise.OnCanceled(BIND(
        IgnoreResult(&IInvoker::Invoke),
        invoker,
        canceler));
}

} // namespace NDetail

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

namespace NDetail {

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
        auto outer = BIND([=] (TArgs... args) -> FR {
            auto promise = NewPromise<R>();
            auto future = promise.ToFuture();
            auto result = invoker->Invoke(BIND(
                inner,
                promise,
                std::forward<TArgs>(args)...));
            if (!result) {
                future.Cancel();
            }
            return future;
        });
        return outer;
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
        auto outer = BIND([=] (TArgs... args) -> FR {
            auto promise = NewPromise<R>();
            auto future = promise.ToFuture();
            bool result = invoker->Invoke(BIND(
                inner,
                promise,
                std::forward<TArgs>(args)... ));
            if (!result) {
                future.Cancel();
            }
            return future;
        });
        return outer;
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
            this_.Run(std::forward<TArgs>(args)...)
                .Subscribe(BIND(&TPromiseSetter<R>::Do, promise));
        });
        auto outer = BIND([=] (TArgs... args) -> FR {
            auto promise = NewPromise<R>();
            auto future = promise.ToFuture();
            bool result = invoker->Invoke(BIND(
                inner,
                promise,
                std::forward<TArgs>(args)...));
            if (!result) {
                future.Cancel();
            }
            return future;
        });
        return outer;
    }
};

} // namespace NDetail

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
/*! \endinternal */
} // namespace NYT

// WARNING: This file was auto-generated.
// Please, consider incorporating any changes into the generator.

// Generated on Wed Oct 16 13:07:52 2013.


#pragma once
#ifndef CALLBACK_VIA_H_
#error "Direct inclusion of this file is not allowed, include callback.h"
#endif
#undef CALLBACK_VIA_H_

#include "invoker.h"
#include "future.h"
#include "invoker_util.h"

namespace NYT {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <bool WrappedInFuture, class TSignature>
struct TAsyncViaHelper;

// Implemented in fibers/fiber.cpp
TClosure GetCurrentFiberCanceler();

template <class T>
void RegisterFiberCancelation(TPromise<T> promise)
{
    auto canceler = GetCurrentFiberCanceler();
    auto invoker = GetCurrentInvoker();
    promise.OnCanceled(BIND(
        IgnoreResult(&IInvoker::Invoke),
        invoker,
        canceler));
}

} // namespace NDetail
// === Arity 0.
template <class R>
TCallback<R()>
TCallback<R()>::Via(IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] () {
        invoker->Invoke(BIND(this_));
    });
}

namespace NDetail
{

template <bool W, class U>
struct TAsyncViaHelper<W, U()>
{
    typedef TCallback<U()> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        ()> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run());
        });
        auto outer = BIND([=] () -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W>
struct TAsyncViaHelper<W, void()>
{
    typedef TCallback<void()> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        ()> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise) {
            RegisterFiberCancelation(promise);
            this_.Run();
            promise.Set();
        });
        auto outer = BIND([=] () -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U>
struct TAsyncViaHelper<true, U()>
{
    typedef TCallback<U()> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        ()> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise) {
            RegisterFiberCancelation(promise);
            this_.Run()
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] () -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U>
TCallback<typename NYT::NDetail::TFutureHelper<U>::TFutureType()>
TCallback<U()>::AsyncVia(IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U()
    >::Do(*this, std::move(invoker));
}
// === Arity 1.
template <class R, class A1>
TCallback<R(A1)>
TCallback<R(A1)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1>
struct TAsyncViaHelper<W, U(A1)>
{
    typedef TCallback<U(A1)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1)));
        });
        auto outer = BIND([=] (A1 a1) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1>
struct TAsyncViaHelper<W, void(A1)>
{
    typedef TCallback<void(A1)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1>
struct TAsyncViaHelper<true, U(A1)>
{
    typedef TCallback<U(A1)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1)>
TCallback<U(A1)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1)
    >::Do(*this, std::move(invoker));
}
// === Arity 2.
template <class R, class A1, class A2>
TCallback<R(A1, A2)>
TCallback<R(A1, A2)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1, A2 a2) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1),
            std::forward<A2>(a2)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1, class A2>
struct TAsyncViaHelper<W, U(A1, A2)>
{
    typedef TCallback<U(A1, A2)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2)));
        });
        auto outer = BIND([=] (A1 a1, A2 a2) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1, class A2>
struct TAsyncViaHelper<W, void(A1, A2)>
{
    typedef TCallback<void(A1, A2)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1, A2)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1, A2 a2) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1, class A2>
struct TAsyncViaHelper<true, U(A1, A2)>
{
    typedef TCallback<U(A1, A2)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1, A2 a2) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1, A2 a2) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1, class A2>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1, A2)>
TCallback<U(A1, A2)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1, A2)
    >::Do(*this, std::move(invoker));
}
// === Arity 3.
template <class R, class A1, class A2, class A3>
TCallback<R(A1, A2, A3)>
TCallback<R(A1, A2, A3)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1, A2 a2, A3 a3) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1),
            std::forward<A2>(a2),
            std::forward<A3>(a3)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1, class A2, class A3>
struct TAsyncViaHelper<W, U(A1, A2, A3)>
{
    typedef TCallback<U(A1, A2, A3)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3)));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1, class A2, class A3>
struct TAsyncViaHelper<W, void(A1, A2, A3)>
{
    typedef TCallback<void(A1, A2, A3)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1, A2, A3)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1, class A2, class A3>
struct TAsyncViaHelper<true, U(A1, A2, A3)>
{
    typedef TCallback<U(A1, A2, A3)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1, class A2, class A3>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1, A2, A3)>
TCallback<U(A1, A2, A3)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1, A2, A3)
    >::Do(*this, std::move(invoker));
}
// === Arity 4.
template <class R, class A1, class A2, class A3, class A4>
TCallback<R(A1, A2, A3, A4)>
TCallback<R(A1, A2, A3, A4)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1),
            std::forward<A2>(a2),
            std::forward<A3>(a3),
            std::forward<A4>(a4)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1, class A2, class A3, class A4>
struct TAsyncViaHelper<W, U(A1, A2, A3, A4)>
{
    typedef TCallback<U(A1, A2, A3, A4)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4)));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1, class A2, class A3, class A4>
struct TAsyncViaHelper<W, void(A1, A2, A3, A4)>
{
    typedef TCallback<void(A1, A2, A3, A4)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1, A2, A3, A4)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1, class A2, class A3, class A4>
struct TAsyncViaHelper<true, U(A1, A2, A3, A4)>
{
    typedef TCallback<U(A1, A2, A3, A4)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1, class A2, class A3, class A4>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1, A2, A3, A4)>
TCallback<U(A1, A2, A3, A4)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1, A2, A3, A4)
    >::Do(*this, std::move(invoker));
}
// === Arity 5.
template <class R, class A1, class A2, class A3, class A4, class A5>
TCallback<R(A1, A2, A3, A4, A5)>
TCallback<R(A1, A2, A3, A4, A5)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1),
            std::forward<A2>(a2),
            std::forward<A3>(a3),
            std::forward<A4>(a4),
            std::forward<A5>(a5)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1, class A2, class A3, class A4, class A5>
struct TAsyncViaHelper<W, U(A1, A2, A3, A4, A5)>
{
    typedef TCallback<U(A1, A2, A3, A4, A5)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4, A5)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4,
            A5 a5) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5)));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1, class A2, class A3, class A4, class A5>
struct TAsyncViaHelper<W, void(A1, A2, A3, A4, A5)>
{
    typedef TCallback<void(A1, A2, A3, A4, A5)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1, A2, A3, A4, A5)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1, class A2, class A3, class A4, class A5>
struct TAsyncViaHelper<true, U(A1, A2, A3, A4, A5)>
{
    typedef TCallback<U(A1, A2, A3, A4, A5)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4, A5)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1, class A2, class A3, class A4, class A5>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1, A2, A3, A4, A5)>
TCallback<U(A1, A2, A3, A4, A5)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1, A2, A3, A4, A5)
    >::Do(*this, std::move(invoker));
}
// === Arity 6.
template <class R, class A1, class A2, class A3, class A4, class A5, class A6>
TCallback<R(A1, A2, A3, A4, A5, A6)>
TCallback<R(A1, A2, A3, A4, A5, A6)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1),
            std::forward<A2>(a2),
            std::forward<A3>(a3),
            std::forward<A4>(a4),
            std::forward<A5>(a5),
            std::forward<A6>(a6)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1, class A2, class A3, class A4, class A5,
    class A6>
struct TAsyncViaHelper<W, U(A1, A2, A3, A4, A5, A6)>
{
    typedef TCallback<U(A1, A2, A3, A4, A5, A6)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4, A5, A6)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5,
            A6 a6) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6)));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1, class A2, class A3, class A4, class A5, class A6>
struct TAsyncViaHelper<W, void(A1, A2, A3, A4, A5, A6)>
{
    typedef TCallback<void(A1, A2, A3, A4, A5, A6)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1, A2, A3, A4, A5, A6)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5,
            A6 a6) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1, class A2, class A3, class A4, class A5, class A6>
struct TAsyncViaHelper<true, U(A1, A2, A3, A4, A5, A6)>
{
    typedef TCallback<U(A1, A2, A3, A4, A5, A6)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4, A5, A6)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5,
            A6 a6) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1, class A2, class A3, class A4, class A5, class A6>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1, A2, A3, A4, A5, A6)>
TCallback<U(A1, A2, A3, A4, A5, A6)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1, A2, A3, A4, A5, A6)
    >::Do(*this, std::move(invoker));
}
// === Arity 7.
template <class R, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
TCallback<R(A1, A2, A3, A4, A5, A6, A7)>
TCallback<R(A1, A2, A3, A4, A5, A6, A7)>::Via(
    IInvokerPtr invoker)
{
    static_assert(NMpl::TIsVoid<R>::Value,
        "Via() can be used with void return type only.");
    YASSERT(invoker);

    auto this_ = *this;
    return BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7) {
        invoker->Invoke(BIND(this_,
            std::forward<A1>(a1),
            std::forward<A2>(a2),
            std::forward<A3>(a3),
            std::forward<A4>(a4),
            std::forward<A5>(a5),
            std::forward<A6>(a6),
            std::forward<A7>(a7)));
    });
}

namespace NDetail
{

template <bool W, class U, class A1, class A2, class A3, class A4, class A5,
    class A6, class A7>
struct TAsyncViaHelper<W, U(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef TCallback<U(A1, A2, A3, A4, A5, A6, A7)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4, A5, A6, A7)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5,
            A6 a6, A7 a7) -> void {
            RegisterFiberCancelation(promise);
            promise.Set(this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6),
                std::forward<A7>(a7)));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6,
            A7 a7) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6),
                std::forward<A7>(a7)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <bool W, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
struct TAsyncViaHelper<W, void(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef TCallback<void(A1, A2, A3, A4, A5, A6, A7)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<void>::TFutureType
        (A1, A2, A3, A4, A5, A6, A7)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<void>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<void>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<void>::TPromiseType PR;

        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5,
            A6 a6, A7 a7) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6),
                std::forward<A7>(a7));
            promise.Set();
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6,
            A7 a7) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<void>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6),
                std::forward<A7>(a7)));
            return promise.ToFuture();
        });
        return outer;
    }
};

template <class U, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
struct TAsyncViaHelper<true, U(A1, A2, A3, A4, A5, A6, A7)>
{
    typedef TCallback<U(A1, A2, A3, A4, A5, A6, A7)> TSourceCallback;
    typedef TCallback<
        typename NYT::NDetail::TFutureHelper<U>::TFutureType
        (A1, A2, A3, A4, A5, A6, A7)> TTargetCallback;

    static inline TTargetCallback Do(
        const TSourceCallback& this_,
        IInvokerPtr invoker)
    {
        // XXX(babenko): Due to MSVC bug in lambda scope resolution
        // the following typedefs are essentially useless and we have to
        // write full type names inside lambdas.
        // XXX(sandello): Burn, MSVC!
        typedef typename NYT::NDetail::TFutureHelper<U>::TValueType R;
        typedef typename NYT::NDetail::TFutureHelper<U>::TFutureType FR;
        typedef typename NYT::NDetail::TFutureHelper<U>::TPromiseType PR;

        static auto promiseSetter = BIND(&TPromiseSetter<R>::Do);
        auto inner = BIND([=] (PR promise, A1 a1, A2 a2, A3 a3, A4 a4, A5 a5,
            A6 a6, A7 a7) {
            RegisterFiberCancelation(promise);
            this_.Run(
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6),
                std::forward<A7>(a7))
                .Subscribe(BIND(promiseSetter, promise));
        });
        auto outer = BIND([=] (A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6,
            A7 a7) -> FR {
            auto promise = NewPromise<
                typename NYT::NDetail::TFutureHelper<U>::TValueType
            >();
            invoker->Invoke(BIND(inner, promise,
                std::forward<A1>(a1),
                std::forward<A2>(a2),
                std::forward<A3>(a3),
                std::forward<A4>(a4),
                std::forward<A5>(a5),
                std::forward<A6>(a6),
                std::forward<A7>(a7)));
            return promise;
        });
        return outer;
    }
};

} // namespace NDetail
template <class U, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
TCallback<
    typename NYT::NDetail::TFutureHelper<U>::TFutureType
    (A1, A2, A3, A4, A5, A6, A7)>
TCallback<U(A1, A2, A3, A4, A5, A6, A7)>::AsyncVia(
    IInvokerPtr invoker)
{
    return NYT::NDetail::TAsyncViaHelper<
        NYT::NDetail::TFutureHelper<U>::WrappedInFuture,
        U(A1, A2, A3, A4, A5, A6, A7)
    >::Do(*this, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NYT

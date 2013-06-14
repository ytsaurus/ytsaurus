#ifndef ASYNC_PIPELINE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_pipeline.h"
#endif
#undef ASYNC_PIPELINE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class Signature>
struct TAsyncPipelineSignatureCracker
{ };

template <class T1, class T2>
struct TAsyncPipelineSignatureCracker<T1(T2)>
{
    typedef T1 TReturn;
    typedef T2 TArg;
};

template <class T1>
struct TAsyncPipelineSignatureCracker<T1()>
{
    typedef T1 TReturn;
    typedef void TArg;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TErrorOrHelpers
{
    typedef TErrorOr<T> TWrapped;
    typedef T TValue;

    static TWrapped Wrapper(T value)
    {
        return TErrorOr<T>(value);
    }
};

template <>
struct TErrorOrHelpers<void>
{
    typedef TError TWrapped;
    typedef void TValue;

    static TWrapped Wrapper()
    {
        return TError();
    }
};

template <class T>
struct TErrorOrHelpers< TErrorOr<T> >
{
    typedef TErrorOr<T> TWrapped;
    typedef T TValue;

    static TWrapped Wrapper(TErrorOr<T> error)
    {
        return error;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TArg, class TReturn>
struct TAsyncPipelineHelpers
{
    typedef typename TErrorOrHelpers<TReturn>::TWrapped TWrapped;

    static TFuture<TWrapped> Wrapper(TCallback<TReturn(TArg)> func, TErrorOr<TArg> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TWrapped(TError(x)));
        }

        try {
            auto&& y = func.Run(x.GetValue());
            return MakeFuture(TWrapped(std::forward<TReturn>(y)));
        } catch (const std::exception& ex) {
            return MakeFuture(TWrapped(ex));
        }
    }
};

template <class TArg, class TReturn>
struct TAsyncPipelineHelpers< TArg, TFuture<TReturn> >
{
    typedef typename TErrorOrHelpers<TReturn>::TWrapped TWrapped;

    static TFuture<TWrapped> Wrapper(TCallback<TFuture<TReturn>(TArg)> func, TErrorOr<TArg> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TWrapped(TError(x)));
        }

        try {
            auto&& y = func.Run(x.GetValue());
            return y.Apply(BIND(&TErrorOrHelpers<TReturn>::Wrapper));
        } catch (const std::exception& ex) {
            return MakeFuture(TWrapped(ex));
        }
    }
};

template <class TArg>
struct TAsyncPipelineHelpers<TArg, void>
{
    typedef TError TWrapped;

    static TFuture<TWrapped> Wrapper(TCallback<void(TArg)> func, TErrorOr<TArg> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TWrapped(TError(x)));
        }

        try {
            func.Run(x.GetValue());
            return MakeFuture(TWrapped());
        } catch (const std::exception& ex) {
            return MakeFuture(TWrapped(ex));
        }
    }
};

template <>
struct TAsyncPipelineHelpers<void, void>
{
    typedef TErrorOr<void> TWrapped;

    static TFuture<TWrapped> Wrapper(TCallback<void(void)> func, TErrorOr<void> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TWrapped(TError(x)));
        }

        try {
            func.Run();
            return MakeFuture(TWrapped());
        } catch (const std::exception& ex) {
            return MakeFuture(TWrapped(ex));
        }
    }
};

template <class TReturn>
struct TAsyncPipelineHelpers< void, TFuture<TReturn> >
{
    typedef typename TErrorOrHelpers<TReturn>::TWrapped TWrapped;

    static TFuture<TWrapped> Wrapper(TCallback<TFuture<TReturn>()> func, TError x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TWrapped(TError(x)));
        }

        try {
            auto&& y = func.Run();
            return y.Apply(BIND(&TErrorOrHelpers<TReturn>::Wrapper));
        } catch (const std::exception& ex) {
            return MakeFuture(TWrapped(ex));
        }
    }
};

template <class TReturn>
struct TAsyncPipelineHelpers<void, TReturn>
{
    typedef typename TErrorOrHelpers<TReturn>::TWrapped TWrapped;

    static TFuture<TWrapped> Wrapper(TCallback<TReturn()> func, TError x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TWrapped(TError(x)));
        }

        try {
            return MakeFuture(TWrapped(func.Run()));
        } catch (const std::exception& ex) {
            return MakeFuture(TWrapped(ex));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAsyncPipeline<T>::TAsyncPipeline(
    IInvokerPtr invoker,
    TCallback< TFuture< TErrorOr<T> >() > lazy)
    : Invoker(invoker)
    , Lazy(lazy)
{ }

template <class T>
TFuture< TErrorOr<T> > TAsyncPipeline<T>::Run()
{
    return Lazy.Run();
}

template <class T>
template <class Signature>
TIntrusivePtr<
    TAsyncPipeline<
        typename TErrorOrHelpers<
            typename NYT::NDetail::TFutureHelper<
                typename TAsyncPipelineSignatureCracker<Signature>::TReturn
            >::TValueType
        >::TValue
    >
>
TAsyncPipeline<T>::Add(TCallback<Signature> func, IInvokerPtr invoker)
{
    typedef typename TAsyncPipelineSignatureCracker<Signature>::TReturn TReturn;
    typedef typename TAsyncPipelineSignatureCracker<Signature>::TArg TArg;
    typedef decltype(Add(func)) TResult;

    auto wrappedFunc = BIND(&TAsyncPipelineHelpers<TArg, TReturn>::Wrapper, func);

    if (invoker) {
        wrappedFunc = wrappedFunc.AsyncVia(invoker);
    }

    auto lazy = Lazy;
    auto newLazy = BIND([=] () {
        return lazy.Run().Apply(wrappedFunc);
    });

    return New<typename TResult::TUnderlying>(Invoker, newLazy);
}

template <class T>
template <class Signature>
TIntrusivePtr<
    TAsyncPipeline<
        typename TErrorOrHelpers<
            typename NYT::NDetail::TFutureHelper<
                typename TAsyncPipelineSignatureCracker<Signature>::TReturn
            >::TValueType
        >::TValue
    >
>
TAsyncPipeline<T>::Add(TCallback<Signature> func)
{
    return Add(func, Invoker);
}

inline TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvokerPtr invoker)
{
    return New< TAsyncPipeline<void> >(
        invoker,
        BIND([=] () {
            return MakeFuture(TError());
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

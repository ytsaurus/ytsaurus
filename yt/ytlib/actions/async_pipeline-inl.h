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
    typedef T1 ReturnType;
    typedef T2 ArgType;
};

template <class T1>
struct TAsyncPipelineSignatureCracker<T1()>
{
    typedef T1 ReturnType;
    typedef void ArgType;
};

template <class T>
struct TValueOrErrorHelpers
{
    typedef TValueOrError<T> TWrappedType;
    typedef T TValueType;

    static TWrappedType Wrapper(T value)
    {
        return TValueOrError<T>(value);
    }
};

template <>
struct TValueOrErrorHelpers<void>
{
    typedef TValueOrError<void> TWrappedType;
    typedef void TValueType;

    static TWrappedType Wrapper()
    {
        return TValueOrError<void>();
    }
};

template <class T>
struct TValueOrErrorHelpers< TValueOrError<T> >
{
    typedef TValueOrError<T> TWrappedType;
    typedef T TValueType;

    static TWrappedType Wrapper(TValueOrError<T> error)
    {
        return error;
    }
};

template <class ArgType, class ReturnType>
struct TAsyncPipelineHelpers
{
    typedef typename TValueOrErrorHelpers<ReturnType>::TWrappedType WrappedReturnType;

    static TFuture<WrappedReturnType> Wrapper(TCallback<ReturnType(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(WrappedReturnType(TError(x)));
        }

        try {
            auto&& y = func.Run(x.Value());
            return MakeFuture(WrappedReturnType(ForwardRV<ReturnType>(y)));
        } catch (const std::exception& ex) {
            return MakeFuture(WrappedReturnType(ex));
        }
    }
};

template <class ArgType, class ReturnType>
struct TAsyncPipelineHelpers< ArgType, TFuture<ReturnType> >
{
    typedef typename TValueOrErrorHelpers<ReturnType>::TWrappedType WrappedReturnType;

    static TFuture<WrappedReturnType> Wrapper(TCallback<TFuture<ReturnType>(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(WrappedReturnType(TError(x)));
        }

        try {
            auto&& y = func.Run(x.Value());
            return y.Apply(BIND(&TValueOrErrorHelpers<ReturnType>::Wrapper));
        } catch (const std::exception& ex) {
            return MakeFuture(WrappedReturnType(ex));
        }
    }
};

template <class ArgType>
struct TAsyncPipelineHelpers<ArgType, void>
{
    typedef TValueOrError<void> WrappedReturnType;

    static TFuture<WrappedReturnType> Wrapper(TCallback<void(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(WrappedReturnType(TError(x)));
        }

        try {
            func.Run(x.Value());
            return MakeFuture(WrappedReturnType());
        } catch (const std::exception& ex) {
            return MakeFuture(WrappedReturnType(ex));
        }
    }
};

template <>
struct TAsyncPipelineHelpers<void, void>
{
    typedef TValueOrError<void> WrappedReturnType;

    static TFuture<WrappedReturnType> Wrapper(TCallback<void(void)> func, TValueOrError<void> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(WrappedReturnType(TError(x)));
        }

        try {
            func.Run();
            return MakeFuture(WrappedReturnType());
        } catch (const std::exception& ex) {
            return MakeFuture(WrappedReturnType(ex));
        }
    }
};

template <class ReturnType>
struct TAsyncPipelineHelpers< void, TFuture<ReturnType> >
{
    typedef typename TValueOrErrorHelpers<ReturnType>::TWrappedType WrappedReturnType;

    static TFuture<WrappedReturnType> Wrapper(TCallback<TFuture<ReturnType>()> func, TValueOrError<void> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(WrappedReturnType(TError(x)));
        }

        try {
            auto&& y = func.Run();
            return y.Apply(BIND(&TValueOrErrorHelpers<ReturnType>::Wrapper));
        } catch (const std::exception& ex) {
            return MakeFuture(WrappedReturnType(ex));
        }
    }
};

template <class T>
TAsyncPipeline<T>::TAsyncPipeline(
    IInvokerPtr invoker,
    TCallback< TFuture< TValueOrError<T> >() > head)
    : Invoker(invoker)
    , Lazy(head)
{ }

template <class T>
TFuture< TValueOrError<T> > TAsyncPipeline<T>::Run()
{
    return Lazy.Run();
}

template <class T>
template <class Signature>
TIntrusivePtr<
    TAsyncPipeline<
        typename TValueOrErrorHelpers<
            typename NYT::NDetail::TFutureHelper<
                typename TAsyncPipelineSignatureCracker<Signature>::ReturnType
            >::TValueType
        >::TValueType
    >
>
TAsyncPipeline<T>::Add(TCallback<Signature> func)
{
    typedef typename TAsyncPipelineSignatureCracker<Signature>::ReturnType ReturnType;
    typedef typename TAsyncPipelineSignatureCracker<Signature>::ArgType ArgType;
    typedef decltype(Add(func)) ResultType;

    auto wrappedFunc = BIND(&TAsyncPipelineHelpers<ArgType, ReturnType>::Wrapper, func);

    if (Invoker) {
        wrappedFunc = wrappedFunc.AsyncVia(Invoker);
    }

    auto lazy = Lazy;
    auto newLazy = BIND([=] () {
        return lazy.Run().Apply(wrappedFunc);
    });

    return New<typename ResultType::TElementType>(Invoker, newLazy);
}

inline TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvokerPtr invoker)
{
    return New< TAsyncPipeline<void> >(
        invoker,
        BIND([=] () {
            return MakeFuture(TValueOrError<void>());
    }));
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYT

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
    static TValueOrError<T> Wrapper(T value)
    {
        return TValueOrError<T>(value);
    }
};

template <>
struct TValueOrErrorHelpers<void>
{
    static TValueOrError<void> Wrapper()
    {
        return TValueOrError<void>();
    }
};

template <class ArgType, class ReturnType>
struct TAsyncPipelineHelpers
{
    static TFuture< TValueOrError<ReturnType> > Wrapper(TCallback<ReturnType(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<ReturnType>(TError(x)));
        }

        try {
            auto&& y = func.Run(x.Value());
            return MakeFuture(TValueOrError<ReturnType>(ForwardRV<ReturnType>(y)));
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<ReturnType>(TError(ex.what())));
        }
    }
};

template <class ArgType, class ReturnType>
struct TAsyncPipelineHelpers< ArgType, TFuture<ReturnType> >
{
    static TFuture< TValueOrError<ReturnType> > Wrapper(TCallback<TFuture<ReturnType>(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<ReturnType>(TError(x)));
        }

        try {
            auto&& y = func.Run(x.Value());
            return y.Apply(BIND(&TValueOrErrorHelpers<ReturnType>::Wrapper));
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<ReturnType>(TError(ex.what())));
        }
    }
};

template <class ArgType>
struct TAsyncPipelineHelpers<ArgType, void>
{
    static TFuture< TValueOrError<void> > Wrapper(TCallback<void(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<void>(TError(x)));
        }

        try {
            func.Run(x.Value());
            return MakeFuture(TValueOrError<void>());
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<void>(TError(ex.what())));
        }
    }
};

template <>
struct TAsyncPipelineHelpers<void, void>
{
    static TFuture< TValueOrError<void> > Wrapper(TCallback<void(void)> func, TValueOrError<void> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<void>(TError(x)));
        }

        try {
            func.Run();
            return MakeFuture(TValueOrError<void>());
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<void>(TError(ex.what())));
        }
    }
};

template <class ReturnType>
struct TAsyncPipelineHelpers< void, TFuture<ReturnType> >
{
    static TFuture< TValueOrError<ReturnType> > Wrapper(TCallback<TFuture<ReturnType>()> func, TValueOrError<void> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<ReturnType>(TError(x)));
        }

        try {
            auto&& y = func.Run();
            return y.Apply(BIND(&TValueOrErrorHelpers<ReturnType>::Wrapper));
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<ReturnType>(TError(ex.what())));
        }
    }
};

template <class T>
TAsyncPipeline<T>::TAsyncPipeline(
    IInvoker::TPtr invoker,
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
TIntrusivePtr< TAsyncPipeline< typename NYT::NDetail::TFutureHelper< typename TAsyncPipelineSignatureCracker<Signature>::ReturnType >::TValueType > >
TAsyncPipeline<T>::Add(TCallback<Signature> func)
{
    typedef typename TAsyncPipelineSignatureCracker<Signature>::ReturnType ReturnType;
    typedef typename TAsyncPipelineSignatureCracker<Signature>::ArgType ArgType;

    auto wrappedFunc = BIND(&TAsyncPipelineHelpers<ArgType, ReturnType>::Wrapper, func);

    if (Invoker) {
        wrappedFunc = wrappedFunc.AsyncVia(Invoker);
    }

    auto lazy = Lazy;
    auto newLazy = BIND([=] () {
        return lazy.Run().Apply(wrappedFunc);
    });

    return New< TAsyncPipeline<typename NYT::NDetail::TFutureHelper<ReturnType>::TValueType> >(Invoker, newLazy);
}

inline TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvoker::TPtr invoker)
{
    return New< TAsyncPipeline<void> >(
        invoker,
        BIND([=] () {
            return MakeFuture(TValueOrError<void>());
    }));
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYT

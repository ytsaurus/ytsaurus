#pragma once

#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class Signature>
struct TAsyncPipelineSignatureCracker;

template <class T>
struct TErrorOrHelpers;

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TAsyncPipeline> TPtr;

    TAsyncPipeline(
        IInvokerPtr invoker,
        TCallback< TFuture< TErrorOr<T> >() > lazy);

    TFuture< TErrorOr<T> > Run();

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
    Add(TCallback<Signature> func);

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
    Add(TCallback<Signature> func, IInvokerPtr invoker);

private:
    IInvokerPtr Invoker;
    TCallback< TFuture< TErrorOr<T> >() > Lazy;

};

TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_PIPELINE_INL_H_
#include "async_pipeline-inl.h"
#undef ASYNC_PIPELINE_INL_H_

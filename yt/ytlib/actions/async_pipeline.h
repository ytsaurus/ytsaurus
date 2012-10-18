#pragma once

#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class Signature>
struct TAsyncPipelineSignatureCracker;

template <class T>
struct TValueOrErrorHelpers;

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TAsyncPipeline> TPtr;

    TAsyncPipeline(
        IInvokerPtr invoker,
        TCallback< TFuture< TValueOrError<T> >() > head);

    TFuture< TValueOrError<T> > Run();

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
    Add(TCallback<Signature> func);

private:
    IInvokerPtr Invoker;
    TCallback< TFuture< TValueOrError<T> >() > Lazy;

};

TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_PIPELINE_INL_H_
#include "async_pipeline-inl.h"
#undef ASYNC_PIPELINE_INL_H_

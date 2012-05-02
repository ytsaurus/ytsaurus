#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class Signature>
struct TAsyncPipelineSignatureCracker;

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TAsyncPipeline> TPtr;

    TAsyncPipeline(
        IInvoker::TPtr invoker,
        TCallback< TFuture< TValueOrError<T> >() > head);

    TFuture< TValueOrError<T> > Run();

    template <class Signature>
    TIntrusivePtr< TAsyncPipeline< typename NYT::NDetail::TFutureHelper< typename TAsyncPipelineSignatureCracker<Signature>::ReturnType >::TValueType > >
    Add(TCallback<Signature> func);

private:
    IInvoker::TPtr Invoker;
    TCallback< TFuture< TValueOrError<T> >() > Lazy;

};

TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvoker::TPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_PIPELINE_INL_H_
#include "async_pipeline-inl.h"
#undef ASYNC_PIPELINE_INL_H_

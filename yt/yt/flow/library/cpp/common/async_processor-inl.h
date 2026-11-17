#pragma once

#ifndef ASYNC_PROCESSOR_INL_H_
    #error "Direct inclusion of this file is not allowed, include async_processor.h"
    // For the sake of sane code completion.
    #include "async_processor.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CYsonMessage TTask, CYsonMessage TResult>
TAsyncProcessorHandle<TTask, TResult>::TAsyncProcessorHandle(TStringBuf name, TStart start, TResultHandler resultHandler)
    : Descriptor_(New<TAsyncProcessorDescriptor>())
{
    THROW_ERROR_EXCEPTION_UNLESS(start, "Start callback of async processor %Qv is null", name);
    THROW_ERROR_EXCEPTION_UNLESS(resultHandler, "Result handler of async processor %Qv is null", name);
    Descriptor_->Name = name;
    Descriptor_->Start = BIND_NO_PROPAGATE([start = std::move(start)] (TYsonMessagePtr task) {
        return start(StaticPointerCast<TTask>(std::move(task))).template As<TYsonMessagePtr>();
    });
    Descriptor_->ResultHandler = BIND_NO_PROPAGATE(
        [resultHandler = std::move(resultHandler)] (const TUntypedCompletedAsyncTaskPtr& completedTask, IOutputCollectorPtr output) {
            resultHandler(TCompletedAsyncTask<TTask, TResult>(completedTask), std::move(output));
        });
}

template <CYsonMessage TTask, CYsonMessage TResult>
const TAsyncProcessorDescriptorPtr& TAsyncProcessorHandle<TTask, TResult>::GetDescriptor() const
{
    THROW_ERROR_EXCEPTION_UNLESS(Descriptor_,
        "Async processor handle is not initialized; see IJobInitContext::InitAsyncProcessor()");
    return Descriptor_;
}

////////////////////////////////////////////////////////////////////////////////

template <CYsonMessage TTask, CYsonMessage TResult>
TCompletedAsyncTask<TTask, TResult>::TCompletedAsyncTask(TUntypedCompletedAsyncTaskPtr untyped)
    : Untyped_(std::move(untyped))
    , Result_(Untyped_->Result.IsOK()
            ? TErrorOr<TIntrusivePtr<TResult>>(StaticPointerCast<TResult>(Untyped_->Result.Value()))
            : TErrorOr<TIntrusivePtr<TResult>>(TError(Untyped_->Result)))
{ }

template <CYsonMessage TTask, CYsonMessage TResult>
const TTask& TCompletedAsyncTask<TTask, TResult>::GetTask() const
{
    return static_cast<const TTask&>(*Untyped_->Task);
}

template <CYsonMessage TTask, CYsonMessage TResult>
const TErrorOr<TIntrusivePtr<TResult>>& TCompletedAsyncTask<TTask, TResult>::GetResult() const
{
    return Result_;
}

template <CYsonMessage TTask, CYsonMessage TResult>
const TInputTimerConstPtr& TCompletedAsyncTask<TTask, TResult>::GetRootTimer() const
{
    return Untyped_->RootTimer;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

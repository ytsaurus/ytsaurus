#pragma once

#include "public.h"

#include "timer.h"
#include "yson_message.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Starts |task| and returns its result. Runs on the serialized invoker of the job and must not block.
using TAsyncProcessorStart = TCallback<TFuture<TYsonMessagePtr>(TYsonMessagePtr task)>;

//! Processes |completedTask| and writes to |output|, whose parent is the root timer of the task.
using TAsyncProcessorResultHandler = TCallback<void(const TUntypedCompletedAsyncTaskPtr& completedTask, IOutputCollectorPtr output)>;

//! An async processor of a computation: runs the tasks of #IOutputCollector::Add() and passes their results
//! to #ResultHandler. Its limits are in #TDynamicAsyncProcessorSpec.
struct TAsyncProcessorDescriptor
    : public TRefCounted
{
    std::string Name;
    // Both callbacks are dropped when the job stops.
    TAsyncProcessorStart Start;
    TAsyncProcessorResultHandler ResultHandler;
};

DEFINE_REFCOUNTED_TYPE(TAsyncProcessorDescriptor)

////////////////////////////////////////////////////////////////////////////////

//! #TCompletedAsyncTask with untyped messages; #RootTimer is the swift timer the task descends from.
struct TUntypedCompletedAsyncTask
    : public TRefCounted
{
    TAsyncProcessorDescriptorPtr Processor;
    TInputTimerConstPtr RootTimer;
    TYsonMessagePtr Task;
    TErrorOr<TYsonMessagePtr> Result;
    //! The size of the result.
    i64 ByteSize = 0;
};

DEFINE_REFCOUNTED_TYPE(TUntypedCompletedAsyncTask)

////////////////////////////////////////////////////////////////////////////////

template <CYsonMessage TTask, CYsonMessage TResult>
class TCompletedAsyncTask;

//! Handle of an async processor with tasks of |TTask| and results of |TResult|.
//! Initialized in #DoInit() by #IJobInitContext::InitAsyncProcessor().
template <CYsonMessage TTask, CYsonMessage TResult>
class TAsyncProcessorHandle
{
public:
    using TStart = TCallback<TFuture<TIntrusivePtr<TResult>>(TIntrusivePtr<TTask> task)>;
    //! Typed #TAsyncProcessorResultHandler.
    using TResultHandler = TCallback<void(const TCompletedAsyncTask<TTask, TResult>& completedTask, IOutputCollectorPtr output)>;

    TAsyncProcessorHandle() = default;

    //! Throws if the handle is not initialized.
    const TAsyncProcessorDescriptorPtr& GetDescriptor() const;

private:
    // An initialized handle is always registered: only the init context creates one.
    friend struct IJobInitContext;

    TAsyncProcessorDescriptorPtr Descriptor_;

    TAsyncProcessorHandle(TStringBuf name, TStart start, TResultHandler resultHandler);
};

//! A task of an async processor with its result, as the result handler of the processor gets it.
template <CYsonMessage TTask, CYsonMessage TResult>
class TCompletedAsyncTask
{
public:
    const TTask& GetTask() const;
    //! The result, or the error of the failed task.
    const TErrorOr<TIntrusivePtr<TResult>>& GetResult() const;

    //! Returns the swift timer the task descends from, e.g. for the state of its key.
    const TInputTimerConstPtr& GetRootTimer() const;

private:
    friend class TAsyncProcessorHandle<TTask, TResult>;

    TUntypedCompletedAsyncTaskPtr Untyped_;
    TErrorOr<TIntrusivePtr<TResult>> Result_;

    //! |untyped| must belong to a processor of these types.
    explicit TCompletedAsyncTask(TUntypedCompletedAsyncTaskPtr untyped);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

#define ASYNC_PROCESSOR_INL_H_
#include "async_processor-inl.h"
#undef ASYNC_PROCESSOR_INL_H_

#pragma once
#ifndef JOB_CONTROLLER_INL_H_
#error "Direct inclusion of this file is not allowed, include job_controller.h"
// For the sake of sane code completion.
#include "job_controller.h"
#endif

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <typename TJobType>
void ITypedJobController<TJobType>::OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks)
{
    auto typedJob = DynamicPointerCast<TJobType>(job);
    YT_VERIFY(typedJob);
    OnJobWaiting(typedJob, callbacks);
}

template <typename TJobType>
void ITypedJobController<TJobType>::OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks)
{
    auto typedJob = DynamicPointerCast<TJobType>(job);
    YT_VERIFY(typedJob);
    OnJobRunning(typedJob, callbacks);
}

template <typename TJobType>
void ITypedJobController<TJobType>::OnJobCompleted(const TJobPtr& job)
{
    auto typedJob = DynamicPointerCast<TJobType>(job);
    YT_VERIFY(typedJob);
    OnJobCompleted(typedJob);
}

template <typename TJobType>
void ITypedJobController<TJobType>::OnJobAborted(const TJobPtr& job)
{
    auto typedJob = DynamicPointerCast<TJobType>(job);
    YT_VERIFY(typedJob);
    OnJobAborted(typedJob);
}

template <typename TJobType>
void ITypedJobController<TJobType>::OnJobFailed(const TJobPtr& job)
{
    auto typedJob = DynamicPointerCast<TJobType>(job);
    YT_VERIFY(typedJob);
    OnJobFailed(typedJob);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

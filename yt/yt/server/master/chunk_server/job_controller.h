#pragma once

#include "public.h"
#include "job_registry.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IJobSchedulingContext
{
    virtual ~IJobSchedulingContext() = default;

    virtual TNode* GetNode() const  = 0;
    virtual const NNodeTrackerClient::NProto::TNodeResources& GetNodeResourceUsage() const = 0;
    virtual const NNodeTrackerClient::NProto::TNodeResources& GetNodeResourceLimits() const = 0;

    virtual TJobId GenerateJobId() const = 0;

    virtual void ScheduleJob(const TJobPtr& job) = 0;

    virtual const IJobRegistryPtr& GetJobRegistry() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IJobControllerCallbacks
{
    virtual ~IJobControllerCallbacks() = default;

    virtual void AbortJob(const TJobPtr& job) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IJobController
    : public virtual TRefCounted
{
    virtual void ScheduleJobs(EJobType jobType, IJobSchedulingContext* context) = 0;

    virtual void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) = 0;
    virtual void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) = 0;

    virtual void OnJobCompleted(const TJobPtr& job) = 0;
    virtual void OnJobAborted(const TJobPtr& job) = 0;
    virtual void OnJobFailed(const TJobPtr& job) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobController)

////////////////////////////////////////////////////////////////////////////////

template <typename TJobType>
struct ITypedJobController
    : public virtual IJobController
{
    virtual void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) override;
    virtual void OnJobWaiting(const TIntrusivePtr<TJobType>& job, IJobControllerCallbacks* callbacks) = 0;

    virtual void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) override;
    virtual void OnJobRunning(const TIntrusivePtr<TJobType>& job, IJobControllerCallbacks* callbacks) = 0;

    virtual void OnJobCompleted(const TJobPtr& job) override;
    virtual void OnJobCompleted(const TIntrusivePtr<TJobType>& job) = 0;

    virtual void OnJobAborted(const TJobPtr& job) override;
    virtual void OnJobAborted(const TIntrusivePtr<TJobType>& job) = 0;

    virtual void OnJobFailed(const TJobPtr& job) override;
    virtual void OnJobFailed(const TIntrusivePtr<TJobType>& job) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICompositeJobController
    : public IJobController
{
    virtual void RegisterJobController(EJobType jobType, IJobControllerPtr controller) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompositeJobController)

////////////////////////////////////////////////////////////////////////////////

ICompositeJobControllerPtr CreateCompositeJobController();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define JOB_CONTROLLER_INL_H_
#include "job_controller-inl.h"
#undef JOB_CONTROLLER_INL_H_

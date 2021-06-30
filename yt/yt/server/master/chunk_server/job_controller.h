#pragma once

#include "public.h"

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
    virtual void ScheduleJobs(IJobSchedulingContext* context) = 0;

    virtual void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) = 0;
    virtual void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) = 0;

    virtual void OnJobCompleted(const TJobPtr& job) = 0;
    virtual void OnJobAborted(const TJobPtr& job) = 0;
    virtual void OnJobFailed(const TJobPtr& job) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobController)

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

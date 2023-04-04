#pragma once

#include "job.h"

#include "controller_agent_connector.h"
#include "scheduler_connector.h"

#include <yt/yt/server/job_proxy/public.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job_tracker_service.pb.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all jobs scheduled to run at this node.
/*!
 *   Maintains a map of jobs, allows new jobs to be started and existing jobs to be stopped.
 *   New jobs are constructed by means of per-type factories registered via #RegisterFactory.
 *
 *   \note Thread affinity: any (unless noted otherwise)
 */
class IJobController
    : public TRefCounted
{
public:
    virtual void Initialize() = 0;

    //! Registers a factory for a given job type.
    virtual void RegisterJobFactory(
        EJobType type,
        TJobFactory factory) = 0;

    virtual void ScheduleStartJobs() = 0;

    //! Finds the job by its id, returns |nullptr| if no job is found.
    /*
     * \note Thread affinity: any
     */
    virtual TJobPtr FindJob(TJobId jobId) const = 0;

    //! Finds the job by its id, throws if no job is found.
    virtual TJobPtr GetJobOrThrow(TJobId jobId) const = 0;

    //! Returns the list of all currently known jobs.
    virtual std::vector<TJobPtr> GetJobs() const = 0;

    //! Finds the job that is held after it has been removed.
    virtual TJobPtr FindRecentlyRemovedJob(TJobId jobId) const = 0;

    //! Checks dynamic config to see if job proxy profiling is disabled.
    virtual bool IsJobProxyProfilingDisabled() const = 0;

    //! Returns dynamic config of job proxy.
    virtual NJobProxy::TJobProxyDynamicConfigPtr GetJobProxyDynamicConfig() const = 0;

    //! Set value of flag disabling all scheduler jobs.
    virtual void SetDisableSchedulerJobs(bool value) = 0;

    virtual bool AreSchedulerJobsDisabled() const noexcept = 0;

    virtual void PrepareAgentHeartbeatRequest(
        const TControllerAgentConnectorPool::TControllerAgentConnector::TReqHeartbeatPtr& request,
        const TAgentHeartbeatContextPtr& context) = 0;
    virtual void ProcessAgentHeartbeatResponse(
        const TControllerAgentConnectorPool::TControllerAgentConnector::TRspHeartbeatPtr& response,
        const TAgentHeartbeatContextPtr& context) = 0;

    //! Prepares a scheduler heartbeat request.
    virtual void PrepareSchedulerHeartbeatRequest(
        const TSchedulerConnector::TReqHeartbeatPtr& request,
        const TSchedulerHeartbeatContextPtr& context) = 0;

    //! Handles scheduler heartbeat response, i.e. starts new jobs, aborts and removes old ones etc.
    virtual void ProcessSchedulerHeartbeatResponse(
        const TSchedulerConnector::TRspHeartbeatPtr& response,
        const TSchedulerHeartbeatContextPtr& context) = 0;

    virtual TBuildInfoPtr GetBuildInfo() const = 0;

    virtual void BuildJobProxyBuildInfo(NYTree::TFluentAny fluent) const = 0;
    virtual void BuildJobsInfo(NYTree::TFluentAny fluent) const = 0;
    virtual void BuildJobControllerInfo(NYTree::TFluentMap fluent) const = 0;

    virtual int GetActiveJobCount() const = 0;

    virtual void OnAgentIncarnationOutdated(const TControllerAgentDescriptor& controllerAgentDescriptor) = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TJobPtr&), JobFinished);
    DECLARE_INTERFACE_SIGNAL(void(const TError& error), JobProxyBuildInfoUpdated);
};

DEFINE_REFCOUNTED_TYPE(IJobController)

////////////////////////////////////////////////////////////////////////////////

IJobControllerPtr CreateJobController(NClusterNode::IBootstrapBase* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

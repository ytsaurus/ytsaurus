#pragma once

#include "public.h"
#include "job.h"

#include <core/actions/signal.h>

#include <ytlib/job_tracker_client/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

//! Controls all jobs scheduled to run at this node.
/*!
 *   Maintains a map of jobs, allows new jobs to be started and existing jobs to be stopped.
 *   New jobs are constructed by means of per-type factories registered via #RegisterFactory.
 *
 */
class TJobController
    : public TRefCounted
{
    DEFINE_SIGNAL(void(), ResourcesUpdated);

public:
    TJobController(
        TJobControllerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Registers a factory for a given job type.
    void RegisterFactory(
        EJobType type,
        TJobFactory factory);

    //! Starts a new job.
    IJobPtr CreateJob(
        const TJobId& jobId,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        NJobTrackerClient::NProto::TJobSpec&& jobSpec);

    //! Stops a job.
    /*!
     *  If the job is running, aborts it.
     */
    void AbortJob(IJobPtr job);

    //! Removes the job from the map.
    /*!
     *  It is illegal to call #Remove before the job is stopped.
     */
    void RemoveJob(IJobPtr job);

    //! Finds the job by its id, returns |nullptr| if no job is found.
    IJobPtr FindJob(const TJobId& jobId);

    //! Finds the job by its id, throws if no job is found.
    IJobPtr GetJobOrThrow(const TJobId& jobId);

    //! Returns the list of all currently known jobs.
    std::vector<IJobPtr> GetJobs();

    //! Returns the maximum allowed resource usage.
    NNodeTrackerClient::NProto::TNodeResources GetResourceLimits();

    //! Return the current resource usage.
    NNodeTrackerClient::NProto::TNodeResources GetResourceUsage(bool includeWaiting = true);

    //! Prepares a heartbeat request.
    void PrepareHeartbeatRequest(
        NObjectClient::TCellTag cellTag,
        NObjectClient::EObjectType jobObjectType,
        NJobTrackerClient::NProto::TReqHeartbeat* request);

    //! Handles heartbeat response, i.e. starts new jobs, aborts and removes old ones etc.
    void ProcessHeartbeatResponse(NJobTrackerClient::NProto::TRspHeartbeat* response);

private:
    const TJobControllerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    yhash_map<EJobType, TJobFactory> Factories_;
    yhash_map<TJobId, IJobPtr> Jobs_;

    bool StartScheduled_ = false;


    TJobFactory GetFactory(EJobType type);
    void ScheduleStart();
    void OnResourcesUpdated(
        TWeakPtr<IJob> job, 
        const NNodeTrackerClient::NProto::TNodeResources& resourceDelta);
    void StartWaitingJobs();

    //! Compares new usage with resource limits. Detects resource overdraft.
    bool CheckResourceUsageDelta(const NNodeTrackerClient::NProto::TNodeResources& delta);

};

DEFINE_REFCOUNTED_TYPE(TJobController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT

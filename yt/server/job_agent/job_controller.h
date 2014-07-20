#pragma once

#include "public.h"
#include "job.h"

#include <core/actions/signal.h>

#include <ytlib/job_tracker_client/job_tracker_service.pb.h>

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

    //! Updates the resource usage of a given job.
    /*!
     *  Resource usage may only decrease.
     */
    void UpdateJobResourceUsage(
        IJobPtr job,
        const NNodeTrackerClient::NProto::TNodeResources& usage);

    //! Updates job progress.
    void UpdateJobProgress(
        IJobPtr job,
        double progress,
        const NJobTrackerClient::NProto::TJobStatistics& jobStatistics);

    //! Compares new usage with resource limits. Detects resource overdraft.
    bool CheckResourceUsageDelta(const NNodeTrackerClient::NProto::TNodeResources& delta);

    //! Updates job result.
    void SetJobResult(
        IJobPtr job,
        const NJobTrackerClient::NProto::TJobResult& result);

    //! Prepares a heartbeat request.
    /*!
     *  Only jobs with type matching #jobTypes are listed.
     */
    void PrepareHeartbeat(NJobTrackerClient::NProto::TReqHeartbeat* request);

    //! Handles heartbeat response, i.e. starts new jobs, aborts and removes old ones etc.
    void ProcessHeartbeat(NJobTrackerClient::NProto::TRspHeartbeat* response);

private:
    TJobControllerConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    yhash_map<EJobType, TJobFactory> Factories;
    yhash_map<TJobId, IJobPtr> Jobs;

    bool StartScheduled;
    bool ResourcesUpdatedFlag;

    TJobFactory GetFactory(EJobType type);
    void ScheduleStart();
    void OnJobFinished(IJobPtr job);
    void OnResourcesReleased();
    void StartWaitingJobs();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT

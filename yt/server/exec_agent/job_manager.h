#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/scheduler/scheduler_service.pb.h>
#include <ytlib/scheduler/job.pb.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Central control point for managing scheduled jobs.
/*!
 *   Maintains a list of jobs, allows new jobs to be started and existing jobs to
 *   be stopped.
 *
 */
class TJobManager
    : public TRefCounted
{
    DEFINE_SIGNAL(void(), ResourcesUpdated);

public:
    TJobManager(
        TJobManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Initializes slots etc.
    void Initialize();

    //! Starts a new job.
    void CreateJob(
        const TJobId& jobId,
        const NScheduler::NProto::TNodeResources& resourceLimits,
        NScheduler::NProto::TJobSpec& jobSpec);

    //! Stops a job.
    /*!
     *  If the job is running, aborts it.
     */
    void AbortJob(const TJobId& jobId);

    //! Removes the job from the list thus making the slot free.
    /*!
     *  It is illegal to call #Remove before the job is stopped.
     */
    void RemoveJob(const TJobId& jobId);

    //! Finds the job by its id, returns NULL if no job is found.
    TJobPtr FindJob(const TJobId& jobId);

    //! Finds the job by its id, throws if no job is found.
    TJobPtr GetJob(const TJobId& jobId);

    //! Returns a list of all currently known jobs.
    std::vector<TJobPtr> GetJobs();

    //! Maximum allowed resource usage.
    NScheduler::NProto::TNodeResources GetResourceLimits();

    //! Current resource usage.
    NScheduler::NProto::TNodeResources GetResourceUsage(bool includeWaiting = true);

    void UpdateResourceUsage(
        const TJobId& jobId,
        const NScheduler::NProto::TNodeResources& usage);

private:
    TJobManagerConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    std::vector<TSlotPtr> Slots;
    yhash_map<TJobId, TJobPtr> Jobs;

    bool StartScheduled;
    bool ResourcesUpdatedFlag;

    TSlotPtr GetFreeSlot();

    void ScheduleStart();
    void OnJobFinished(TJobPtr job);
    void OnResourcesReleased();
    void StartWaitingJobs();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExexNode
} // namespace NYT

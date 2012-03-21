#pragma once

#include "public.h"

#include <ytlib/scheduler/jobs.pb.h>
#include <ytlib/misc/thread_affinity.h>

//#include <ytlib/exec/scheduler_internal_proxy.h>
//#include <ytlib/chunk_holder/chunk_cache.h>
//#include <ytlib/misc/error.h>

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
public:
    typedef TIntrusivePtr<TJobManager> TPtr;

    TJobManager(
        TJobManagerConfigPtr config,
        TBootstrap* bootstrap);

    //~TJobManager();

    //! Starts a new job.
    void StartJob(
        const TJobId& jobId,
        const NScheduler::NProto::TJobSpec& jobSpec);

    //! Stops a job.
    /*!
     *  If the job is running, aborts it.
     */
    void StopJob(const TJobId& jobId);

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
    std::vector<TJobPtr> GetAllJobs();

    int GetTotalSlotCount();
    int GetFreeSlotCount();

private:
    TJobManagerConfigPtr Config;
    TBootstrap* Bootstrap;

    std::vector<TSlotPtr> Slots;
    yhash_map<TJobId, TJobPtr> Jobs;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExexNode 
} // namespace NYT

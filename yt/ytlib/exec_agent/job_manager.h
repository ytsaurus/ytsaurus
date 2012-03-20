#pragma once

#include "common.h"
#include "job.h"
#include "slot.h"
#include "environment.h"
#include "environment_manager.h"

#include "operations.pb.h"

#include <ytlib/exec/scheduler_internal_proxy.h>
#include <ytlib/chunk_holder/chunk_cache.h>
#include <ytlib/misc/error.h>

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
        NChunkHolder::TChunkCache* chunkCache,
        NRpc::IChannel::TPtr masterChannel);

    ~TJobManager();

    //! Starts a new job.
    void StartJob(
        const TJobId& jobId,
        const NScheduler::NProto::TJobSpec& jobSpec);

    // TODO(babenko):
    // void StopJob(const TJobId& jobId);
    //void CancelOperation(const TOperationId& operationId);

    TJobPtr GetJob(const TJobId& jobId);

    //! Returns a list of all currently known jobs.
    std::vector<TJobPtr> GetJobs();

    IInvoker::TPtr GetInvoker();

    void SetJobResult(
        const TJobId& jobId, 
        const NScheduler::NProto::TJobResult& jobResult);

    //const NScheduler::NProto::TJobSpec& GetJobSpec(const TJobId& jobId);

private:

    void OnJobStarted(const TJobId& jobId);

    void OnJobFinished(
        NScheduler::NProto::TJobResult jobResult,
        const TJobId& jobId);

    TConfig::TPtr Config;

    TActionQueue::TPtr JobManagerThread;

    std::vector<TSlotPtr> Slots;

    yhash_map<TJobId, TIntrusivePtr<TJob> > Jobs;

    TEnvironmentManager EnvironmentManager;

    NChunkHolder::TChunkCachePtr ChunkCache;
    NRpc::IChannel::TPtr MasterChannel;

    NScheduler::TSchedulerInternalProxy SchedulerProxy;

    DECLARE_THREAD_AFFINITY_SLOT(JobManagerThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExexNode 
} // namespace NYT

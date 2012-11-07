#pragma once

#include "public.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/thread_affinity.h>

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/actions/signal.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/file_client/file_ypath.pb.h>

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/logging/tagged_logger.h>

#include <server/scheduler/job_resources.h>

#include <server/job_proxy/public.h>

#include <server/chunk_holder/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
    DEFINE_SIGNAL(void(const NScheduler::NProto::TNodeResources&, const NScheduler::NProto::TNodeResources&), ResourcesReleased);

public:
    TJob(
        const TJobId& jobId,
        NScheduler::NProto::TJobSpec&& jobSpec,
        NJobProxy::TJobProxyConfigPtr proxyConfig,
        NChunkHolder::TChunkCachePtr chunkCache);

    void Start(TEnvironmentManagerPtr environmentManager, TSlotPtr slot);

    //! Kills the job if it is running.
    void Abort(const TError& error);

    const TJobId& GetId() const;

    const NScheduler::NProto::TJobSpec& GetSpec();

    NScheduler::EJobState GetState() const;
    NScheduler::EJobPhase GetPhase() const;

    NScheduler::NProto::TNodeResources GetResourceUtilization() const;

    //! Notifies the exec agent that job 
    /*
     * New utilization should not exceed initial utilization.
     */
    void ReleaseResources(const NScheduler::NProto::TNodeResources& newUtilization);

    double GetProgress() const;
    void UpdateProgress(double progress);

    const NScheduler::NProto::TJobResult& GetResult() const;
    void SetResult(const NScheduler::NProto::TJobResult& jobResult);

private:
    void DoStart(TEnvironmentManagerPtr environmentManager);
    void PrepareUserJob(
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        TParallelAwaiterPtr awaiter);
    void OnChunkDownloaded(
        const NFileClient::NProto::TRspFetchFile& fetchRsp,
        TValueOrError<NChunkHolder::TCachedChunkPtr> result);

    void RunJobProxy();
    void SetResult(const TError& error);

    bool IsResultSet() const;
    void FinalizeJob();

    //! Called by ProxyController when proxy process finishes.
    void OnJobExit(TError error);

    void DoAbort(
        const TError& error, 
        NScheduler::EJobState resultState, 
        bool killJobProxy = false);

    const TJobId JobId;
    const NScheduler::NProto::TJobSpec JobSpec;

    TSpinLock ResourcesLock;
    NScheduler::NProto::TNodeResources ResourceUtilization;

    NLog::TTaggedLogger Logger;

    NChunkHolder::TChunkCachePtr ChunkCache;

    TSlotPtr Slot;

    NScheduler::EJobState JobState;
    NScheduler::EJobPhase JobPhase;

    double Progress;

    NJobProxy::TJobProxyConfigPtr ProxyConfig;

    std::vector<NChunkHolder::TCachedChunkPtr> CachedChunks;

    IProxyControllerPtr ProxyController;

    // Protects #JobResult.
    TSpinLock ResultLock;
    TNullable<NScheduler::NProto::TJobResult> JobResult;
    TPromise<void> JobFinished;


    DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


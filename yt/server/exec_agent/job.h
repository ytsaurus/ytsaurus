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

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <server/job_proxy/public.h>

#include <server/chunk_holder/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
public:
    TJob(
        const TJobId& jobId,
        NScheduler::NProto::TJobSpec&& jobSpec,
        NJobProxy::TJobProxyConfigPtr proxyConfig,
        NChunkHolder::TChunkCachePtr chunkCache,
        TSlotPtr slot);
    ~TJob();

    void Start(TEnvironmentManagerPtr environmentManager);

    //! Kills the job if it is running.
    void Abort();
    void Abort(const TError& error);

    const TJobId& GetId() const;

    const NScheduler::NProto::TJobSpec& GetSpec();

    NScheduler::EJobState GetState() const;
    NScheduler::EJobPhase GetPhase() const;

    NScheduler::NProto::TNodeResources GetResourceUtilization() const;

    // New utilization should not exceed initial utilization.
    void UpdateResourceUtilization(const NScheduler::NProto::TNodeResources& utilization);

    double GetProgress() const;
    void UpdateProgress(double progress);

    const NScheduler::NProto::TJobResult& GetResult() const;
    void SetResult(const NScheduler::NProto::TJobResult& jobResult);

    DECLARE_SIGNAL(void(), Finished);
    DEFINE_SIGNAL(void(), ResourceUtilizationSet);

private:
    void DoStart(TEnvironmentManagerPtr environmentManager);
    void PrepareUserJob(
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        TParallelAwaiterPtr awaiter);
    void OnChunkDownloaded(
        const NFileClient::NProto::TRspFetch& fetchRsp,
        TValueOrError<NChunkHolder::TCachedChunkPtr> result);

    void RunJobProxy();
    void SetResult(const TError& error);

    bool IsResultSet() const;

    //! Called by ProxyController when proxy process finishes.
    void OnJobExit(TError error);

    void DoAbort(
        const TError& error, 
        NScheduler::EJobState resultState, 
        bool killJobProxy = false);

    const TJobId JobId;
    const NScheduler::NProto::TJobSpec JobSpec;

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
    TSpinLock SpinLock;
    TNullable<NScheduler::NProto::TJobResult> JobResult;
    TPromise<void> JobFinished;


    DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


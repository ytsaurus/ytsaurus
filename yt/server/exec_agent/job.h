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
        const NScheduler::NProto::TNodeResources& resourceLimits,
        NScheduler::NProto::TJobSpec&& jobSpec,
        TBootstrap* bootstrap);

    /*!
        \note Thread affinity: control thread.
     */
    void Start(TEnvironmentManagerPtr environmentManager, TSlotPtr slot);

    //! Kills the job if it is running.
    /*!
        \note Thread affinity: control thread.
     */
    void Abort(const TError& error);

    /*!
        \note Thread affinity: any.
     */
    const TJobId& GetId() const;

    /*!
        \note Thread affinity: any.
     */
    const NScheduler::NProto::TNodeResources& GetResourceLimits() const;

    /*!
        \note Thread affinity: any.
     */
    const NScheduler::NProto::TJobSpec& GetSpec() const;

    /*!
        \note Thread affinity: any.
     */
    NScheduler::EJobState GetState() const;

    /*!
        \note Thread affinity: any.
     */
    NScheduler::EJobPhase GetPhase() const;

    /*!
        \note Thread affinity: any.
     */
    NScheduler::NProto::TNodeResources GetResourceUsage() const;

    //! Notifies the exec agent that job resource usage is changed.
    /*!
     *  \note Thread affinity: any.
     *  New usage should not exceed the previous one.
     */
    void ReleaseResources(const NScheduler::NProto::TNodeResources& newUsage);

    double GetProgress() const;
    void UpdateProgress(double progress);

    /*!
        \note Thread affinity: any.
     */
    const NScheduler::NProto::TJobResult& GetResult() const;

    /*!
        \note Thread affinity: any.
     */
    void SetResult(const NScheduler::NProto::TJobResult& jobResult);

private:
    void DoStart(TEnvironmentManagerPtr environmentManager);

    void PrepareUserJob(
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        TParallelAwaiterPtr awaiter);
    TPromise<void> PrepareDownloadingTableFile(
        const NYT::NScheduler::NProto::TTableFile& rsp);

    void OnChunkDownloaded(
        const NFileClient::NProto::TRspFetchFile& fetchRsp,
        TValueOrError<NChunkHolder::TCachedChunkPtr> result);

    typedef std::vector< TValueOrError<NChunkHolder::TCachedChunkPtr> > TDownloadedChunks;
    void OnTableDownloaded(
        const NYT::NScheduler::NProto::TTableFile& tableFileRsp,
        TSharedPtr<TDownloadedChunks> downloadedChunks,
        TPromise<void> promise);

    void RunJobProxy();
    void SetResult(const TError& error);

    bool IsResultSet() const;
    void FinalizeJob();

    //! Called by ProxyController when proxy process finishes.
    void OnJobExit(TError error);

    void DoAbort(
        const TError& error,
        NScheduler::EJobState resultState);

    const TJobId JobId;
    const NScheduler::NProto::TJobSpec JobSpec;
    const NScheduler::NProto::TNodeResources ResourceLimits;

    TSpinLock ResourcesLock;
    NScheduler::NProto::TNodeResources ResourceUsage;

    NLog::TTaggedLogger Logger;

    TBootstrap* Bootstrap;

    NChunkHolder::TChunkCachePtr ChunkCache;

    TSlotPtr Slot;

    NScheduler::EJobState JobState;
    NScheduler::EJobPhase JobPhase;

    double Progress;

    std::vector<NChunkHolder::TCachedChunkPtr> CachedChunks;

    IProxyControllerPtr ProxyController;

    // Protects #JobResult.
    TSpinLock ResultLock;
    TNullable<NScheduler::NProto::TJobResult> JobResult;

    NJobProxy::TJobProxyConfigPtr ProxyConfig;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


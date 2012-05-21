#pragma once

#include "public.h"

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/signal.h>
#include <ytlib/chunk_holder/public.h>
// TODO(babenko): consider removing.
#include <ytlib/chunk_holder/chunk_cache.h>
#include <ytlib/rpc/public.h>
#include <ytlib/ytree/public.h>

#include <ytlib/file_server/file_ypath.pb.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public virtual TRefCounted
{
public:
    TJob(
        const TJobId& jobId,
        const NScheduler::NProto::TJobSpec& jobSpec,
        const NYTree::TYson& proxyConfig,
        NChunkHolder::TChunkCachePtr chunkCache,
        TSlotPtr slot);
    ~TJob();

    void Start(TEnvironmentManager* environmentManager);

    //! Kills the job if it is running.
    void Abort();

    const TJobId& GetId() const;

    const NScheduler::NProto::TJobSpec& GetSpec();

    NScheduler::EJobState GetState() const;
    NScheduler::EJobProgress GetProgress() const;

    const NScheduler::NProto::TJobResult& GetResult() const;
    void SetResult(const NScheduler::NProto::TJobResult& jobResult);

    DECLARE_SIGNAL(void(), Finished);

private:
    void DoStart(TEnvironmentManagerPtr environmentManager);
    void OnChunkDownloaded(
        const NFileServer::NProto::TRspFetch& fetchRsp,
        NChunkHolder::TChunkCache::TDownloadResult result);

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

    NScheduler::EJobState JobState;
    NScheduler::EJobProgress JobProgress;

    NYTree::TYson ProxyConfig;

    TSlotPtr Slot;

    NChunkHolder::TChunkCachePtr ChunkCache;
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


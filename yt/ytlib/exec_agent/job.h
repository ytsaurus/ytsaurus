#pragma once

#include "public.h"

#include <ytlib/scheduler/jobs.pb.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/signal.h>
#include <ytlib/chunk_holder/public.h>
//ToDo: consider removing.
#include <ytlib/chunk_holder/chunk_cache.h>
#include <ytlib/rpc/channel.h>

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
        NChunkHolder::TChunkCachePtr chunkCache,
        TSlotPtr slot);
    ~TJob();

    void Start(TEnvironmentManager* environmentManager);

    //! Kills the job if it is running.
    void Abort(const TError& error);

    const TJobId& GetId() const;

    const NScheduler::NProto::TJobSpec& GetSpec();

    NScheduler::EJobState GetState() const;
    NScheduler::EJobProgress GetProgress() const;
    
    NScheduler::NProto::TJobResult GetResult();
    void SetResult(const NScheduler::NProto::TJobResult& jobResult);
    void SetResult(const TError& error);

    DECLARE_SIGNAL(void(), Started);
    DECLARE_SIGNAL(void(NScheduler::NProto::TJobResult), Finished);

private:
    void DoStart(TEnvironmentManagerPtr environmentManager);
    void OnChunkDownloaded(
        NChunkHolder::TChunkCache::TDownloadResult result,
        const Stroka& fileName,
        bool executable);

    void RunJobProxy();

    //! Called by ProxyController when proxy process finishes.
    void OnJobExit(TError error);

    void DoAbort(const TError& error, NScheduler::EJobState resultState);

    const TJobId JobId;
    const NScheduler::NProto::TJobSpec JobSpec;

    NScheduler::EJobState JobState;
    NScheduler::EJobProgress JobProgress;

    TSlotPtr Slot;

    NChunkHolder::TChunkCachePtr ChunkCache;
    std::vector<NChunkHolder::TCachedChunkPtr> CachedChunks;

    TAutoPtr<IProxyController> ProxyController;

    // Protects #JobResult.
    TSpinLock SpinLock;
    NScheduler::NProto::TJobResult JobResult;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


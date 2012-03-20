#pragma once

#include "public.h"
#include "jobs.pb.h"

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

    void Start(TEnvironmentManager* environmentManager);

    //! Kills the job if it is running. Cleans up the slot.
    void Abort(const TError& error);

    const TJobId& GetId() const;

    const NScheduler::NProto::TJobSpec& GetSpec();

    NScheduler::EJobState GetState();


    NScheduler::NProto::TJobResult GetResult();
    void SetResult(const NScheduler::NProto::TJobResult& jobResult);

    DECLARE_SIGNAL(void(), Started);
    DECLARE_SIGNAL(void(NScheduler::NProto::TJobResult), Finished);

private:
    void DoStart(TEnvironmentManagerPtr environmentManager);
    void OnChunkDownloaded(
        NChunkHolder::TChunkCache::TDownloadResult result,
        const Stroka& fileName,
        bool executable);

    void RunJobProxy();

    //void OnJobExit(TError error);

    //void DoCancel(const TError& error);


    TJobId JobId;
    const NScheduler::NProto::TJobSpec JobSpec;
    NChunkHolder::TChunkCachePtr ChunkCache;
    TSlotPtr Slot;
    TAutoPtr<IProxyController> ProxyController;

    NScheduler::EJobState JobState;

    NScheduler::NProto::TJobResult JobResult;

    std::vector<NChunkHolder::TCachedChunkPtr> CachedChunks;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


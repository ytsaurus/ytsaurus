#pragma once

#include "public.h"
#include "jobs.pb.h"

#include <ytlib/misc/error.h>
#include <ytlib/actions/signal.h>
#include <ytlib/chunk_holder/public.h>
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
        NRpc::IChannel::TPtr masterChannel,
        TSlotPtr slot,
        IProxyController* proxyController);

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
    //void RunJobProxy();

    //void PrepareFiles();

    //void OnFilesFetched(
    //    NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    //void OnChunkDownloaded(
    //    NChunkHolder::TChunkCache::TDownloadResult result,
    //    int fileIndex,
    //    const Stroka& fileName,
    //    bool executable);

    //void OnJobExit(TError error);

    //void DoCancel(const TError& error);

    //void StartComplete();

    TJobId JobId;
    const NScheduler::NProto::TJobSpec JobSpec;
    NChunkHolder::TChunkCachePtr ChunkCache;
    NRpc::IChannel::TPtr MasterChannel;
    TSlotPtr Slot;
    TAutoPtr<IProxyController> ProxyController;

    //NChunkHolder::TChunkCachePtr ChunkCache;
    //NRpc::IChannel::TPtr MasterChannel;

    //NCypress::TCypressServiceProxy CypressProxy;
    //TError Error;

    TFuture<NScheduler::NProto::TJobResult>::TPtr JobResult;
    TFuture<TVoid>::TPtr Started;
    TFuture<NScheduler::NProto::TJobResult>::TPtr Finished;

    //yvector<NChunkHolder::TCachedChunkPtr> CachedChunks;

    //DECLARE_THREAD_AFFINITY_SLOT(JobThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT


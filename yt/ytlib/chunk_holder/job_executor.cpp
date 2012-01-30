#include "stdafx.h"
#include "job_executor.h"

#include <ytlib/misc/string.h>
#include <ytlib/chunk_client/remote_writer.h>

namespace NYT {
namespace NChunkHolder {

using namespace NYT::NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobExecutor* owner,
    IInvoker* serviceInvoker,
    EJobType jobType,
    const TJobId& jobId,
    TStoredChunk* chunk,
    const yvector<Stroka>& targetAddresses)
    : Owner(owner)
    , JobType(jobType)
    , JobId(jobId)
    , State(EJobState::Running)
    , Chunk(chunk)
    , TargetAddresses(targetAddresses)
    , CancelableInvoker(New<TCancelableInvoker>(serviceInvoker))
    , Logger(ChunkHolderLogger)
{
    YASSERT(serviceInvoker);
    YASSERT(chunk);

    Logger.AddTag(Sprintf("JobId: %s", ~JobId.ToString()));
}

EJobType TJob::GetType() const
{
    return JobType;
}

TJobId TJob::GetJobId() const
{
    return JobId;
}

NYT::NChunkHolder::EJobState TJob::GetState() const
{
    return State;
}

yvector<Stroka> TJob::GetTargetAddresses() const
{
    return TargetAddresses;
}

TChunk::TPtr TJob::GetChunk() const
{
    return Chunk;
}

void TJob::Start()
{
    switch (JobType) {
        case EJobType::Remove: {
            LOG_INFO("Removal job started (ChunkId: %s)",
                ~Chunk->GetId().ToString());

            Owner->ChunkStore->RemoveChunk(~Chunk);

            LOG_DEBUG("Removal job completed");

            State = EJobState::Completed;
            break;
        }

        case EJobType::Replicate: {
            LOG_INFO("Replication job started (TargetAddresses: [%s], ChunkId: %s)",
                ~JoinToString(TargetAddresses),
                ~Chunk->GetId().ToString());

            Chunk->GetInfo()->Subscribe(
                FromMethod(
                    &TJob::OnChunkInfoLoaded,
                    TPtr(this))
                ->Via(CancelableInvoker));
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TJob::Stop()
{
    CancelableInvoker->Cancel();
    if (Writer) {
        Writer->Cancel(TError("Replication job stopped"));
    }
}

void TJob::OnChunkInfoLoaded(NChunkClient::IAsyncReader::TGetInfoResult result)
{
    if (!result.IsOK()) {
        LOG_WARNING("Error getting chunk info\n%s",
            ~result.ToString());

        State = EJobState::Failed;
        return;
    }

    ChunkInfo = result.Value();

    Writer = New<TRemoteWriter>(
        ~Owner->Config->RemoteWriter,
        Chunk->GetId(),
        TargetAddresses);
    Writer->Open();

    ReplicateBlock(TError(), 0);
}

void TJob::ReplicateBlock(TError error, int blockIndex)
{
    if (!error.IsOK()) {
        LOG_WARNING("Replication failed (BlockIndex: %d)\n%s",
            blockIndex,
            ~error.ToString());

        State = EJobState::Failed;
        return;
    }

    if (blockIndex >= static_cast<int>(ChunkInfo.blocks_size())) {
        LOG_DEBUG("All blocks are enqueued for replication");

        Writer->AsyncClose(ChunkInfo.attributes())->Subscribe(
            FromMethod(
                &TJob::OnWriterClosed,
                TPtr(this))
            ->Via(CancelableInvoker));
        return;
    }

    TBlockId blockId(Chunk->GetId(), blockIndex);

    LOG_DEBUG("Retrieving block for replication (BlockIndex: %d)",
        blockIndex);

    Owner->BlockStore->GetBlock(blockId)->Subscribe(
        FromMethod(
            &TJob::OnBlockLoaded,
            TPtr(this),
            blockIndex)
        ->Via(CancelableInvoker));
}

void TJob::OnBlockLoaded(TBlockStore::TGetBlockResult result, int blockIndex)
{
    if (!result.IsOK()) {
        LOG_WARNING("Error getting block for replication (BlockIndex: %d)\n%s",
            blockIndex,
            ~result.ToString());

        State = EJobState::Failed;
        return;
    } 

    auto block = result.Value();
    Writer->AsyncWriteBlock(block->GetData())->Subscribe(
        FromMethod(
            &TJob::ReplicateBlock,
            TPtr(this),
            blockIndex + 1)
        ->Via(CancelableInvoker));
}

void TJob::OnWriterClosed(TError error)
{
    if (error.IsOK()) {
        LOG_DEBUG("Replication job completed");

        Writer.Reset();
        State = EJobState::Completed;
    } else {
        LOG_WARNING("Replication job failed\n%s",
            ~error.ToString());

        Writer.Reset();
        State = EJobState::Failed;
    }
}

////////////////////////////////////////////////////////////////////////////////

TJobExecutor::TJobExecutor(
    TConfig* config,
    TChunkStore* chunkStore,
    TBlockStore* blockStore,
    IInvoker* serviceInvoker)
    : Config(config)
    , ChunkStore(chunkStore)
    , BlockStore(blockStore)
    , ServiceInvoker(serviceInvoker)
{
    YASSERT(chunkStore);
    YASSERT(blockStore);
    YASSERT(serviceInvoker);
}

TJob::TPtr TJobExecutor::StartJob(
    EJobType jobType,
    const TJobId& jobId,
    TStoredChunk* chunk,
    const yvector<Stroka>& targetAddresses)
{
    auto job = New<TJob>(
        this,
        ~ServiceInvoker,
        jobType,
        jobId,
        chunk,
        targetAddresses);
    YVERIFY(Jobs.insert(MakePair(jobId, job)).second);
    job->Start();

    return job;
}

void TJobExecutor::StopJob(TJob* job)
{
    job->Stop();
    YVERIFY(Jobs.erase(job->GetJobId()) == 1);
    
    LOG_INFO("Job stopped (JobId: %s, State: %s)",
        ~job->GetJobId().ToString(),
        ~job->GetState().ToString());
}

TJob::TPtr TJobExecutor::FindJob(const TJobId& jobId)
{
    auto it = Jobs.find(jobId);
    return it == Jobs.end() ? NULL : it->second;
}

yvector<TJob::TPtr> TJobExecutor::GetAllJobs()
{
    yvector<TJob::TPtr> result;
    FOREACH(const auto& pair, Jobs) {
        result.push_back(pair.second);
    }
    return result;
}

void TJobExecutor::StopAllJobs()
{
    FOREACH(auto& pair, Jobs) {
        pair.second->Stop();
    }
    Jobs.clear();

    LOG_INFO("All jobs stopped");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

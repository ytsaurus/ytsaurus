#include "replicator.h"

#include "../misc/assert.h"
#include "../chunk_client/remote_chunk_writer.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    IInvoker::TPtr serviceInvoker,
    TChunkStore::TPtr chunkStore,
    TBlockStore::TPtr blockStore,
    EJobType jobType,
    const TJobId& jobId,
    TChunk::TPtr chunk,
    const yvector<Stroka>& targetAddresses)
    : ChunkStore(chunkStore)
    , BlockStore(blockStore)
    , JobType(jobType)
    , JobId(jobId)
    , State(EJobState::Running)
    , Chunk(chunk)
    , TargetAddresses(targetAddresses)
    , CancelableInvoker(New<TCancelableInvoker>(serviceInvoker))
{ }

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
        case EJobType::Remove:
            LOG_INFO("Removal job started (JobId: %s, ChunkId: %s)",
                ~JobId.ToString(),
                ~Chunk->GetId().ToString());

            ChunkStore->RemoveChunk(Chunk);

            LOG_DEBUG("Removal job completed (JobId: %s)",
                ~JobId.ToString());

            State = EJobState::Completed;
            break;

        case EJobType::Replicate:
            LOG_INFO("Replication job started (JobId: %s, TargetAddresses: [%s], ChunkId: %s)",
                ~JobId.ToString(),
                ~JoinStroku(TargetAddresses, ", "),
                ~Chunk->GetId().ToString());

            ChunkStore->GetChunkMeta(Chunk)->Subscribe(
                FromMethod(
                &TJob::OnGotMeta,
                TPtr(this))
                ->Via(~CancelableInvoker));
            break;

        default:
            YASSERT(false);
            break;
    }
}

void TJob::OnGotMeta(TChunkMeta::TPtr meta)
{
    Meta = meta;

    Writer = ~New<TRemoteChunkWriter>(
        TRemoteChunkWriter::TConfig(),
        Chunk->GetId(),
        TargetAddresses);

    ReplicateBlock(0);
}

void TJob::Stop()
{
    CancelableInvoker->Cancel();
    if (~Writer != NULL) {
        Writer->Cancel();
    }
}

bool TJob::ReplicateBlock(int blockIndex)
{
    if (blockIndex >= Meta->GetBlockCount()) {
        LOG_DEBUG("All blocks are enqueued for replication (JobId: %s)",
            ~JobId.ToString());

        Writer->AsyncClose()->Subscribe(
            FromMethod(
                &TJob::OnWriterClosed,
                TPtr(this))
            ->Via(~CancelableInvoker));
        return false;
    }

    TBlockId blockId(Chunk->GetId(), blockIndex);

    LOG_DEBUG("Retrieving block for replication (JobId: %s, BlockIndex: %d)",
        ~JobId.ToString(), 
        blockIndex);

    BlockStore->FindBlock(blockId)->Subscribe(
        FromMethod(
            &TJob::OnBlockLoaded,
            TPtr(this),
            blockIndex)
        ->Via(~CancelableInvoker));
    return true;
}

void TJob::OnBlockLoaded(TCachedBlock::TPtr cachedBlock, int blockIndex)
{
    TAsyncResult<TVoid>::TPtr ready;
    IChunkWriter::EResult result = Writer->AsyncWriteBlock(cachedBlock->GetData(), &ready);
    switch (result) {
        case IChunkWriter::EResult::OK:
            LOG_DEBUG("Block is enqueued to replication writer (JobId: %s, BlockIndex: %d)",
                    ~JobId.ToString(),
                    blockIndex);

            ReplicateBlock(blockIndex + 1);
            break;

        case IChunkWriter::EResult::TryLater:
            LOG_DEBUG("Replication writer window overflow (JobId: %s, BlockIndex: %d)",
                ~JobId.ToString(),
                blockIndex);

            YASSERT(~ready != NULL);
            ready->Subscribe(
                FromMethod(
                    &TJob::OnBlockLoaded,
                    TPtr(this),
                    cachedBlock,
                    blockIndex)
                ->ToParamAction<TVoid>()
                ->Via(~CancelableInvoker));
            break;

        case IChunkWriter::EResult::Failed:
            LOG_WARNING("Replication failed (JobId: %s, BlockIndex: %d)",
                ~JobId.ToString(),
                blockIndex);

            State = EJobState::Failed;
            break;

        default:
            YASSERT(false);
            break;
    }
}

void TJob::OnWriterClosed(IChunkWriter::EResult result)
{
    switch (result) {
        case IChunkWriter::EResult::OK:
            LOG_DEBUG("Replication job completed (JobId: %s)",
                ~JobId.ToString());

            Writer.Drop();
            State = EJobState::Completed;
            break;

        case IChunkWriter::EResult::Failed:
            LOG_WARNING("Replication job failed (JobId: %s)",
                ~JobId.ToString());

            Writer->Cancel();
            Writer.Drop();
            State = EJobState::Failed;
            break;

        default:
            YASSERT(false);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

TReplicator::TReplicator(
    TChunkStore::TPtr chunkStore,
    TBlockStore::TPtr blockStore,
    IInvoker::TPtr serviceInvoker)
    : ChunkStore(chunkStore)
    , BlockStore(blockStore)
    , ServiceInvoker(serviceInvoker)
{ }

TJob::TPtr TReplicator::StartJob(
    EJobType jobType,
    const TJobId& jobId,
    TChunk::TPtr chunk,
    const yvector<Stroka>& targetAddresses)
{
    TJob::TPtr job = New<TJob>(
        ServiceInvoker,
        ChunkStore,
        BlockStore,
        jobType,
        jobId,
        chunk,
        targetAddresses);
    YVERIFY(Jobs.insert(MakePair(jobId, job)).Second());
    job->Start();

    return job;
}

void TReplicator::StopJob(TJob::TPtr job)
{
    job->Stop();
    YVERIFY(Jobs.erase(job->GetJobId()) == 1);
    
    LOG_INFO("Replication job stopped (JobId: %s, State: %s)",
        ~job->GetJobId().ToString(),
        ~job->GetState().ToString());
}

TJob::TPtr TReplicator::FindJob(const TJobId& jobId)
{
    auto it = Jobs.find(jobId);
    return it == Jobs.end() ? NULL : it->Second();
}

yvector<TJob::TPtr> TReplicator::GetAllJobs()
{
    yvector<TJob::TPtr> result;
    for (TJobMap::iterator it = Jobs.begin();
         it != Jobs.end();
         ++it)
    {
        result.push_back(it->Second());
    }
    return result;
}

void TReplicator::StopAllJobs()
{
    for (TJobMap::iterator it = Jobs.begin();
        it != Jobs.end();
        ++it)
    {
        it->Second()->Stop();
    }
    Jobs.clear();

    LOG_INFO("All replication jobs stopped");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

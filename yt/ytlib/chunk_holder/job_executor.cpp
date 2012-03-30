#include "stdafx.h"
#include "common.h"
#include "job_executor.h"
#include "block_store.h"
#include "chunk_store.h"
#include "chunk.h"
#include "config.h"
#include "location.h"

#include <ytlib/misc/string.h>
#include <ytlib/chunk_client/remote_writer.h>

namespace NYT {
namespace NChunkHolder {

using namespace NYT::NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobExecutorPtr owner,
    IInvoker::TPtr serviceInvoker,
    EJobType jobType,
    const TJobId& jobId,
    TStoredChunkPtr chunk,
    const yvector<Stroka>& targetAddresses)
    : Owner(owner)
    , JobType(jobType)
    , JobId(jobId)
    , State(EJobState::Running)
    , Chunk(chunk)
    , TargetAddresses(targetAddresses)
    , CancelableContext(New<TCancelableContext>())
    , CancelableInvoker(CancelableContext->CreateInvoker(serviceInvoker))
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

EJobState TJob::GetState() const
{
    return State;
}

yvector<Stroka> TJob::GetTargetAddresses() const
{
    return TargetAddresses;
}

TChunkPtr TJob::GetChunk() const
{
    return Chunk;
}

void TJob::Start()
{
    auto this_ = MakeStrong(this);

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

            Chunk
                ->GetInfo()
                ->Subscribe(BIND([=] (IAsyncReader::TGetInfoResult result) {
                    if (!result.IsOK()) {
                        LOG_WARNING("Error getting chunk info (ChunkId: %s)\n%s",
                            ~Chunk->GetId().ToString(),
                            ~result.ToString());

                        this_->State = EJobState::Failed;
                        return;
                    }

                    this_->ChunkInfo = result.Value();

                    this_->Writer = New<TRemoteWriter>(
                        ~this_->Owner->Config->ReplicationRemoteWriter,
                        this_->Chunk->GetId(),
                        this_->TargetAddresses);
                    this_->Writer->Open();

                    ReplicateBlock(0, TError());
                })
                .Via(CancelableInvoker));
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TJob::Stop()
{
    CancelableContext->Cancel();
    Writer.Reset();
}

void TJob::ReplicateBlock(int blockIndex, TError error)
{
    auto this_ = MakeStrong(this);

    if (!error.IsOK()) {
        LOG_WARNING("Replication failed (BlockIndex: %d)\n%s",
            blockIndex,
            ~error.ToString());

        State = EJobState::Failed;
        return;
    }

    if (blockIndex >= static_cast<int>(ChunkInfo.blocks_size())) {
        LOG_DEBUG("All blocks are enqueued for replication");

        Writer
            ->AsyncClose(std::vector<TSharedRef>(), ChunkInfo.attributes())
            ->Subscribe(BIND([=] (TError error) {
                if (error.IsOK()) {
                    LOG_DEBUG("Replication job completed");

                    this_->Writer.Reset();
                    this_->State = EJobState::Completed;
                } else {
                    LOG_WARNING("Replication job failed\n%s", ~error.ToString());

                    this_->Writer.Reset();
                    this_->State = EJobState::Failed;
                }
            })
            .Via(CancelableInvoker));
        return;
    }

    TBlockId blockId(Chunk->GetId(), blockIndex);

    LOG_DEBUG("Retrieving block for replication (BlockIndex: %d)",
        blockIndex);

    Owner
        ->BlockStore
        ->GetBlock(blockId)
        ->Subscribe(
            BIND([=] (TBlockStore::TGetBlockResult result) {
                if (!result.IsOK()) {
                    LOG_WARNING("Error getting block for replication (BlockIndex: %d)\n%s",
                        blockIndex,
                        ~result.ToString());

                    this_->State = EJobState::Failed;
                    return;
                } 

                std::vector<TSharedRef> blocks;
                blocks.push_back(result.Value()->GetData());
                this_->Writer->AsyncWriteBlocks(MoveRV(blocks))->Subscribe(
                    BIND(
                        &TJob::ReplicateBlock,
                        this_,
                        blockIndex + 1)
                    .Via(CancelableInvoker));
            })
            .Via(CancelableInvoker));
}

////////////////////////////////////////////////////////////////////////////////

TJobExecutor::TJobExecutor(
    TChunkHolderConfigPtr config,
    TChunkStorePtr chunkStore,
    TBlockStorePtr blockStore,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , ChunkStore(chunkStore)
    , BlockStore(blockStore)
    , ServiceInvoker(serviceInvoker)
{
    YASSERT(chunkStore);
    YASSERT(blockStore);
    YASSERT(serviceInvoker);
}

TJobPtr TJobExecutor::StartJob(
    EJobType jobType,
    const TJobId& jobId,
    TStoredChunkPtr chunk,
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

void TJobExecutor::StopJob(TJobPtr job)
{
    job->Stop();
    YVERIFY(Jobs.erase(job->GetJobId()) == 1);
    
    LOG_INFO("Job stopped (JobId: %s, State: %s)",
        ~job->GetJobId().ToString(),
        ~job->GetState().ToString());
}

TJobPtr TJobExecutor::FindJob(const TJobId& jobId)
{
    auto it = Jobs.find(jobId);
    return it == Jobs.end() ? NULL : it->second;
}

yvector<TJobPtr> TJobExecutor::GetAllJobs()
{
    yvector<TJobPtr> result;
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

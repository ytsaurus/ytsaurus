#include "stdafx.h"
#include "private.h"
#include "job_executor.h"
#include "block_store.h"
#include "chunk_store.h"
#include "chunk.h"
#include "config.h"
#include "location.h"
#include "chunk_meta_extensions.h"

#include <ytlib/misc/string.h>
#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/async_writer.h>


namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobExecutorPtr owner,
    IInvokerPtr controlInvoker,
    EJobType jobType,
    const TJobId& jobId,
    TStoredChunkPtr chunk,
    const std::vector<Stroka>& targetAddresses)
    : Owner(owner)
    , JobType(jobType)
    , JobId(jobId)
    , State(EJobState::Running)
    , Chunk(chunk)
    , TargetAddresses(targetAddresses)
    , CancelableContext(New<TCancelableContext>())
    , CancelableInvoker(CancelableContext->CreateInvoker(controlInvoker))
    , Logger(DataNodeLogger)
{
    YCHECK(controlInvoker);
    YCHECK(chunk);

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

std::vector<Stroka> TJob::GetTargetAddresses() const
{
    return TargetAddresses;
}

TChunkPtr TJob::GetChunk() const
{
    return Chunk;
}

void TJob::Start()
{
    switch (JobType) {
        case EJobType::Remove:
            RunRemove();
            break;

        case EJobType::Replicate:
            RunReplicate();
            break;

        default:
            YUNREACHABLE();
    }
}

void TJob::Stop()
{
    CancelableContext->Cancel();
    Writer.Reset();
}

void TJob::RunRemove()
{
    LOG_INFO("Removal job started (ChunkId: %s)",
        ~Chunk->GetId().ToString());

    Owner->ChunkStore->RemoveChunk(Chunk);

    LOG_DEBUG("Removal job completed");

    State = EJobState::Completed;
}

void TJob::RunReplicate()
{
    LOG_INFO("Replication job started (TargetAddresses: [%s], ChunkId: %s)",
        ~JoinToString(TargetAddresses),
        ~Chunk->GetId().ToString());

    auto this_ = MakeStrong(this);
    Chunk
        ->GetMeta()
        .Subscribe(BIND([=] (IAsyncReader::TGetMetaResult result) {
            if (!result.IsOK()) {
                LOG_WARNING("Error getting chunk meta (ChunkId: %s)\n%s",
                    ~Chunk->GetId().ToString(),
                    ~result.ToString());

                this_->State = EJobState::Failed;
                return;
            }

            LOG_INFO("Received chunk meta for replication (ChunkId: %s)",
                ~Chunk->GetId().ToString());

            this_->ChunkMeta = result.Value();

            this_->Writer = New<TRemoteWriter>(
                this_->Owner->Config->ReplicationRemoteWriter,
                this_->Chunk->GetId(),
                this_->TargetAddresses);
            this_->Writer->Open();

            ReplicateBlock(0, TError());
        })
        .Via(CancelableInvoker));
}

void TJob::ReplicateBlock(int blockIndex, TError error)
{
    auto this_ = MakeStrong(this);

    if (!error.IsOK()) {
        LOG_WARNING("Replication failed\n%s", ~error.ToString());

        State = EJobState::Failed;
        return;
    }

    auto blocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());
    if (blockIndex >= static_cast<int>(blocksExt.blocks_size())) {
        LOG_DEBUG("All blocks are enqueued for replication");

        Writer
            ->AsyncClose(ChunkMeta)
            .Subscribe(BIND([=] (TError error) {
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

    LOG_DEBUG("Retrieving block for replication (BlockIndex: %d)", blockIndex);

    Owner
        ->BlockStore
        ->GetBlock(blockId, false)
        .Subscribe(
            BIND([=] (TBlockStore::TGetBlockResult result) {
                if (!result.IsOK()) {
                    LOG_WARNING("Error getting block %d for replication\n%s",
                        blockIndex,
                        ~result.ToString());

                    State = EJobState::Failed;
                    return;
                } 

                auto block = result.Value()->GetData();
                auto nextBlockIndex = blockIndex;
                if (Writer->TryWriteBlock(block)) {
                    ++nextBlockIndex;
                }

                Writer->GetReadyEvent().Subscribe(BIND(
                    &TJob::ReplicateBlock,
                    this_,
                    nextBlockIndex)
                .Via(CancelableInvoker));
            })
            .Via(CancelableInvoker));
}

////////////////////////////////////////////////////////////////////////////////

TJobExecutor::TJobExecutor(
    TDataNodeConfigPtr config,
    TChunkStorePtr chunkStore,
    TBlockStorePtr blockStore,
    IInvokerPtr controlInvoker)
    : Config(config)
    , ChunkStore(chunkStore)
    , BlockStore(blockStore)
    , ControlInvoker(controlInvoker)
{
    YCHECK(chunkStore);
    YCHECK(blockStore);
    YCHECK(controlInvoker);
}

TJobPtr TJobExecutor::StartJob(
    EJobType jobType,
    const TJobId& jobId,
    TStoredChunkPtr chunk,
    const std::vector<Stroka>& targetAddresses)
{
    auto job = New<TJob>(
        this,
        ControlInvoker,
        jobType,
        jobId,
        chunk,
        targetAddresses);
    YCHECK(Jobs.insert(MakePair(jobId, job)).second);
    job->Start();

    return job;
}

void TJobExecutor::StopJob(TJobPtr job)
{
    job->Stop();
    YCHECK(Jobs.erase(job->GetJobId()) == 1);
    
    LOG_INFO("Job stopped (JobId: %s, State: %s)",
        ~job->GetJobId().ToString(),
        ~job->GetState().ToString());
}

TJobPtr TJobExecutor::FindJob(const TJobId& jobId)
{
    auto it = Jobs.find(jobId);
    return it == Jobs.end() ? NULL : it->second;
}

std::vector<TJobPtr> TJobExecutor::GetAllJobs()
{
    std::vector<TJobPtr> result;
    FOREACH (const auto& pair, Jobs) {
        result.push_back(pair.second);
    }
    return result;
}

void TJobExecutor::StopAllJobs()
{
    FOREACH (auto& pair, Jobs) {
        pair.second->Stop();
    }
    Jobs.clear();

    LOG_INFO("All jobs stopped");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

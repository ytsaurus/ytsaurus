#include "stdafx.h"
#include "private.h"
#include "job_executor.h"
#include "block_store.h"
#include "chunk_store.h"
#include "chunk.h"
#include "config.h"
#include "location.h"
#include "chunk_meta_extensions.h"
#include "bootstrap.h"

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
    TBootstrap* bootstrap,
    EJobType jobType,
    const TJobId& jobId,
    const TChunkId& chunkId,
    const std::vector<Stroka>& targetAddresses)
    : Bootstrap(bootstrap)
    , JobType(jobType)
    , JobId(jobId)
    , State(EJobState::Running)
    , ChunkId(chunkId)
    , TargetAddresses(targetAddresses)
    , CancelableContext(New<TCancelableContext>())
    , CancelableInvoker(CancelableContext->CreateInvoker(Bootstrap->GetControlInvoker()))
    , Logger(DataNodeLogger)
{
    YCHECK(bootstrap);

    Logger.AddTag(Sprintf("ChunkId: %s, JobId: %s",
        ~ChunkId.ToString(),
        ~JobId.ToString()));
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
    Chunk = Bootstrap->GetChunkStore()->FindChunk(ChunkId);
    if (!Chunk) {
        SetFailed(TError("No such chunk %s", ~ChunkId.ToString()));
        return;
    }


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
    LOG_INFO("Removal job started",
        ~Chunk->GetId().ToString());

    Bootstrap->GetChunkStore()->RemoveChunk(Chunk);

    SetCompleted();
}

void TJob::RunReplicate()
{
    LOG_INFO("Replication job started (TargetAddresses: [%s])",
        ~JoinToString(TargetAddresses),
        ~Chunk->GetId().ToString());

    auto this_ = MakeStrong(this);
    Chunk
        ->GetMeta()
        .Subscribe(BIND([=] (IAsyncReader::TGetMetaResult result) {
            if (!result.IsOK()) {
                this_->SetFailed(TError(
                    "Error getting chunk meta\n%s",
                    ~Chunk->GetId().ToString(),
                    ~result.ToString()));
                return;
            }

            LOG_INFO("Chunk meta received");

            this_->ChunkMeta = result.Value();

            this_->Writer = New<TRemoteWriter>(
                this_->Bootstrap->GetConfig()->ReplicationRemoteWriter,
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
        SetFailed(error);
        return;
    }

    auto blocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());
    if (blockIndex >= static_cast<int>(blocksExt.blocks_size())) {
        LOG_DEBUG("All blocks are enqueued for replication");

        Writer
            ->AsyncClose(ChunkMeta)
            .Subscribe(BIND([=] (TError error) {
                this_->Writer.Reset();
                if (error.IsOK()) {
                    this_->SetCompleted();
                } else {
                    this_->SetFailed(error);
                }
            })
            .Via(CancelableInvoker));
        return;
    }

    TBlockId blockId(Chunk->GetId(), blockIndex);

    LOG_DEBUG("Retrieving block for replication (BlockIndex: %d)", blockIndex);

    Bootstrap
        ->GetBlockStore()
        ->GetBlock(blockId, false)
        .Subscribe(
            BIND([=] (TBlockStore::TGetBlockResult result) {
                if (!result.IsOK()) {
                    this_->SetFailed(TError(
                        "Error getting block %d for replication\n%s",
                        blockIndex,
                        ~result.ToString()));
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

void TJob::SetCompleted()
{
    State = EJobState::Completed;
    LOG_INFO("Job completed");
}

void TJob::SetFailed(const TError& error)
{
    State = EJobState::Failed;
    LOG_ERROR("Job failed\n%s", ~error.ToString());
}

////////////////////////////////////////////////////////////////////////////////

TJobExecutor::TJobExecutor(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{
    YCHECK(bootstrap);
}

TJobPtr TJobExecutor::StartJob(
    EJobType jobType,
    const TJobId& jobId,
    const TChunkId& chunkId,
    const std::vector<Stroka>& targetAddresses)
{
    auto job = New<TJob>(
        Bootstrap,
        jobType,
        jobId,
        chunkId,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

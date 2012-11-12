#include "stdafx.h"
#include "private.h"
#include "job_executor.h"
#include "block_store.h"
#include "chunk_store.h"
#include "chunk.h"
#include "config.h"
#include "location.h"
#include "bootstrap.h"
#include "chunk_registry.h"

#include <ytlib/misc/string.h>

#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NChunkClient::NProto;

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
    LOG_INFO("Removal job started");

    auto chunk = Bootstrap->GetChunkStore()->FindChunk(ChunkId);
    if (!chunk) {
        SetFailed(TError("No such chunk: %s", ~ChunkId.ToString()));
        return;
    }

    Bootstrap->GetChunkStore()->RemoveChunk(chunk).Subscribe(BIND(
        &TJob::SetCompleted, 
        MakeStrong(this)));
}

void TJob::RunReplicate()
{
    LOG_INFO("Replication job started (TargetAddresses: [%s])",
        ~JoinToString(TargetAddresses));

    auto chunk = Bootstrap->GetChunkRegistry()->FindChunk(ChunkId);
    if (!chunk) {
        SetFailed(TError("No such chunk: %s", ~ChunkId.ToString()));
        return;
    }

    auto this_ = MakeStrong(this);
    chunk->GetMeta().Subscribe(BIND([=] (IAsyncReader::TGetMetaResult result) {
        if (!result.IsOK()) {
            this_->SetFailed(TError(
                "Error getting meta of chunk: %s",
                ~ToString(chunk->GetId()))
                << result);
            return;
        }

        LOG_INFO("Chunk meta received");

        this_->ChunkMeta = result.Value();

        this_->Writer = New<TRemoteWriter>(
            this_->Bootstrap->GetConfig()->ReplicationRemoteWriter,
            this_->ChunkId,
            this_->TargetAddresses);
        this_->Writer->Open();

        ReplicateBlock(0, TError());
    }).Via(CancelableInvoker));
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

        Writer->AsyncClose(ChunkMeta).Subscribe(BIND([=] (TError error) {
            this_->Writer.Reset();
            if (error.IsOK()) {
                this_->SetCompleted();
            } else {
                this_->SetFailed(error);
            }
        }).Via(CancelableInvoker));
        return;
    }

    TBlockId blockId(ChunkId, blockIndex);

    LOG_DEBUG("Retrieving block for replication (BlockIndex: %d)", blockIndex);

    Bootstrap
        ->GetBlockStore()
        ->GetBlock(blockId, false)
        .Subscribe(
            BIND([=] (TBlockStore::TGetBlockResult result) {
                if (!result.IsOK()) {
                    this_->SetFailed(TError(
                        "Error retrieving block %d for replication",
                        ~blockId.ToString())
                        << result);
                    return;
                } 

                auto block = result.Value()->GetData();
                auto nextBlockIndex = blockIndex;
                if (Writer->TryWriteBlock(block)) {
                    ++nextBlockIndex;
                }

                Writer->GetReadyEvent().Subscribe(
                    BIND(&TJob::ReplicateBlock, this_, nextBlockIndex)
                    .Via(CancelableInvoker));
            }).Via(CancelableInvoker));
}

void TJob::SetCompleted()
{
    State = EJobState::Completed;
    LOG_INFO("Job completed");
}

void TJob::SetFailed(const TError& error)
{
    State = EJobState::Failed;
    LOG_ERROR(error, "Job failed");
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

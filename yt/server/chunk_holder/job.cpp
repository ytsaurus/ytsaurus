#include "stdafx.h"
#include "job.h"
#include "chunk.h"
#include "chunk_store.h"
#include "block_store.h"
#include "location.h"
#include "config.h"
#include "private.h"

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/actions/cancelable_context.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/job.pb.h>

#include <server/job_agent/job.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NChunkHolder {

using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NCellNode;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;

using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TJobBase
    : public IJob
{
    DEFINE_SIGNAL(void(), ResourcesReleased);

public:
    TJobBase(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : JobId(jobId)
        , Config(config)
        , Bootstrap(bootstrap)
        , Logger(DataNodeLogger)
        , JobState(EJobState::Waiting)
        , JobPhase(EJobPhase::Created)
        , Progress(0.0)
        , CancelableContext(New<TCancelableContext>())
        , CancelableInvoker(CancelableContext->CreateInvoker(Bootstrap->GetControlInvoker()))
    {
        JobSpec.Swap(&jobSpec);

        Logger.AddTag(Sprintf("JobId: %s", ~ToString(jobId)));
    }

    virtual void Start() override
    {
        JobState = EJobState::Running;
        JobPhase = EJobPhase::Running;

        const auto& chunkSpecExt = JobSpec.GetExtension(TChunkJobSpecExt::chunk_job_spec_ext);
        auto chunkId = FromProto<TChunkId>(chunkSpecExt.chunk_id());

        auto chunkStore = Bootstrap->GetChunkStore();
        Chunk = chunkStore->FindChunk(chunkId);
        if (!Chunk) {
            SetFailed(TError("No such chunk %s", ~ToString(chunkId)));
            return;
        }
        
        DoStart();
    }

    virtual void Abort(const TError& error) override
    {
        if (JobState != EJobState::Running)
            return;

        CancelableContext->Cancel();
        SetAborted(error);
    }

    virtual const TJobId& GetId() const override
    {
        return JobId;
    }

    virtual const TJobSpec& GetSpec() const override
    {
        return JobSpec;
    }

    virtual EJobState GetState() const override
    {
        return JobState;
    }

    virtual EJobPhase GetPhase() const override
    {
        return JobPhase;
    }

    virtual TNodeResources GetResourceUsage() const override
    {
        return ResourceLimits;
    }

    virtual void SetResourceUsage(const TNodeResources& /*newUsage*/) override
    {
        YUNREACHABLE();
    }

    virtual TJobResult GetResult() const override
    {
        return Result;
    }

    virtual void SetResult(const TJobResult& /*result*/) override
    {
        YUNREACHABLE();
    }

    virtual double GetProgress() const override
    {
        return Progress;
    }

    virtual void SetProgress(double value) override
    {
        Progress = value;
    }

protected:
    TJobId JobId;
    TJobSpec JobSpec;
    TNodeResources ResourceLimits;
    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;

    NLog::TTaggedLogger Logger;

    EJobState JobState;
    EJobPhase JobPhase;

    double Progress;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableInvoker;

    TJobResult Result;

    TStoredChunkPtr Chunk;

    virtual void DoStart() = 0;


    void SetFinished(EJobState finalState, const TError& error)
    {
        if (JobState != EJobState::Running)
            return;

        JobPhase = EJobPhase::Finished;
        JobState = finalState;
        ToProto(Result.mutable_error(), error);

        Chunk.Reset();
        CancelableContext.Reset();
        CancelableInvoker.Reset();
    }

    void SetCompleted()
    {
        LOG_INFO("Job completed");
        Progress = 1.0;
        SetFinished(EJobState::Completed, TError());
    }

    void SetFailed(const TError& error)
    {
        LOG_ERROR(error, "Job failed");
        SetFinished(EJobState::Failed, error);
    }

    void SetAborted(const TError& error)
    {
        LOG_INFO(error, "Job aborted");
        SetFinished(EJobState::Aborted, error);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TRemovalJob
    : public TJobBase
{
public:
    TRemovalJob(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
    { }

private:
    virtual void DoStart() override
    {
        auto chunkStore = Bootstrap->GetChunkStore();
        chunkStore->RemoveChunk(Chunk).Subscribe(BIND(
            &TRemovalJob::OnChunkRemoved,
            MakeStrong(this)));
    }

    void OnChunkRemoved()
    {
        SetCompleted();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TReplicationJob
    : public TJobBase
{
public:
    TReplicationJob(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
        , ReplicationJobSpecExt(JobSpec.GetExtension(TReplicationJobSpecExt::replication_job_spec_ext))
        , CurrentBlockIndex(0)
    { }

private:
    TReplicationJobSpecExt ReplicationJobSpecExt;

    int CurrentBlockIndex;
    TChunkMeta ChunkMeta;
    TBlocksExt BlocksExt;
    IAsyncWriterPtr Writer;

    virtual void DoStart() override
    {
        LOG_INFO("Retrieving chunk meta");

        Chunk->GetMeta().Subscribe(
            BIND(&TReplicationJob::OnGotChunkMeta, MakeStrong(this))
                .Via(CancelableInvoker));
    }

    void OnGotChunkMeta(IAsyncReader::TGetMetaResult result)
    {
        if (!result.IsOK()) {
            SetFailed(TError("Error getting meta of chunk %s",
                ~ToString(Chunk->GetId()))
                << result);
            return;
        }

        LOG_INFO("Chunk meta received");

        ChunkMeta = result.Value();
        BlocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());

        auto targets = FromProto<TNodeDescriptor>(ReplicationJobSpecExt.target_descriptors());

        Writer = CreateReplicationWriter(
            Config->ReplicationWriter,
            Chunk->GetId(),
            targets);
        Writer->Open();

        ReplicateBlock(TError());
    }

    void ReplicateBlock(TError error)
    {
        if (!error.IsOK()) {
            SetFailed(error);
            return;
        }

        SetProgress((double) CurrentBlockIndex / BlocksExt.blocks_size());

        auto this_ = MakeStrong(this);

        auto blocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());
        if (CurrentBlockIndex >= static_cast<int>(blocksExt.blocks_size())) {
            LOG_DEBUG("All blocks are enqueued for replication");

            Writer->AsyncClose(ChunkMeta).Subscribe(
                BIND(&TReplicationJob::OnWriterClosed, this_)
                    .Via(CancelableInvoker));
            return;
        }

        LOG_DEBUG("Retrieving block %d for replication", CurrentBlockIndex);

        TBlockId blockId(Chunk->GetId(), CurrentBlockIndex);
        auto blockStore = Bootstrap->GetBlockStore();
        blockStore->GetBlock(blockId, false).Subscribe(
            BIND(&TReplicationJob::OnWriterReady, this_)
                .Via(CancelableInvoker));
    }

    void OnWriterClosed(TError error)
    {
        if (!error.IsOK()) {
            SetFailed(error);
            return;
        }

        SetCompleted();
    }

    void OnWriterReady(TBlockStore::TGetBlockResult result)
    {
        if (!result.IsOK()) {
            SetFailed(result);
            return;
        }

        auto block = result.Value()->GetData();
        if (Writer->TryWriteBlock(block)) {
            ++CurrentBlockIndex;
        }

        auto this_ = MakeStrong(this);
        Writer->GetReadyEvent().Subscribe(
            BIND(&TReplicationJob::ReplicateBlock, this_)
                .Via(CancelableInvoker));
    }

};

////////////////////////////////////////////////////////////////////////////////

class TRepairJob
    : public TJobBase
{
public:
    TRepairJob(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
        , RepairJobSpecExt(JobSpec.GetExtension(TRepairJobSpecExt::repair_job_spec_ext))
    { }

private:
    TRepairJobSpecExt RepairJobSpecExt;

    virtual void DoStart() override
    {
    }

};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateChunkJob(
    const TJobId& jobId,
    TJobSpec&& jobSpec,
    const TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
{
    auto type = EJobType(jobSpec.type());
    switch (type) {
        case EJobType::ReplicateChunk:
            return New<TReplicationJob>(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                config,
                bootstrap);

        case EJobType::RemoveChunk:
            return New<TRemovalJob>(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                config,
                bootstrap);

        case EJobType::RepairChunk:
            return New<TRepairJob>(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                config,
                bootstrap);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT


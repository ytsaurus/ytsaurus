#include "stdafx.h"
#include "job.h"
#include "chunk.h"
#include "chunk_store.h"
#include "block_store.h"
#include "location.h"
#include "config.h"
#include "private.h"

#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/misc/string.h>

#include <ytlib/erasure/codec.h>

#include <ytlib/actions/cancelable_context.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/node_tracker_client/helpers.h>
#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/erasure_reader.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/job.pb.h>

#include <server/job_agent/job.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

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

class TChunkJobBase
    : public IJob
{
    DEFINE_SIGNAL(void(), ResourcesReleased);

public:
    TChunkJobBase(
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

        DoPrepare();

        if (JobState != EJobState::Running)
            return;

        DoRun();
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

    TChunkId ChunkId;


    virtual void DoPrepare()
    {
        const auto& chunkSpecExt = JobSpec.GetExtension(TChunkJobSpecExt::chunk_job_spec_ext);
        ChunkId = FromProto<TChunkId>(chunkSpecExt.chunk_id());

        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId)));
    }

    virtual void DoRun() = 0;


    void SetCompleted()
    {
        LOG_INFO("Job completed");
        Progress = 1.0;
        DoSetFinished(EJobState::Completed, TError());
    }

    void SetFailed(const TError& error)
    {
        LOG_ERROR(error, "Job failed");
        DoSetFinished(EJobState::Failed, error);
    }

    void SetAborted(const TError& error)
    {
        LOG_INFO(error, "Job aborted");
        DoSetFinished(EJobState::Aborted, error);
    }

    void SetFinished(const TError& error)
    {
        if (error.IsOK()) {
            SetCompleted();
        } else {
            SetFailed(error);
        }
    }

private:
    void DoSetFinished(EJobState finalState, const TError& error)
    {
        if (JobState != EJobState::Running)
            return;

        JobPhase = EJobPhase::Finished;
        JobState = finalState;
        ToProto(Result.mutable_error(), error);

        CancelableContext.Reset();
        CancelableInvoker.Reset();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkJobBase
    : public TChunkJobBase
{
public:
    TLocalChunkJobBase(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TChunkJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
    { }

protected:
    virtual void DoPrepare()
    {
        TChunkJobBase::DoPrepare();

        auto chunkStore = Bootstrap->GetChunkStore();
        Chunk = chunkStore->FindChunk(ChunkId);
        if (!Chunk) {
            SetFailed(TError("Chunk %s does not exists on node", ~ToString(ChunkId)));
            return;
        }
    }

    TStoredChunkPtr Chunk;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkRemovalJob
    : public TLocalChunkJobBase
{
public:
    TChunkRemovalJob(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TLocalChunkJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
    { }

private:
    virtual void DoRun() override
    {
        auto chunkStore = Bootstrap->GetChunkStore();
        chunkStore->RemoveChunk(Chunk).Subscribe(BIND(
            &TChunkRemovalJob::OnChunkRemoved,
            MakeStrong(this)));
    }

    void OnChunkRemoved()
    {
        SetCompleted();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicationJob
    : public TLocalChunkJobBase
{
public:
    TChunkReplicationJob(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TLocalChunkJobBase(
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

    virtual void DoRun() override
    {
        LOG_INFO("Retrieving chunk meta");

        Chunk->GetMeta(0).Subscribe(
            BIND(&TChunkReplicationJob::OnGotChunkMeta, MakeStrong(this))
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
            targets,
            Bootstrap->GetReplicationOutThrottler());
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
                BIND(&TChunkReplicationJob::OnWriterClosed, this_)
                    .Via(CancelableInvoker));
            return;
        }

        LOG_DEBUG("Retrieving block %d for replication", CurrentBlockIndex);

        TBlockId blockId(Chunk->GetId(), CurrentBlockIndex);
        auto blockStore = Bootstrap->GetBlockStore();
        blockStore->GetBlock(blockId, 0, false).Subscribe(
            BIND(&TChunkReplicationJob::OnWriterReady, this_)
                .Via(CancelableInvoker));
    }

    void OnWriterReady(TBlockStore::TGetBlockResult result)
    {
        if (!result.IsOK()) {
            SetFailed(result);
            return;
        }

        auto block = result.Value()->GetData();

        Writer->WriteBlock(block);
        ++CurrentBlockIndex;

        auto this_ = MakeStrong(this);
        Writer->GetReadyEvent().Subscribe(
            BIND(&TChunkReplicationJob::ReplicateBlock, this_)
                .Via(CancelableInvoker));
    }

    void OnWriterClosed(TError error)
    {
        SetFinished(error);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkRepairJob
    : public TChunkJobBase
{
public:
    TChunkRepairJob(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TChunkJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
        , RepairJobSpecExt(JobSpec.GetExtension(TRepairJobSpecExt::repair_job_spec_ext))
    { }

private:
    TRepairJobSpecExt RepairJobSpecExt;

    virtual void DoRun() override
    {
        auto codecId = NErasure::ECodec(RepairJobSpecExt.erasure_codec());
        auto* codec = NErasure::GetCodec(codecId);

        auto replicas = FromProto<NChunkClient::TChunkReplica>(RepairJobSpecExt.replicas());
        auto targets = FromProto<TNodeDescriptor>(RepairJobSpecExt.target_descriptors());
        auto erasedIndexes = FromProto<int, NErasure::TPartIndexList>(RepairJobSpecExt.erased_indexes());
        YCHECK(targets.size() == erasedIndexes.size());

        // Compute repair plan.
        auto repairIndexes = codec->GetRepairIndices(erasedIndexes);
        if (!repairIndexes) {
            SetFailed(TError("Codec is unable to repair the chunk"));
            return;
        }

        LOG_INFO("Preparing to repair (ErasedIndexes: [%s], RepairIndexes: [%s], Targets: [%s])",
            ~JoinToString(erasedIndexes),
            ~JoinToString(*repairIndexes),
            ~JoinToString(targets));

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(RepairJobSpecExt.node_directory());

        auto config = Bootstrap->GetConfig()->DataNode;

        std::vector<IAsyncReaderPtr> readers;
        FOREACH (int partIndex, *repairIndexes) {
            TChunkReplicaList partReplicas;
            FOREACH (auto replica, replicas) {
                if (replica.GetIndex() == partIndex) {
                    partReplicas.push_back(replica);
                }
            }
            YCHECK(!partReplicas.empty());

            auto partId = ErasurePartIdFromChunkId(ChunkId, partIndex);
            auto reader = CreateReplicationReader(
                config->ReplicationReader,
                Bootstrap->GetBlockStore()->GetBlockCache(),
                Bootstrap->GetMasterChannel(),
                nodeDirectory,
                Bootstrap->GetLocalDescriptor(),
                partId,
                partReplicas,
                Bootstrap->GetRepairInThrottler());
            readers.push_back(reader);
        }

        std::vector<IAsyncWriterPtr> writers;
        for (int index = 0; index < static_cast<int>(erasedIndexes.size()); ++index) {
            int partIndex = erasedIndexes[index];
            const auto& target = targets[index];
            auto partId = ErasurePartIdFromChunkId(ChunkId, partIndex);
            auto writer = CreateReplicationWriter(
                config->ReplicationWriter,
                partId,
                std::vector<TNodeDescriptor>(1, target),
                Bootstrap->GetRepairOutThrottler());
            writers.push_back(writer);
        }

        RepairErasedBlocks(
            codec,
            erasedIndexes,
            readers,
            writers,
            CancelableContext,
            BIND(&TChunkRepairJob::OnProgress, MakeWeak(this)).Via(CancelableInvoker))
        .Subscribe(BIND(&TChunkRepairJob::OnRepaired, MakeStrong(this))
            .Via(CancelableInvoker));
    }

    void OnRepaired(TError error)
    {
        SetFinished(error);
    }

    void OnProgress(double progress)
    {
        SetProgress(progress);
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
            return New<TChunkReplicationJob>(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                config,
                bootstrap);

        case EJobType::RemoveChunk:
            return New<TChunkRemovalJob>(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                config,
                bootstrap);

        case EJobType::RepairChunk:
            return New<TChunkRepairJob>(
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


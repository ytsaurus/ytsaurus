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

        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(chunkId)));

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
            SetFailed(error);
        } else {
            SetCompleted();
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

        Chunk.Reset();
        CancelableContext.Reset();
        CancelableInvoker.Reset();
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
        auto codecId = NErasure::ECodec(RepairJobSpecExt.erasure_codec());
        auto* codec = NErasure::GetCodec(codecId);

        auto replicas = FromProto<NChunkClient::TChunkReplica>(RepairJobSpecExt.replicas());
        auto targets = FromProto<TNodeDescriptor>(RepairJobSpecExt.target_descriptors());

        int totalBlockCount = codec->GetTotalBlockCount();
        NErasure::TBlockIndexSet erasedIndexSet((1 << totalBlockCount) - 1);

        // Figure out erased parts.
        FOREACH (auto replica, replicas) {
            int index = replica.GetIndex();
            erasedIndexSet.reset(index);
        }
        NErasure::TBlockIndexList erasedIndexList;
        for (int index = 0; index < totalBlockCount; ++index) {
            if (erasedIndexSet[index])  {
                erasedIndexList.push_back(index);
            }
        }

        // Compute repair plan.
        auto repairIndexes = codec->GetRepairIndices(erasedIndexList);
        if (!repairIndexes) {
            SetFailed(TError("Codec is unable to repair the chunk"));
            return;
        }

        // TODO(babenko): move to RepairErasedBlocks
        LOG_INFO("Preparing to repair (ErasedIndexes: [%s], RepairIndexes: [%s])",
            ~JoinToString(erasedIndexList),
            ~JoinToString(*repairIndexes));

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(RepairJobSpecExt.node_directory());

        auto config = Bootstrap->GetConfig()->DataNode;

        std::vector<IAsyncReaderPtr> readers;
        FOREACH (int index, *repairIndexes) {
            TChunkReplicaList partReplicas;
            FOREACH (auto replica, replicas) {
                if (replica.GetIndex() == index) {
                    partReplicas.push_back(replica);
                }
            }

            auto reader = CreateReplicationReader(
                config->ReplicationReader,
                Bootstrap->GetBlockStore()->GetBlockCache(),
                Bootstrap->GetMasterChannel(),
                nodeDirectory,
                Bootstrap->GetLocalDescriptor(),
                Chunk->GetId(),
                partReplicas);
            readers.push_back(reader);
        }

        std::vector<IAsyncWriterPtr> writers;
        FOREACH (int index, erasedIndexList) {
            auto writer = CreateReplicationWriter(
                config->ReplicationWriter,
                Chunk->GetId(),
                targets);
            writers.push_back(writer);            
        }

        RepairErasedBlocks(
            codec,
            erasedIndexList,
            readers,
            writers)
        .Subscribe(BIND(&TRepairJob::OnRepaired, MakeStrong(this))
            .Via(CancelableInvoker));
    }

    void OnRepaired(TError error)
    {
        SetFinished(error);
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


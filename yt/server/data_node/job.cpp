#include "stdafx.h"
#include "job.h"
#include "chunk.h"
#include "chunk_store.h"
#include "block_store.h"
#include "location.h"
#include "config.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>

#include <core/erasure/codec.h>

#include <core/concurrency/fiber.h>

#include <core/actions/cancelable_context.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/node_tracker_client/helpers.h>
#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/erasure_reader.h>
#include <ytlib/chunk_client/job.pb.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/replication_reader.h>

#include <ytlib/api/client.h>

#include <server/job_agent/job.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NDataNode {

using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NCellNode;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

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
        , ResourceLimits(resourceLimits)
        , Config(config)
        , Bootstrap(bootstrap)
        , Logger(DataNodeLogger)
        , JobState(EJobState::Waiting)
        , JobPhase(EJobPhase::Created)
        , Progress(0.0)
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

        JobFuture = BIND(&TChunkJobBase::GuardedRun, MakeStrong(this))
            .AsyncVia(Bootstrap->GetControlInvoker())
            .Run();
    }

    virtual void Abort(const TError& error) override
    {
        if (JobState != EJobState::Running)
            return;

        JobFuture.Cancel();
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

    virtual TJobStatistics GetJobStatistics() const override
    {
        // ToDo(psushin)
        return ZeroJobStatistics();
    }

    virtual void SetJobStatistics(const TJobStatistics& statistics) override
    {
        YUNREACHABLE();
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

    TFuture<void> JobFuture;

    TJobResult Result;

    TChunkId ChunkId;


    virtual void DoPrepare()
    {
        const auto& chunkSpecExt = JobSpec.GetExtension(TChunkJobSpecExt::chunk_job_spec_ext);
        ChunkId = FromProto<TChunkId>(chunkSpecExt.chunk_id());

        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId)));
    }

    virtual void DoRun() = 0;


    void GuardedRun()
    {
        try {
            DoRun();
            SetCompleted();
        } catch (const std::exception& ex) {
            SetFailed(ex);
        }
    }

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
        ToProto(Result.mutable_statistics(), GetJobStatistics());
        ResourceLimits = ZeroNodeResources();
        JobFuture.Reset();
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
        WaitFor(chunkStore->RemoveChunk(Chunk));
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
    { }

private:
    TReplicationJobSpecExt ReplicationJobSpecExt;

    virtual void DoRun() override
    {
        LOG_INFO("Retrieving chunk meta");

        auto metaOrError = WaitFor(Chunk->GetMeta(0));
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %s",
            ~ToString(ChunkId));

        LOG_INFO("Chunk meta received");
        const auto& chunkMeta = metaOrError.Value();
        const auto& blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta->extensions());

        auto targets = FromProto<TNodeDescriptor>(ReplicationJobSpecExt.target_descriptors());

        auto writer = CreateReplicationWriter(
            Config->ReplicationWriter,
            ChunkId,
            targets,
            EWriteSessionType::Replication,
            Bootstrap->GetReplicationOutThrottler());
        writer->Open();

        auto blockStore = Bootstrap->GetBlockStore();

        for (int index = 0; index < blocksExt.blocks_size(); ++index) {
            LOG_DEBUG("Retrieving block %d for replication", index);

            TBlockId blockId(ChunkId, index);

            auto blockOrError = WaitFor(blockStore->GetBlock(blockId, 0, false));
            THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError, "Error getting block %s for replication",
                ~ToString(blockId));

            auto block = blockOrError.Value()->GetData();
            auto result = writer->WriteBlock(block);
            if (!result) {
                auto error = WaitFor(writer->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error writing block %s for replication",
                    ~ToString(blockId));
            }
        }

        LOG_DEBUG("All blocks are enqueued for replication");

        {
            auto error = WaitFor(writer->AsyncClose(*chunkMeta));
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing replication writer");
        }
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
            THROW_ERROR_EXCEPTION("Codec is unable to repair the chunk");
        }

        LOG_INFO("Preparing to repair (ErasedIndexes: [%s], RepairIndexes: [%s], Targets: [%s])",
            ~JoinToString(erasedIndexes),
            ~JoinToString(*repairIndexes),
            ~JoinToString(targets));

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(RepairJobSpecExt.node_directory());

        auto config = Bootstrap->GetConfig()->DataNode;

        std::vector<IAsyncReaderPtr> readers;
        for (int partIndex : *repairIndexes) {
            TChunkReplicaList partReplicas;
            for (auto replica : replicas) {
                if (replica.GetIndex() == partIndex) {
                    partReplicas.push_back(replica);
                }
            }
            YCHECK(!partReplicas.empty());

            auto partId = ErasurePartIdFromChunkId(ChunkId, partIndex);
            auto reader = CreateReplicationReader(
                config->RepairReader,
                Bootstrap->GetBlockStore()->GetBlockCache(),
                Bootstrap->GetMasterClient()->GetMasterChannel(),
                nodeDirectory,
                Bootstrap->GetLocalDescriptor(),
                partId,
                partReplicas,
                EReadSessionType::Repair,
                Bootstrap->GetRepairInThrottler());
            readers.push_back(reader);
        }

        std::vector<IAsyncWriterPtr> writers;
        for (int index = 0; index < static_cast<int>(erasedIndexes.size()); ++index) {
            int partIndex = erasedIndexes[index];
            const auto& target = targets[index];
            auto partId = ErasurePartIdFromChunkId(ChunkId, partIndex);
            auto writer = CreateReplicationWriter(
                config->RepairWriter,
                partId,
                std::vector<TNodeDescriptor>(1, target),
                EWriteSessionType::Repair,
                Bootstrap->GetRepairOutThrottler());
            writers.push_back(writer);
        }

        auto onProgress =
            BIND(&TChunkRepairJob::SetProgress, MakeWeak(this))
                .Via(GetCurrentInvoker());

        auto asyncRepairError = RepairErasedParts(
            codec,
            erasedIndexes,
            readers,
            writers,
            onProgress);

        // Make sure the repair is canceled when the current fiber terminates.
        TFutureCancelationGuard<TError> repairErrorGuard(asyncRepairError);

        auto repairError = WaitFor(asyncRepairError);
        THROW_ERROR_EXCEPTION_IF_FAILED(repairError, "Error repairing chunk %s",
            ~ToString(ChunkId));
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

} // namespace NDataNode
} // namespace NYT


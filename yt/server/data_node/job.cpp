#include "stdafx.h"
#include "job.h"
#include "chunk.h"
#include "chunk_store.h"
#include "block_store.h"
#include "location.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>

#include <core/erasure/codec.h>

#include <core/concurrency/scheduler.h>

#include <core/actions/cancelable_context.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/node_tracker_client/helpers.h>
#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/chunk_client/writer.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/erasure_reader.h>
#include <ytlib/chunk_client/job.pb.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/replication_reader.h>

#include <ytlib/api/client.h>

#include <server/hydra/changelog.h>

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
public:
    DEFINE_SIGNAL(void(), ResourcesReleased);

public:
    TChunkJobBase(
        const TJobId& jobId,
        TJobSpec&& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : JobId_(jobId)
        , ResourceLimits_(resourceLimits)
        , Config_(config)
        , Bootstrap_(bootstrap)
        , Logger(DataNodeLogger)
        , JobState_(EJobState::Waiting)
        , JobPhase_(EJobPhase::Created)
        , Progress_(0.0)
    {
        JobSpec_.Swap(&jobSpec);

        Logger.AddTag(Sprintf("JobId: %s", ~ToString(jobId)));
    }

    virtual void Start() override
    {
        JobState_ = EJobState::Running;
        JobPhase_ = EJobPhase::Running;

        DoPrepare();

        if (JobState_ != EJobState::Running)
            return;

        JobFuture_ = BIND(&TChunkJobBase::GuardedRun, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run();
    }

    virtual void Abort(const TError& error) override
    {
        if (JobState_ != EJobState::Running)
            return;

        JobFuture_.Cancel();
        SetAborted(error);
    }

    virtual const TJobId& GetId() const override
    {
        return JobId_;
    }

    virtual const TJobSpec& GetSpec() const override
    {
        return JobSpec_;
    }

    virtual EJobState GetState() const override
    {
        return JobState_;
    }

    virtual EJobPhase GetPhase() const override
    {
        return JobPhase_;
    }

    virtual TNodeResources GetResourceUsage() const override
    {
        return ResourceLimits_;
    }

    virtual void SetResourceUsage(const TNodeResources& /*newUsage*/) override
    {
        YUNREACHABLE();
    }

    virtual TJobResult GetResult() const override
    {
        return Result_;
    }

    virtual void SetResult(const TJobResult& /*result*/) override
    {
        YUNREACHABLE();
    }

    virtual double GetProgress() const override
    {
        return Progress_;
    }

    virtual void SetProgress(double value) override
    {
        Progress_ = value;
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
    TJobId JobId_;
    TJobSpec JobSpec_;
    TNodeResources ResourceLimits_;
    TDataNodeConfigPtr Config_;
    TBootstrap* Bootstrap_;

    NLog::TTaggedLogger Logger;

    EJobState JobState_;
    EJobPhase JobPhase_;

    double Progress_;

    TFuture<void> JobFuture_;

    TJobResult Result_;

    TChunkId ChunkId_;


    virtual void DoPrepare()
    {
        const auto& chunkSpecExt = JobSpec_.GetExtension(TChunkJobSpecExt::chunk_job_spec_ext);
        ChunkId_ = FromProto<TChunkId>(chunkSpecExt.chunk_id());

        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId_)));
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
        Progress_ = 1.0;
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
        if (JobState_ != EJobState::Running)
            return;

        JobPhase_ = EJobPhase::Finished;
        JobState_ = finalState;
        ToProto(Result_.mutable_error(), error);
        ToProto(Result_.mutable_statistics(), GetJobStatistics());
        ResourceLimits_ = ZeroNodeResources();
        JobFuture_.Reset();
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
    IChunkPtr Chunk_;

    virtual void DoPrepare()
    {
        TChunkJobBase::DoPrepare();

        auto chunkStore = Bootstrap_->GetChunkStore();
        Chunk_ = chunkStore->FindChunk(ChunkId_);
        if (!Chunk_) {
            SetFailed(TError("Chunk_ %s is missing", ~ToString(ChunkId_)));
            return;
        }
    }

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
        auto chunkStore = Bootstrap_->GetChunkStore();
        WaitFor(chunkStore->RemoveChunk(Chunk_));
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
        , ReplicateChunkJobSpecExt_(JobSpec_.GetExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext))
    { }

private:
    TReplicateChunkJobSpecExt ReplicateChunkJobSpecExt_;

    virtual void DoRun() override
    {
        LOG_INFO("Retrieving chunk meta");

        auto metaOrError = WaitFor(Chunk_->GetMeta(0));
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %s",
            ~ToString(ChunkId_));

        LOG_INFO("Chunk_ meta received");
        const auto& chunkMeta = metaOrError.Value();
        const auto& blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta->extensions());

        auto targets = FromProto<TNodeDescriptor>(ReplicateChunkJobSpecExt_.targets());

        auto writer = CreateReplicationWriter(
            Config_->ReplicationWriter,
            ChunkId_,
            targets,
            EWriteSessionType::Replication,
            Bootstrap_->GetReplicationOutThrottler());
        writer->Open();

        auto blockStore = Bootstrap_->GetBlockStore();

        for (int index = 0; index < blocksExt.blocks_size(); ++index) {
            LOG_DEBUG("Retrieving block %d for replication", index);

            TBlockId blockId(ChunkId_, index);
            auto blockOrError = WaitFor(blockStore->GetBlocks(
                blockId.ChunkId,
                blockId.BlockIndex,
                1,
                0,
                false));
            THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError, "Error getting block %s for replication",
                ~ToString(blockId));

            const auto& block = blockOrError.Value()[0];
            auto result = writer->WriteBlock(block);
            if (!result) {
                auto error = WaitFor(writer->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error writing block %s for replication",
                    ~ToString(blockId));
            }
        }

        LOG_DEBUG("All blocks are enqueued for replication");

        {
            auto error = WaitFor(writer->Close(*chunkMeta));
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
        , RepairJobSpecExt_(JobSpec_.GetExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext))
    { }

private:
    TRepairChunkJobSpecExt RepairJobSpecExt_;

    virtual void DoRun() override
    {
        auto codecId = NErasure::ECodec(RepairJobSpecExt_.erasure_codec());
        auto* codec = NErasure::GetCodec(codecId);

        auto replicas = FromProto<NChunkClient::TChunkReplica>(RepairJobSpecExt_.replicas());
        auto targets = FromProto<TNodeDescriptor>(RepairJobSpecExt_.targets());
        auto erasedIndexes = FromProto<int, NErasure::TPartIndexList>(RepairJobSpecExt_.erased_indexes());
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
        nodeDirectory->MergeFrom(RepairJobSpecExt_.node_directory());

        auto config = Bootstrap_->GetConfig()->DataNode;

        std::vector<IReaderPtr> readers;
        for (int partIndex : *repairIndexes) {
            TChunkReplicaList partReplicas;
            for (auto replica : replicas) {
                if (replica.GetIndex() == partIndex) {
                    partReplicas.push_back(replica);
                }
            }
            YCHECK(!partReplicas.empty());

            auto partId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
            auto reader = CreateReplicationReader(
                config->RepairReader,
                Bootstrap_->GetBlockStore()->GetBlockCache(),
                Bootstrap_->GetMasterClient()->GetMasterChannel(),
                nodeDirectory,
                Bootstrap_->GetLocalDescriptor(),
                partId,
                partReplicas,
                DefaultNetworkName,
                EReadSessionType::Repair,
                Bootstrap_->GetRepairInThrottler());
            readers.push_back(reader);
        }

        std::vector<IWriterPtr> writers;
        for (int index = 0; index < static_cast<int>(erasedIndexes.size()); ++index) {
            int partIndex = erasedIndexes[index];
            const auto& target = targets[index];
            auto partId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
            auto writer = CreateReplicationWriter(
                config->RepairWriter,
                partId,
                std::vector<TNodeDescriptor>(1, target),
                EWriteSessionType::Repair,
                Bootstrap_->GetRepairOutThrottler());
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
            ~ToString(ChunkId_));
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSealChunkJob
    : public TLocalChunkJobBase
{
public:
    TSealChunkJob(
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
        , SealJobSpecExt_(JobSpec_.GetExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext))
    { }

private:
    TSealChunkJobSpecExt SealJobSpecExt_;

    virtual void DoRun() override
    {
        auto journalChunk = Chunk_->AsJournalChunk();

        if (journalChunk->IsActive()) {
            THROW_ERROR_EXCEPTION("Cannot seal an active journal chunk %s",
                ~ToString(ChunkId_));
        }

        auto journalDispatcher = Bootstrap_->GetJournalDispatcher();
        auto location = Chunk_->GetLocation();
        auto changelog = journalDispatcher->OpenChangelog(location, ChunkId_);

        if (changelog->IsSealed()) {
            LOG_INFO("Chunk %s is already sealed",
                ~ToString(ChunkId_));
            return;
        }

        journalChunk->AttachChangelog(changelog);
        try {
            int sealRecordCount = SealJobSpecExt_.record_count();
            if (changelog->GetRecordCount() < sealRecordCount) {
                THROW_ERROR_EXCEPTION("Record download is not implemented");
            }

            LOG_INFO("Started sealing journal chunk");
            {
                auto result = WaitFor(changelog->Seal(sealRecordCount));
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
            LOG_INFO("Finished sealing journal chunk");
        } catch (...) {
            journalChunk->DetachChangelog();
            throw;
        }
        journalChunk->DetachChangelog();

        auto chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->UpdateExistingChunk(Chunk_);
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

        case EJobType::SealChunk:
            return New<TSealChunkJob>(
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


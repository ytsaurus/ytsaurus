#include "stdafx.h"
#include "job.h"
#include "chunk.h"
#include "chunk_store.h"
#include "block_store.h"
#include "location.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "session_manager.h"
#include "session.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>

#include <core/erasure/codec.h>

#include <core/concurrency/scheduler.h>

#include <core/actions/cancelable_context.h>

#include <core/logging/log.h>

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

#include <ytlib/object_client/helpers.h>

#include <ytlib/api/client.h>

#include <server/hydra/changelog.h>

#include <server/job_agent/job.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NDataNode {

using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCellNode;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;

using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

static const int FetchPriority = 0;

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

        Logger.AddTag("JobId: %v", jobId);
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

    NLog::TLogger Logger;

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

        Logger.AddTag("ChunkId: %v", ChunkId_);
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
            SetFailed(TError("Chunk %v is missing", ChunkId_));
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
        auto sessionManager = Bootstrap_->GetSessionManager();
        auto session = sessionManager->FindSession(Chunk_->GetId());
        if (session) {
            session->Cancel(TError("Chunk is removed"));
        }

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
        auto metaOrError = WaitFor(Chunk_->GetMeta(0));
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
            ChunkId_);

        LOG_INFO("Chunk meta fetched");
        const auto& meta = metaOrError.Value();

        auto targets = FromProto<TNodeDescriptor>(ReplicateChunkJobSpecExt_.targets());

        auto writer = CreateReplicationWriter(
            Config_->ReplicationWriter,
            ChunkId_,
            targets,
            EWriteSessionType::Replication,
            Bootstrap_->GetReplicationOutThrottler());
        writer->Open();

        int blockIndex = 0;
        int blockCount = GetBlockCount(*meta);

        auto blockStore = Bootstrap_->GetBlockStore();

        while (blockIndex < blockCount) {
            auto getResult = WaitFor(blockStore->GetBlocks(
                ChunkId_,
                blockIndex,
                blockCount - blockIndex,
                FetchPriority));
            THROW_ERROR_EXCEPTION_IF_FAILED(getResult, "Error reading chunk %v during replication",
                ChunkId_);
            const auto& blocks = getResult.Value();

            LOG_DEBUG("Enqueuing blocks for replication (Blocks: %v-%v)",
                blockIndex,
                blockIndex + blocks.size() - 1);

            auto writeResult = writer->WriteBlocks(blocks);
            if (!writeResult) {
                auto error = WaitFor(writer->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error writing chunk %v during replication",
                    ChunkId_);
            }

            blockIndex += blocks.size();
        }

        LOG_DEBUG("All blocks are enqueued for replication");

        {
            auto error = WaitFor(writer->Close(*meta));
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing replication writer");
        }
    }

    int GetBlockCount(const TChunkMeta& meta)
    {
        switch (TypeFromId(DecodeChunkId(ChunkId_).Id)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                auto blocksExt = GetProtoExtension<TBlocksExt>(meta.extensions());
                return blocksExt.blocks_size();
            }

            case EObjectType::JournalChunk: {
                auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
                if (!miscExt.sealed()) {
                    THROW_ERROR_EXCEPTION("Cannot replicate an unsealed chunk %v",
                        ChunkId_);
                }
                return miscExt.row_count();
            }

            default:
                YUNREACHABLE();
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

        auto replicas = FromProto<TChunkReplica>(RepairJobSpecExt_.replicas());
        auto targets = FromProto<TNodeDescriptor>(RepairJobSpecExt_.targets());
        auto erasedIndexes = FromProto<int, NErasure::TPartIndexList>(RepairJobSpecExt_.erased_indexes());
        YCHECK(targets.size() == erasedIndexes.size());

        // Compute repair plan.
        auto repairIndexes = codec->GetRepairIndices(erasedIndexes);
        if (!repairIndexes) {
            THROW_ERROR_EXCEPTION("Codec is unable to repair the chunk");
        }

        LOG_INFO("Preparing to repair (ErasedIndexes: [%v], RepairIndexes: [%v], Targets: [%v])",
            JoinToString(erasedIndexes),
            JoinToString(*repairIndexes),
            JoinToString(targets));

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(RepairJobSpecExt_.node_directory());

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
                Config_->RepairReader,
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
                Config_->RepairWriter,
                partId,
                std::vector<TNodeDescriptor>(1, target),
                EWriteSessionType::Repair,
                Bootstrap_->GetRepairOutThrottler());
            writers.push_back(writer);
        }

        {
            auto onProgress = BIND(&TChunkRepairJob::SetProgress, MakeWeak(this))
                .Via(GetCurrentInvoker());

            auto asyncError = RepairErasedParts(
                codec,
                erasedIndexes,
                readers,
                writers,
                onProgress);

            // Make sure the repair is canceled when the current fiber terminates.
            TFutureCancelationGuard<TError> repairErrorGuard(asyncError);

            auto repairError = WaitFor(asyncError);
            THROW_ERROR_EXCEPTION_IF_FAILED(repairError, "Error repairing chunk %v",
                ChunkId_);
        }
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
        if (Chunk_->IsActive()) {
            THROW_ERROR_EXCEPTION("Cannot seal an active journal chunk %v",
                ChunkId_);
        }

        auto journalDispatcher = Bootstrap_->GetJournalDispatcher();
        auto location = Chunk_->GetLocation();
        auto changelog = journalDispatcher->OpenChangelog(location, ChunkId_, false);

        if (changelog->IsSealed()) {
            LOG_INFO("Chunk %v is already sealed",
                ChunkId_);
            return;
        }

        auto journalChunk = Chunk_->AsJournalChunk();
        if (journalChunk->HasAttachedChangelog()) {
            THROW_ERROR_EXCEPTION("Journal chunk %v is already being written to",
                ChunkId_);
        }
        journalChunk->AttachChangelog(changelog);

        try {
            i64 currentRowCount = changelog->GetRecordCount();
            i64 sealRowCount = SealJobSpecExt_.row_count();
            if (currentRowCount < sealRowCount) {
                LOG_INFO("Started downloading missing journal chunk rows (Rows: %v-%v)",
                    currentRowCount,
                    sealRowCount - 1);

                auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
                nodeDirectory->MergeFrom(SealJobSpecExt_.node_directory());

                auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(SealJobSpecExt_.replicas());

                auto reader = CreateReplicationReader(
                    Config_->SealReader,
                    Bootstrap_->GetBlockStore()->GetBlockCache(),
                    Bootstrap_->GetMasterClient()->GetMasterChannel(),
                    nodeDirectory,
                    Null,
                    ChunkId_,
                    replicas,
                    DefaultNetworkName,
                    EReadSessionType::Replication,
                    Bootstrap_->GetReplicationInThrottler());

                while (currentRowCount < sealRowCount) {
                    auto blocksOrError = WaitFor(reader->ReadBlocks(
                        currentRowCount,
                        sealRowCount - currentRowCount));
                    THROW_ERROR_EXCEPTION_IF_FAILED(blocksOrError);

                    const auto& blocks = blocksOrError.Value();
                    int blockCount = static_cast<int>(blocks.size());

                    if (blockCount == 0) {
                        THROW_ERROR_EXCEPTION("Cannot download missing rows %v-%v to seal chunk %v",
                            currentRowCount,
                            sealRowCount - 1,
                            ChunkId_);
                    }

                    LOG_INFO("Journal chunk rows downloaded (Blocks: %v-%v)",
                        currentRowCount,
                        currentRowCount + blockCount - 1);

                    for (const auto& block : blocks) {
                        changelog->Append(block);
                    }

                    currentRowCount += blockCount;
                }

                LOG_INFO("Finished downloading missing journal chunk rows");
            }

            LOG_INFO("Started sealing journal chunk (RowCount: %v)",
                sealRowCount);
            {
                auto error = WaitFor(changelog->Seal(sealRowCount));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
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


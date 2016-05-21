#include "job.h"
#include "private.h"
#include "chunk_block_manager.h"
#include "chunk.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "master_connector.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/hydra/changelog.h>

#include <yt/server/job_agent/job.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/erasure_reader.h>
#include <yt/ytlib/chunk_client/job.pb.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/replication_writer.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/string.h>

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
using namespace NYson;

using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TChunkJobBase
    : public IJob
{
public:
    DEFINE_SIGNAL(void(const TNodeResources& resourcesDelta), ResourcesUpdated);

public:
    TChunkJobBase(
        const TJobId& jobId,
        const TJobSpec& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : JobId_(jobId)
        , JobSpec_(jobSpec)
        , Config_(config)
        , Bootstrap_(bootstrap)
        , ChunkJobSpecExt_(JobSpec_.GetExtension(TChunkJobSpecExt::chunk_job_spec_ext))
        , ChunkId_(FromProto<TChunkId>(ChunkJobSpecExt_.chunk_id()))
        , ResourceLimits_(resourceLimits)
    {
        Logger.AddTag("JobId: %v, JobType: %v, ChunkId: %v",
            JobId_,
            GetType(),
            ChunkId_);
    }

    virtual void Start() override
    {
        JobState_ = EJobState::Running;
        JobPhase_ = EJobPhase::Running;
        JobFuture_ = BIND(&TChunkJobBase::GuardedRun, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run();
    }

    virtual void Abort(const TError& error) override
    {
        switch (JobState_) {
            case EJobState::Waiting:
                SetAborted(error);
                return;

            case EJobState::Running:
                JobFuture_.Cancel();
                SetAborted(error);
                return;

            default:
                return;
        }
    }

    virtual const TJobId& GetId() const override
    {
        return JobId_;
    }

    virtual const TOperationId& GetOperationId() const override
    {
        return NullOperationId;
    }

    virtual EJobType GetType() const override
    {
        return EJobType(JobSpec_.type());
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

    virtual void SetStatistics(const NJobTrackerClient::NProto::TStatistics& /*statistics*/) override
    {
        YUNREACHABLE();
    }

    virtual std::vector<TChunkId> DumpInputContexts() const override
    {
        THROW_ERROR_EXCEPTION("Input context dumping is not supported");
    }

    virtual TYsonString Strace() const override
    {
        THROW_ERROR_EXCEPTION("Stracing is not supported");
    }

    virtual void SignalJob(const Stroka& /*signalName*/) override
    {
        THROW_ERROR_EXCEPTION("Signaling is not supported");
    }

    virtual TYsonString PollJobShell(const TYsonString& /*parameters*/) override
    {
        THROW_ERROR_EXCEPTION("Job shell is not supported");
    }

protected:
    const TJobId JobId_;
    const TJobSpec JobSpec_;
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    const TChunkJobSpecExt ChunkJobSpecExt_;
    const TChunkId ChunkId_;

    TNodeResources ResourceLimits_;

    NLogging::TLogger Logger = DataNodeLogger;

    EJobState JobState_ = EJobState::Waiting;
    EJobPhase JobPhase_ = EJobPhase::Created;

    double Progress_ = 0.0;

    TFuture<void> JobFuture_;

    TJobResult Result_;


    virtual void DoRun() = 0;


    void GuardedRun()
    {
        LOG_INFO("Job started");
        try {
            DoRun();
        } catch (const std::exception& ex) {
            SetFailed(ex);
            return;
        }
        SetCompleted();
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


    IChunkPtr GetLocalChunkOrThrow()
    {
        auto chunkStore = Bootstrap_->GetChunkStore();
        return chunkStore->GetChunkOrThrow(ChunkId_);
    }

private:
    void DoSetFinished(EJobState finalState, const TError& error)
    {
        if (JobState_ != EJobState::Running && JobState_ != EJobState::Waiting)
            return;

        JobPhase_ = EJobPhase::Finished;
        JobState_ = finalState;
        ToProto(Result_.mutable_error(), error);
        Result_.mutable_statistics();
        auto deltaResources = ZeroNodeResources() - ResourceLimits_;
        ResourceLimits_ = ZeroNodeResources();
        JobFuture_.Reset();
        ResourcesUpdated_.Fire(deltaResources);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkRemovalJob
    : public TChunkJobBase
{
public:
    TChunkRemovalJob(
        const TJobId& jobId,
        const TJobSpec& jobSpec,
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

private:
    virtual void DoRun() override
    {
        auto chunk = GetLocalChunkOrThrow();
        auto chunkStore = Bootstrap_->GetChunkStore();
        WaitFor(chunkStore->RemoveChunk(chunk))
            .ThrowOnError();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicationJob
    : public TChunkJobBase
{
public:
    TChunkReplicationJob(
        const TJobId& jobId,
        const TJobSpec& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TChunkJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
        , ReplicateChunkJobSpecExt_(JobSpec_.GetExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext))
    { }

private:
    const TReplicateChunkJobSpecExt ReplicateChunkJobSpecExt_;


    virtual void DoRun() override
    {
        auto chunk = GetLocalChunkOrThrow();

        LOG_INFO("Fetching chunk meta");

        auto asyncMeta = chunk->ReadMeta(Config_->ReplicationWriter->WorkloadDescriptor);
        auto meta = WaitFor(asyncMeta)
            .ValueOrThrow();

        LOG_INFO("Chunk meta fetched");

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(ReplicateChunkJobSpecExt_.node_directory());

        auto targets = FromProto<TChunkReplicaList>(ReplicateChunkJobSpecExt_.targets());

        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;

        auto writer = CreateReplicationWriter(
            Config_->ReplicationWriter,
            options,
            ChunkId_,
            targets,
            nodeDirectory,
            Bootstrap_->GetMasterClient(),
            GetNullBlockCache(),
            Bootstrap_->GetReplicationOutThrottler());

        WaitFor(writer->Open())
            .ThrowOnError();

        int currentBlockIndex = 0;
        int blockCount = GetBlockCount(*meta);
        while (currentBlockIndex < blockCount) {
            TBlockReadOptions options;
            options.WorkloadDescriptor = Config_->ReplicationWriter->WorkloadDescriptor;
            options.BlockCache = Bootstrap_->GetBlockCache();

            auto chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            auto asyncReadBlocks = chunkBlockManager->ReadBlockRange(
                ChunkId_,
                currentBlockIndex,
                blockCount - currentBlockIndex,
                options);

            auto readBlocks = WaitFor(asyncReadBlocks)
                .ValueOrThrow();

            std::vector<TSharedRef> writeBlocks;
            for (const auto& block : readBlocks) {
                if (!block)
                    break;
                writeBlocks.push_back(block);
            }

            LOG_DEBUG("Enqueuing blocks for replication (Blocks: %v-%v)",
                currentBlockIndex,
                currentBlockIndex + writeBlocks.size() - 1);

            auto writeResult = writer->WriteBlocks(writeBlocks);
            if (!writeResult) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }

            currentBlockIndex += writeBlocks.size();
        }

        LOG_DEBUG("All blocks are enqueued for replication");

        WaitFor(writer->Close(*meta))
            .ThrowOnError();
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
        const TJobSpec& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TChunkJobBase(
            jobId,
            std::move(jobSpec),
            resourceLimits,
            config,
            bootstrap)
        , RepairChunkJobSpecExt_(JobSpec_.GetExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext))
    { }

private:
    const TRepairChunkJobSpecExt RepairChunkJobSpecExt_;


    virtual void DoRun() override
    {
        auto codecId = NErasure::ECodec(RepairChunkJobSpecExt_.erasure_codec());
        auto* codec = NErasure::GetCodec(codecId);

        auto replicas = FromProto<TChunkReplicaList>(RepairChunkJobSpecExt_.replicas());
        auto targets = FromProto<TChunkReplicaList>(RepairChunkJobSpecExt_.targets());
        auto erasedIndexes = FromProto<NErasure::TPartIndexList>(RepairChunkJobSpecExt_.erased_indexes());
        YCHECK(targets.size() == erasedIndexes.size());

        // Compute repair plan.
        auto repairIndexes = codec->GetRepairIndices(erasedIndexes);
        if (!repairIndexes) {
            THROW_ERROR_EXCEPTION("Codec is unable to repair the chunk");
        }

        LOG_INFO("Preparing to repair (ErasedIndexes: %v, RepairIndexes: %v, Targets: %v)",
            erasedIndexes,
            *repairIndexes,
            targets);

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(RepairChunkJobSpecExt_.node_directory());

        std::vector<IChunkReaderPtr> readers;
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
                New<TRemoteReaderOptions>(),
                Bootstrap_->GetMasterClient(),
                nodeDirectory,
                Bootstrap_->GetMasterConnector()->GetLocalDescriptor(),
                partId,
                partReplicas,
                Bootstrap_->GetBlockCache(),
                Bootstrap_->GetRepairInThrottler());
            readers.push_back(reader);
        }

        std::vector<IChunkWriterPtr> writers;
        for (int index = 0; index < static_cast<int>(erasedIndexes.size()); ++index) {
            int partIndex = erasedIndexes[index];
            auto partId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
            auto options = New<TRemoteWriterOptions>();
            options->AllowAllocatingNewTargetNodes = false;
            auto writer = CreateReplicationWriter(
                Config_->RepairWriter,
                options,
                partId,
                TChunkReplicaList(1, targets[index]),
                nodeDirectory,
                Bootstrap_->GetMasterClient(),
                GetNullBlockCache(),
                Bootstrap_->GetRepairOutThrottler());
            writers.push_back(writer);
        }

        {
            auto onProgress = BIND(&TChunkRepairJob::SetProgress, MakeWeak(this))
                .Via(GetCurrentInvoker());

            auto result = RepairErasedParts(
                codec,
                erasedIndexes,
                readers,
                writers,
                Config_->RepairReader->WorkloadDescriptor,
                onProgress);

            auto repairError = WaitFor(result);
            THROW_ERROR_EXCEPTION_IF_FAILED(repairError, "Error repairing chunk %v",
                ChunkId_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSealChunkJob
    : public TChunkJobBase
{
public:
    TSealChunkJob(
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
        , SealJobSpecExt_(JobSpec_.GetExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext))
    { }

private:
    const TSealChunkJobSpecExt SealJobSpecExt_;


    virtual void DoRun() override
    {
        auto chunk = GetLocalChunkOrThrow();

        if (chunk->GetType() != EObjectType::JournalChunk) {
            THROW_ERROR_EXCEPTION("Cannot seal a non-journal chunk %v",
                ChunkId_);
        }

        auto journalChunk = chunk->AsJournalChunk();
        if (journalChunk->IsActive()) {
            THROW_ERROR_EXCEPTION("Cannot seal an active journal chunk %v",
                ChunkId_);
        }

        auto readGuard = TChunkReadGuard::TryAcquire(chunk);
        if (!readGuard) {
            THROW_ERROR_EXCEPTION("Cannot lock chunk %v",
                ChunkId_);
        }

        auto journalDispatcher = Bootstrap_->GetJournalDispatcher();
        auto location = journalChunk->GetStoreLocation();
        auto changelog = WaitFor(journalDispatcher->OpenChangelog(location, ChunkId_))
            .ValueOrThrow();

        if (journalChunk->HasAttachedChangelog()) {
            THROW_ERROR_EXCEPTION("Journal chunk %v is already being written",
                ChunkId_);
        }

        if (journalChunk->IsSealed()) {
            LOG_INFO("Chunk %v is already sealed",
                ChunkId_);
            return;
        }

        TJournalChunkChangelogGuard changelogGuard(journalChunk, changelog);
        i64 currentRowCount = changelog->GetRecordCount();
        i64 sealRowCount = SealJobSpecExt_.row_count();
        if (currentRowCount < sealRowCount) {
            LOG_INFO("Started downloading missing journal chunk rows (Rows: %v-%v)",
                currentRowCount,
                sealRowCount - 1);

            auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
            nodeDirectory->MergeFrom(SealJobSpecExt_.node_directory());

            auto replicas = FromProto<TChunkReplicaList>(SealJobSpecExt_.replicas());

            auto reader = CreateReplicationReader(
                Config_->SealReader,
                New<TRemoteReaderOptions>(),
                Bootstrap_->GetMasterClient(),
                nodeDirectory,
                Bootstrap_->GetMasterConnector()->GetLocalDescriptor(),
                ChunkId_,
                replicas,
                Bootstrap_->GetBlockCache(),
                Bootstrap_->GetReplicationInThrottler());

            while (currentRowCount < sealRowCount) {
                auto asyncBlocks  = reader->ReadBlocks(
                    Config_->RepairReader->WorkloadDescriptor,
                    currentRowCount,
                    sealRowCount - currentRowCount);
                auto blocks = WaitFor(asyncBlocks)
                    .ValueOrThrow();

                int blockCount = blocks.size();
                if (blockCount == 0) {
                    THROW_ERROR_EXCEPTION("Cannot download missing rows %v-%v to seal chunk %v",
                        currentRowCount,
                        sealRowCount - 1,
                        ChunkId_);
                }

                LOG_INFO("Journal chunk rows downloaded (Rows: %v-%v)",
                    currentRowCount,
                    currentRowCount + blockCount - 1);

                for (const auto& block : blocks) {
                    changelog->Append(block);
                }

                currentRowCount += blockCount;
            }

            WaitFor(changelog->Flush())
                .ThrowOnError();
        
            LOG_INFO("Finished downloading missing journal chunk rows");
        }

        LOG_INFO("Started sealing journal chunk (RowCount: %v)",
            sealRowCount);

        WaitFor(journalChunk->Seal())
            .ThrowOnError();

        LOG_INFO("Finished sealing journal chunk");

        auto chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->UpdateExistingChunk(chunk);
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


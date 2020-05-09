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

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/server/node/job_agent/job.h>

#include <yt/server/lib/hydra/changelog.h>

#include <yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/erasure_repair.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/replication_writer.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/ytlib/journal_client/erasure_repair.h>
#include <yt/ytlib/journal_client/chunk_reader.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/string.h>

namespace NYT::NDataNode {

using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NClusterNode;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NYson;
using namespace NCoreDump;

using NNodeTrackerClient::TNodeDescriptor;
using NChunkClient::TChunkReaderStatistics;
using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TChunkJobBase
    : public IJob
{
public:
    DEFINE_SIGNAL(void(const TNodeResources& resourcesDelta), ResourcesUpdated);
    DEFINE_SIGNAL(void(), PortsReleased);
    DEFINE_SIGNAL(void(), JobFinished);

public:
    TChunkJobBase(
        TJobId jobId,
        const TJobSpec& jobSpec,
        const TNodeResources& resourceLimits,
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : JobId_(jobId)
        , JobSpec_(jobSpec)
        , Config_(config)
        , StartTime_(TInstant::Now())
        , Bootstrap_(bootstrap)
        , Logger(NLogging::TLogger(DataNodeLogger)
            .AddTag("JobId: %v, JobType: %v",
                JobId_,
                GetType()))
        , ResourceLimits_(resourceLimits)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        JobState_ = EJobState::Running;
        JobPhase_ = EJobPhase::Running;
        JobFuture_ = BIND(&TChunkJobBase::GuardedRun, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run();
    }

    virtual void Abort(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        switch (JobState_) {
            case EJobState::Waiting:
                SetAborted(error);
                return;

            case EJobState::Running:
                JobFuture_.Cancel(error);
                SetAborted(error);
                return;

            default:
                return;
        }
    }

    virtual void Fail() override
    {
        THROW_ERROR_EXCEPTION("Failing is not supported");
    }

    virtual TJobId GetId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobId_;
    }

    virtual TOperationId GetOperationId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return {};
    }

    virtual EJobType GetType() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CheckedEnumCast<EJobType>(JobSpec_.type());
    }

    virtual const TJobSpec& GetSpec() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobSpec_;
    }

    virtual int GetPortCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return 0;
    }

    virtual EJobState GetState() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return JobState_;
    }

    virtual EJobPhase GetPhase() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return JobPhase_;
    }

    virtual int GetSlotIndex() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return -1;
    }

    virtual TNodeResources GetResourceUsage() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return ResourceLimits_;
    }

    std::vector<int> GetPorts() const override
    {
        YT_ABORT();
    }

    void SetPorts(const std::vector<int>&) override
    {
        YT_ABORT();
    }

    virtual void SetResourceUsage(const TNodeResources& /*newUsage*/) override
    {
        YT_ABORT();
    }

    virtual TJobResult GetResult() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return Result_;
    }

    virtual void SetResult(const TJobResult& /*result*/) override
    {
        YT_ABORT();
    }

    virtual double GetProgress() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return Progress_;
    }

    virtual void SetProgress(double value) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        Progress_ = value;
    }

    virtual i64 GetStderrSize() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return JobStderrSize_;
    }

    virtual void SetStderrSize(i64 value) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        JobStderrSize_ = value;
    }

    virtual void SetStderr(const TString& value) override
    {
        YT_ABORT();
    }

    virtual void SetFailContext(const TString& value) override
    {
        YT_ABORT();
    }

    virtual void SetProfile(const TJobProfile& value) override
    {
        YT_ABORT();
    }

    virtual void SetCoreInfos(TCoreInfos value) override
    {
        YT_ABORT();
    }

    virtual TYsonString GetStatistics() const override
    {
        return TYsonString();
    }

    virtual void SetStatistics(const TYsonString& /*statistics*/) override
    {
        YT_ABORT();
    }

    virtual TInstant GetStartTime() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return StartTime_;
    }

    virtual std::optional<TDuration> GetPrepareDuration() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return std::nullopt;
    }

    virtual std::optional<TDuration> GetPrepareRootFSDuration() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return std::nullopt;
    }

    virtual std::optional<TDuration> GetDownloadDuration() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return std::nullopt;
    }

    virtual std::optional<TDuration> GetExecDuration() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return std::nullopt;
    }

    virtual TInstant GetStatisticsLastSendTime() const override
    {
        YT_ABORT();
    }

    virtual void ResetStatisticsLastSendTime() override
    {
        YT_ABORT();
    }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        THROW_ERROR_EXCEPTION("Input context dumping is not supported");
    }

    virtual TString GetStderr() override
    {
        THROW_ERROR_EXCEPTION("Getting stderr is not supported");
    }

    virtual std::optional<TString> GetFailContext() override
    {
        THROW_ERROR_EXCEPTION("Getting fail context is not supported");
    }

    virtual TYsonString StraceJob() override
    {
        THROW_ERROR_EXCEPTION("Stracing is not supported");
    }

    virtual void SignalJob(const TString& /*signalName*/) override
    {
        THROW_ERROR_EXCEPTION("Signaling is not supported");
    }

    virtual TYsonString PollJobShell(const TYsonString& /*parameters*/) override
    {
        THROW_ERROR_EXCEPTION("Job shell is not supported");
    }

    virtual void Interrupt() override
    {
        THROW_ERROR_EXCEPTION("Interrupting is not supported");
    }

    virtual void OnJobPrepared() override
    {
        YT_ABORT();
    }

    virtual void ReportStatistics(TNodeJobReport&&) override
    {
        YT_ABORT();
    }

    virtual void ReportSpec() override
    {
        YT_ABORT();
    }

    virtual void ReportStderr() override
    {
        YT_ABORT();
    }

    virtual void ReportFailContext() override
    {
        YT_ABORT();
    }

    virtual void ReportProfile() override
    {
        YT_ABORT();
    }

    virtual bool GetStored() const override
    {
        return false;
    }

    virtual void SetStored(bool /* value */) override
    {
        YT_ABORT();
    }

protected:
    const TJobId JobId_;
    const TJobSpec JobSpec_;
    const TDataNodeConfigPtr Config_;
    const TInstant StartTime_;
    TBootstrap* const Bootstrap_;

    const NLogging::TLogger Logger;

    TNodeResources ResourceLimits_;

    EJobState JobState_ = EJobState::Waiting;
    EJobPhase JobPhase_ = EJobPhase::Created;

    double Progress_ = 0.0;
    ui64 JobStderrSize_ = 0;

    TString Stderr_;

    TFuture<void> JobFuture_;

    TJobResult Result_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    virtual void DoRun() = 0;

    void GuardedRun()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

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
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Job completed");
        Progress_ = 1.0;
        DoSetFinished(EJobState::Completed, TError());
    }

    void SetFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_ERROR(error, "Job failed");
        DoSetFinished(EJobState::Failed, error);
    }

    void SetAborted(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO(error, "Job aborted");
        DoSetFinished(EJobState::Aborted, error);
    }

    IChunkPtr GetLocalChunkOrThrow(TChunkId chunkId, int mediumIndex)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        return chunkStore->GetChunkOrThrow(chunkId, mediumIndex);
    }

private:
    void DoSetFinished(EJobState finalState, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (JobState_ != EJobState::Running && JobState_ != EJobState::Waiting) {
            return;
        }

        JobPhase_ = EJobPhase::Finished;
        JobState_ = finalState;
        JobFinished_.Fire();
        ToProto(Result_.mutable_error(), error);
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
        TJobId jobId,
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
        , JobSpecExt_(JobSpec_.GetExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext))
    { }

private:
    const TRemoveChunkJobSpecExt JobSpecExt_;

    virtual void DoRun() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto chunkId = FromProto<TChunkId>(JobSpecExt_.chunk_id());
        int mediumIndex = JobSpecExt_.medium_index();

        YT_LOG_INFO("Chunk removal job started (ChunkId: %v@v)",
            chunkId,
            mediumIndex);

        auto chunk = GetLocalChunkOrThrow(chunkId, mediumIndex);
        const auto& chunkStore = Bootstrap_->GetChunkStore();
        WaitFor(chunkStore->RemoveChunk(chunk))
            .ThrowOnError();

        // Wait for the removal notification to be delivered to master.
        // Cf. YT-6532.
        // Once we switch from push replication to pull, this code is likely
        // to appear in TReplicateChunkJob as well.
        YT_LOG_INFO("Waiting for heartbeat barrier");
        const auto& masterConnector = Bootstrap_->GetMasterConnector();
        WaitFor(masterConnector->GetHeartbeatBarrier(CellTagFromId(chunkId)))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicationJob
    : public TChunkJobBase
{
public:
    TChunkReplicationJob(
        TJobId jobId,
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
        , JobSpecExt_(JobSpec_.GetExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext))
    { }

private:
    const TReplicateChunkJobSpecExt JobSpecExt_;

    virtual void DoRun() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto chunkId = FromProto<TChunkId>(JobSpecExt_.chunk_id());
        int sourceMediumIndex = JobSpecExt_.source_medium_index();
        auto targetReplicas = FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.target_replicas());

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(JobSpecExt_.node_directory());

        // Compute target medium index.
        if (targetReplicas.empty()) {
            THROW_ERROR_EXCEPTION("No target replicas");
        }
        int targetMediumIndex = targetReplicas[0].GetMediumIndex();
        auto sessionId = TSessionId(chunkId, targetMediumIndex);

        YT_LOG_INFO("Chunk replication job started (ChunkId: %v@%v, TargetReplicas: %v)",
            chunkId,
            sourceMediumIndex,
            MakeFormattableView(targetReplicas, TChunkReplicaAddressFormatter(nodeDirectory)));

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemReplication;
        workloadDescriptor.Annotations.push_back(Format("Replication of chunk %v",
            chunkId));

        auto chunk = GetLocalChunkOrThrow(chunkId, sourceMediumIndex);

        TBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = workloadDescriptor;
        blockReadOptions.BlockCache = Bootstrap_->GetBlockCache();
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        TRefCountedChunkMetaPtr meta;
        {
            YT_LOG_DEBUG("Fetching chunk meta");

            auto asyncMeta = chunk->ReadMeta(blockReadOptions);
            meta = WaitFor(asyncMeta)
                .ValueOrThrow();

            YT_LOG_DEBUG("Chunk meta fetched");
        }

        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;

        auto writer = CreateReplicationWriter(
            Config_->ReplicationWriter,
            options,
            sessionId,
            targetReplicas,
            nodeDirectory,
            Bootstrap_->GetMasterClient(),
            GetNullBlockCache(),
            /* trafficMeter */ nullptr,
            Bootstrap_->GetReplicationOutThrottler());

        {
            YT_LOG_DEBUG("Started opening writer");

            WaitFor(writer->Open())
                .ThrowOnError();

            YT_LOG_DEBUG("Writer opened");
        }

        int currentBlockIndex = 0;
        int blockCount = GetBlockCount(chunkId, *meta);
        while (currentBlockIndex < blockCount) {

            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            auto asyncReadBlocks = chunkBlockManager->ReadBlockRange(
                chunkId,
                currentBlockIndex,
                blockCount - currentBlockIndex,
                blockReadOptions);

            auto readBlocks = WaitFor(asyncReadBlocks)
                .ValueOrThrow();

            std::vector<TBlock> writeBlocks;
            for (const auto& block : readBlocks) {
                if (!block)
                    break;
                writeBlocks.push_back(block);
            }

            YT_LOG_DEBUG("Enqueuing blocks for replication (Blocks: %v-%v)",
                currentBlockIndex,
                currentBlockIndex + static_cast<int>(writeBlocks.size()) - 1);

            auto writeResult = writer->WriteBlocks(writeBlocks);
            if (!writeResult) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }

            currentBlockIndex += writeBlocks.size();
        }

        YT_LOG_DEBUG("All blocks are enqueued for replication");

        {
            YT_LOG_DEBUG("Started closing writer");

            WaitFor(writer->Close(meta))
                .ThrowOnError();

            YT_LOG_DEBUG("Writer closed");
        }
    }

    static int GetBlockCount(TChunkId chunkId, const TChunkMeta& meta)
    {
        switch (TypeFromId(DecodeChunkId(chunkId).Id)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                auto blocksExt = GetProtoExtension<TBlocksExt>(meta.extensions());
                return blocksExt.blocks_size();
            }

            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk: {
                auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());
                if (!miscExt.sealed()) {
                    THROW_ERROR_EXCEPTION("Cannot replicate an unsealed chunk %v",
                        chunkId);
                }
                return miscExt.row_count();
            }

            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkRepairJob
    : public TChunkJobBase
{
public:
    TChunkRepairJob(
        TJobId jobId,
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
        , JobSpecExt_(JobSpec_.GetExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext))
        , ChunkId_(FromProto<TChunkId>(JobSpecExt_.chunk_id()))
        , SourceReplicas_(FromProto<TChunkReplicaList>(JobSpecExt_.source_replicas()))
        , TargetReplicas_(FromProto<TChunkReplicaWithMediumList>(JobSpecExt_.target_replicas()))
    { }

private:
    const TRepairChunkJobSpecExt JobSpecExt_;
    const TChunkId ChunkId_;
    const TChunkReplicaList SourceReplicas_;
    const TChunkReplicaWithMediumList TargetReplicas_;

    const NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();

    IChunkReaderPtr CreateReader(int partIndex)
    {
        TChunkReplicaList partReplicas;
        for (auto replica : SourceReplicas_) {
            if (replica.GetReplicaIndex() == partIndex) {
                partReplicas.push_back(replica);
            }
        }

        if (partReplicas.empty()) {
            THROW_ERROR_EXCEPTION("No source replicas for part %v",
                partIndex);
        }

        auto options = New<TRemoteReaderOptions>();
        options->AllowFetchingSeedsFromMaster = false;

        auto partChunkId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
        auto reader = CreateReplicationReader(
            Config_->RepairReader,
            options,
            Bootstrap_->GetMasterClient(),
            NodeDirectory_,
            Bootstrap_->GetMasterConnector()->GetLocalDescriptor(),
            Bootstrap_->GetMasterConnector()->GetNodeId(),
            partChunkId,
            partReplicas,
            Bootstrap_->GetBlockCache(),
            /* trafficMeter */ nullptr,
            Bootstrap_->GetRepairInThrottler());

        return reader;
    }

    IChunkWriterPtr CreateWriter(int partIndex)
    {
        auto targetReplica = [&] {
            for (auto replica : TargetReplicas_) {
                if (replica.GetReplicaIndex() == partIndex) {
                    return replica;
                }
            }
            YT_ABORT();
        }();
        auto partChunkId = ErasurePartIdFromChunkId(ChunkId_, partIndex);
        auto partSessionId = TSessionId(partChunkId, targetReplica.GetMediumIndex());
        auto options = New<TRemoteWriterOptions>();
        options->AllowAllocatingNewTargetNodes = false;
        auto writer = CreateReplicationWriter(
            Config_->RepairWriter,
            options,
            partSessionId,
            TChunkReplicaWithMediumList(1, targetReplica),
            NodeDirectory_,
            Bootstrap_->GetMasterClient(),
            GetNullBlockCache(),
            /* trafficMeter */ nullptr,
            Bootstrap_->GetRepairOutThrottler());
        return writer;
    }

    virtual void DoRun() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto codecId = CheckedEnumCast<NErasure::ECodec>(JobSpecExt_.erasure_codec());
        auto* codec = NErasure::GetCodec(codecId);
        auto decommission = JobSpecExt_.decommission();
        auto rowCount = JobSpecExt_.has_row_count() ? std::make_optional<i64>(JobSpecExt_.row_count()) : std::nullopt;

        NodeDirectory_->MergeFrom(JobSpecExt_.node_directory());

        YT_LOG_INFO("Chunk repair job started (ChunkId: %v, Codec: %v, "
            "SourceReplicas: %v, TargetReplicas: %v, Decommission: %v, RowCount: %v)",
            ChunkId_,
            codecId,
            MakeFormattableView(SourceReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            MakeFormattableView(TargetReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            decommission,
            rowCount);

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = decommission ? EWorkloadCategory::SystemReplication : EWorkloadCategory::SystemRepair;
        workloadDescriptor.Annotations.push_back(Format("%v of chunk %v",
            decommission ? "Decommission via repair" : "Repair",
            ChunkId_));

        // TODO(savrus) profile chunk reader statistics.
        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = workloadDescriptor;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        NErasure::TPartIndexList sourcePartIndexes;
        for (auto replica : SourceReplicas_) {
            sourcePartIndexes.push_back(replica.GetReplicaIndex());
        }
        SortUnique(sourcePartIndexes);

        NErasure::TPartIndexList erasedPartIndexes;
        for (auto replica : TargetReplicas_) {
            erasedPartIndexes.push_back(replica.GetReplicaIndex());
        }
        SortUnique(erasedPartIndexes);

        std::vector<IChunkWriterPtr> writers;
        for (int partIndex : erasedPartIndexes) {
            writers.push_back(CreateWriter(partIndex));
        }

        {
            TFuture<void> future;
            auto chunkType = TypeFromId(ChunkId_);
            switch (chunkType) {
                case EObjectType::ErasureChunk: {
                    auto repairPartIndexes = codec->GetRepairIndices(erasedPartIndexes);
                    if (!repairPartIndexes) {
                        THROW_ERROR_EXCEPTION("Codec is unable to repair the chunk");
                    }
                    
                    std::vector<IChunkReaderPtr> readers;
                    for (int partIndex : *repairPartIndexes) {
                        readers.push_back(CreateReader(partIndex));
                    }

                    future = NChunkClient::RepairErasedParts(
                        codec, // XXX(babenko): codec id?
                        erasedPartIndexes,
                        readers,
                        writers,
                        blockReadOptions);
                        // XXX(babenko): pass logger
                    break;
                }
                
                case EObjectType::ErasureJournalChunk: {
                    std::vector<IChunkReaderPtr> readers;
                    for (int partIndex : sourcePartIndexes) {
                        readers.push_back(CreateReader(partIndex));
                    }

                    future = NJournalClient::RepairErasedParts(
                        Config_->RepairReader,
                        codecId,
                        *rowCount,
                        erasedPartIndexes,
                        readers,
                        writers,
                        blockReadOptions,
                        Logger);
                    break;
                }
                
                default:
                    THROW_ERROR_EXCEPTION("Unsupported chunk type %Qlv",
                        chunkType);
            }

            WaitFor(future)
                .ThrowOnError();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSealChunkJob
    : public TChunkJobBase
{
public:
    TSealChunkJob(
        TJobId jobId,
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
        , JobSpecExt_(JobSpec_.GetExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext))
    { }

private:
    const TSealChunkJobSpecExt JobSpecExt_;

    virtual void DoRun() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto chunkId = FromProto<TChunkId>(JobSpecExt_.chunk_id());
        auto codecId = CheckedEnumCast<NErasure::ECodec>(JobSpecExt_.codec_id());
        int mediumIndex = JobSpecExt_.medium_index();
        auto sourceReplicas = FromProto<TChunkReplicaList>(JobSpecExt_.source_replicas());
        i64 sealRowCount = JobSpecExt_.row_count();

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->MergeFrom(JobSpecExt_.node_directory());

        YT_LOG_INFO("Chunk seal job started (ChunkId: %v@%v, Codec: %v, SourceReplicas: %v, RowCount: %v)",
            chunkId,
            mediumIndex,
            codecId,
            MakeFormattableView(sourceReplicas, TChunkReplicaAddressFormatter(nodeDirectory)),
            sealRowCount);

        auto chunk = GetLocalChunkOrThrow(chunkId, mediumIndex);
        if (!chunk->IsJournalChunk()) {
            THROW_ERROR_EXCEPTION("Cannot seal a non-journal chunk %v",
                chunkId);
        }

        auto journalChunk = chunk->AsJournalChunk();
        if (journalChunk->IsSealed()) {
            YT_LOG_INFO("Chunk is already sealed");
            return;
        }

        TWorkloadDescriptor workloadDescriptor;
        workloadDescriptor.Category = EWorkloadCategory::SystemTabletLogging;
        workloadDescriptor.Annotations.push_back(Format("Seal of chunk %v",
            chunkId));

        auto updateGuard = TChunkUpdateGuard::Acquire(chunk);

        const auto& journalDispatcher = Bootstrap_->GetJournalDispatcher();
        const auto& location = journalChunk->GetStoreLocation();
        auto changelog = WaitFor(journalDispatcher->OpenChangelog(location, chunkId))
            .ValueOrThrow();

        i64 currentRowCount = changelog->GetRecordCount();
        if (currentRowCount < sealRowCount) {
            YT_LOG_DEBUG("Job will read missing journal chunk rows (Rows: %v-%v)",
                currentRowCount,
                sealRowCount - 1);

            auto reader = NJournalClient::CreateChunkReader(
                Config_->SealReader,
                Bootstrap_->GetMasterClient(),
                nodeDirectory,
                chunkId,
                codecId,
                sourceReplicas,
                Bootstrap_->GetBlockCache(),
                /* trafficMeter */ nullptr,
                Bootstrap_->GetReplicationInThrottler());

            // TODO(savrus) profile chunk reader statistics.
            TClientBlockReadOptions blockReadOptions;
            blockReadOptions.WorkloadDescriptor = workloadDescriptor;
            blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

            while (currentRowCount < sealRowCount) {
                YT_LOG_DEBUG("Reading rows (Rows: %v-%v)",
                    currentRowCount,
                    sealRowCount - 1);

                auto asyncBlocks = reader->ReadBlocks(
                    blockReadOptions,
                    currentRowCount,
                    sealRowCount - currentRowCount);
                auto blocks = WaitFor(asyncBlocks)
                    .ValueOrThrow();

                int blockCount = blocks.size();
                if (blockCount == 0) {
                    THROW_ERROR_EXCEPTION("Rows %v-%v are missing but needed to seal chunk %v",
                        currentRowCount,
                        sealRowCount - 1,
                        chunkId);
                }

                YT_LOG_DEBUG("Rows received (Rows: %v-%v)",
                    currentRowCount,
                    currentRowCount + blockCount - 1);

                std::vector<TSharedRef> records;
                records.reserve(blocks.size());
                for (const auto& block : blocks) {
                    records.push_back(block.Data);
                }
                changelog->Append(records);

                currentRowCount += blockCount;
            }

            WaitFor(changelog->Flush())
                .ThrowOnError();

            YT_LOG_DEBUG("Finished downloading missing journal chunk rows");
        }

        YT_LOG_DEBUG("Started sealing journal chunk (RowCount: %v)",
            sealRowCount);

        WaitFor(journalChunk->Seal())
            .ThrowOnError();

        YT_LOG_DEBUG("Finished sealing journal chunk");

        journalChunk->UpdateCachedParams(changelog);

        const auto& chunkStore = Bootstrap_->GetChunkStore();
        chunkStore->UpdateExistingChunk(chunk);
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateChunkJob(
    TJobId jobId,
    TJobSpec&& jobSpec,
    const TNodeResources& resourceLimits,
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
{
    auto type = CheckedEnumCast<EJobType>(jobSpec.type());
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
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode


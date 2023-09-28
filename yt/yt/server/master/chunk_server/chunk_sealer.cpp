#include "chunk_sealer.h"

#include "private.h"
#include "chunk.h"
#include "chunk_autotomizer.h"
#include "chunk_list.h"
#include "chunk_location.h"
#include "chunk_tree.h"
#include "chunk_manager.h"
#include "chunk_owner_base.h"
#include "chunk_replicator.h"
#include "config.h"
#include "helpers.h"
#include "chunk_scanner.h"
#include "job.h"
#include "job_registry.h"
#include "private.h"

#include <yt/yt/server/master/chunk_server/proto/chunk_autotomizer.pb.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <deque>

namespace NYT::NChunkServer {

using namespace NRpc;
using namespace NConcurrency;
using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NJournalClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NCellMaster;
using namespace NProfiling;

using NYT::ToProto;
using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSealJob
    : public TJob
{
public:
    TSealJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes)
        : TJob(
            jobId,
            EJobType::SealChunk,
            jobEpoch,
            node,
            TSealJob::GetResourceUsage(),
            TChunkIdWithIndexes(ToChunkIdWithIndexes(chunkWithIndexes)))
        , Chunk_(chunkWithIndexes.GetPtr())
    { }

    bool FillJobSpec(TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const override
    {
        if (!IsObjectAlive(Chunk_)) {
            return false;
        }

        auto chunkId = GetChunkIdWithIndexes();

        auto* jobSpecExt = jobSpec->MutableExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext);
        ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkId));
        jobSpecExt->set_codec_id(ToProto<int>(Chunk_->GetErasureCodec()));
        jobSpecExt->set_medium_index(chunkId.MediumIndex);
        jobSpecExt->set_row_count(Chunk_->GetPhysicalSealedRowCount());

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

        // This one should not have sequoia replicas.
        const auto& replicas = Chunk_->StoredReplicas();
        builder.Add(replicas);
        for (auto replica : replicas) {
            const auto* location = replica.GetPtr();
            jobSpecExt->add_legacy_source_replicas(ToProto<ui32>(TNodePtrWithReplicaIndex(
                location->GetNode(),
                replica.GetReplicaIndex())));
            jobSpecExt->add_source_replicas(ToProto<ui64>(TNodePtrWithReplicaAndMediumIndex(
                location->GetNode(),
                replica.GetReplicaIndex(),
                location->GetEffectiveMediumIndex())));
        }

        return true;
    }

private:
    const TEphemeralObjectPtr<TChunk> Chunk_;

    static TNodeResources GetResourceUsage()
    {
        TNodeResources resourceUsage;
        resourceUsage.set_seal_slots(1);

        return resourceUsage;
    }
};

DEFINE_REFCOUNTED_TYPE(TSealJob)

////////////////////////////////////////////////////////////////////////////////

class TChunkSealer
    : public IChunkSealer
{
public:
    explicit TChunkSealer(TBootstrap* bootstrap)
        : Config_(bootstrap->GetConfig()->ChunkManager)
        , Bootstrap_(bootstrap)
        , SuccessfulSealCounter_(ChunkServerProfiler.Counter("/chunk_sealer/successful_seals"))
        , UnsuccessfuleSealCounter_(ChunkServerProfiler.Counter("/chunk_sealer/unsuccessful_seals"))
        , SealScanner_(std::make_unique<TChunkScanner>(
            Bootstrap_->GetObjectManager(),
            EChunkScanKind::Seal,
            /*isJournal*/ true))
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TChunkSealer::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Start() override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            SealScanner_->Start(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));
        }

        SealExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkSealer),
            BIND(&TChunkSealer::OnRefresh, MakeWeak(this)),
            GetDynamicConfig()->ChunkRefreshPeriod);
        SealExecutor_->Start();

        YT_VERIFY(!std::exchange(Running_, true));

        YT_LOG_INFO("Chunk sealer started");
    }

    void Stop() override
    {
        if (!Running_) {
            return;
        }

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            SealScanner_->Stop(shardIndex);
        }

        SealExecutor_->Stop();
        SealExecutor_.Reset();

        YT_VERIFY(std::exchange(Running_, false));

        YT_LOG_INFO("Chunk sealer stopped");
    }

    bool IsEnabled() override
    {
        return Enabled_;
    }

    void ScheduleSeal(TChunk* chunk) override
    {
        YT_ASSERT(IsObjectAlive(chunk));

        // Fast path.
        if (!Running_) {
            return;
        }

        if (IsSealNeeded(chunk)) {
            SealScanner_->EnqueueChunk(chunk);
        }
    }

    void OnChunkDestroyed(TChunk* chunk) override
    {
        SealScanner_->OnChunkDestroyed(chunk);
    }

    void OnProfiling(TSensorBuffer* buffer) const override
    {
        buffer->AddGauge("/seal_queue_size", SealScanner_->GetQueueSize());
    }

    // IJobController implementation.
    void ScheduleJobs(EJobType jobType, IJobSchedulingContext* context) override
    {
        YT_VERIFY(IsMasterJobType(jobType));

        if (jobType != EJobType::SealChunk) {
            return;
        }

        auto* node = context->GetNode();
        const auto& resourceUsage = context->GetNodeResourceUsage();
        const auto& resourceLimits = context->GetNodeResourceLimits();

        int misscheduledSealJobs = 0;

        auto hasSpareSealResources = [&] {
            return
                misscheduledSealJobs < GetDynamicConfig()->MaxMisscheduledSealJobsPerHeartbeat &&
                resourceUsage.seal_slots() < resourceLimits.seal_slots();
        };

        TCompactVector<
            std::pair<TChunkLocation*, TChunkLocation::TChunkSealQueue::iterator>,
            TypicalChunkLocationCount> locations;
        for (auto* location : node->ChunkLocations()) {
            auto& queue = location->ChunkSealQueue();
            if (!queue.empty()) {
                locations.emplace_back(location, queue.begin());
            }
        }
        int activeLocationCount = std::ssize(locations);

        while (activeLocationCount > 0 && hasSpareSealResources()) {
            for (auto& [location, replicaIterator] : locations) {
                if (!hasSpareSealResources()) {
                    break;
                }

                auto& queue = location->ChunkSealQueue();
                if (replicaIterator == queue.end()) {
                    continue;
                }

                auto replica = replicaIterator;
                if (++replicaIterator == queue.end()) {
                    --activeLocationCount;
                }

                auto* chunk = replica->GetPtr();
                if (!chunk->IsRefreshActual()) {
                    queue.erase(replica);
                    ++misscheduledSealJobs;
                    continue;
                }

                TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes(
                    chunk,
                    replica->GetReplicaIndex(),
                    location->GetEffectiveMediumIndex());
                if (TryScheduleSealJob(context, chunkWithIndexes)) {
                    queue.erase(replica);
                } else {
                    ++misscheduledSealJobs;
                }
            }
        }
    }

    void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        // In Chunk Sealer we don't distinguish between waiting and running jobs.
        OnJobRunning(job, callbacks);
    }

    void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        if (TInstant::Now() - job->GetStartTime() > GetDynamicConfig()->JobTimeout) {
            YT_LOG_WARNING("Job timed out, aborting (JobId: %v, JobType: %v, Address: %v, Duration: %v, ChunkId: %v)",
                job->GetJobId(),
                job->GetType(),
                job->NodeAddress(),
                TInstant::Now() - job->GetStartTime(),
                job->GetChunkIdWithIndexes());

            callbacks->AbortJob(job);
        }
    }

    void OnJobCompleted(const TJobPtr& /*job*/) override
    { }

    void OnJobAborted(const TJobPtr& /*job*/) override
    { }

    void OnJobFailed(const TJobPtr& /*job*/) override
    { }

private:
    const TChunkManagerConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    // Profiling stuff.
    const TCounter SuccessfulSealCounter_;
    const TCounter UnsuccessfuleSealCounter_;

    const TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(0);

    TPeriodicExecutorPtr SealExecutor_;
    const std::unique_ptr<TChunkScanner> SealScanner_;

    bool Enabled_ = true;
    bool Running_ = false;

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        if (SealExecutor_) {
            SealExecutor_->SetPeriod(GetDynamicConfig()->ChunkRefreshPeriod);
        }
        Semaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentChunkSeals);
    }

    static bool IsSealNeeded(TChunk* chunk)
    {
        return
            IsObjectAlive(chunk) &&
            chunk->IsJournal() &&
            chunk->IsConfirmed() &&
            !chunk->IsSealed();
    }

    static bool IsAttached(TChunk* chunk)
    {
        return chunk->HasParents();
    }

    static bool IsLocked(TChunk* chunk)
    {
        for (auto [parent, cardinality] : chunk->Parents()) {
            auto nodes = GetOwningNodes(parent);
            for (auto* node : nodes) {
                if (node->GetUpdateMode() != EUpdateMode::None) {
                    return true;
                }
            }
        }
        return false;
    }

    bool HasEnoughReplicas(TChunk* chunk)
    {
        // This one should not have sequoia replicas.
        return std::ssize(chunk->StoredReplicas()) >= chunk->GetReadQuorum();
    }

    static bool IsFirstUnsealedInChunkList(TChunk* chunk)
    {
        for (auto [chunkTree, cardinality] : chunk->Parents()) {
            const auto* chunkList = chunkTree->As<TChunkList>();
            int index = GetChildIndex(chunkList, chunk);
            if (index > 0 && !chunkList->Children()[index - 1]->IsSealed()) {
                return false;
            }
        }
        return true;
    }

    bool CanBeSealed(TChunk* chunk)
    {
        if (!IsSealNeeded(chunk) || !HasEnoughReplicas(chunk)) {
            return false;
        }

        // Chunk was forcefully marked as sealable.
        if (chunk->GetSealable()) {
            return true;
        }

        const auto& parents = chunk->Parents();
        if (std::ssize(parents) != 1 || parents.begin()->first->AsChunkList()->GetKind() != EChunkListKind::JournalRoot) {
            // Chunk does not belong to a journal, probably it is a journal hunk chunk.
            return false;
        }

        // Chunk is ready to be sealed in journal.
        return
            IsAttached(chunk) &&
            !IsLocked(chunk) &&
            IsFirstUnsealedInChunkList(chunk);
    }


    void RescheduleSeal(TChunkId chunkId)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(chunkId);
        if (IsSealNeeded(chunk)) {
            EnqueueChunk(chunk);
        }
    }

    void EnqueueChunk(TChunk* chunk)
    {
        if (!SealScanner_->EnqueueChunk(chunk)) {
            return;
        }

        YT_LOG_DEBUG("Chunk added to seal queue (ChunkId: %v)",
            chunk->GetId());
    }

    void OnRefresh()
    {
        OnCheckEnabled();

        if (!IsEnabled()) {
            return;
        }

        int totalCount = 0;
        while (totalCount < GetDynamicConfig()->MaxChunksPerSeal &&
               SealScanner_->HasUnscannedChunk())
        {
            auto guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_);
            if (!guard) {
                return;
            }

            ++totalCount;
            auto [chunk, errorCount] = SealScanner_->DequeueChunk();
            if (!chunk) {
                continue;
            }

            if (CanBeSealed(chunk)) {
                BIND(&TChunkSealer::SealChunk, MakeStrong(this), chunk->GetId(), Passed(std::move(guard)))
                    .AsyncVia(GetCurrentInvoker())
                    .Run();
            }
        }
    }

    void OnCheckEnabled()
    {
        auto enabledInConfig = GetDynamicConfig()->EnableChunkSealer;

        if (!enabledInConfig && Enabled_) {
            YT_LOG_INFO("Chunk sealer disabled");
            Enabled_ = false;
            return;
        }

        if (enabledInConfig && !Enabled_) {
            YT_LOG_INFO("Chunk sealer enabled");
            Enabled_ = true;
            return;
        }
    }

    void SealChunk(
        TChunkId chunkId,
        TAsyncSemaphoreGuard /*guard*/)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(chunkId);
        if (!CanBeSealed(chunk)) {
            return;
        }

        try {
            GuardedSealChunk(chunk);
            SuccessfulSealCounter_.Increment();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error sealing journal chunk; backing off (ChunkId: %v)",
                chunkId);

            UnsuccessfuleSealCounter_.Increment();

            TDelayedExecutor::Submit(
                BIND(&TChunkSealer::RescheduleSeal, MakeStrong(this), chunkId)
                    .Via(GetCurrentInvoker()),
                GetDynamicConfig()->ChunkSealBackoffTime);
        }
    }

    void GuardedSealChunk(TChunk* chunk)
    {
        // NB: Copy all the needed properties into locals. The subsequent code involves yields
        // and the chunk may expire. See YT-8120.
        auto chunkId = chunk->GetId();
        auto overlayed = chunk->GetOverlayed();
        auto codecId = chunk->GetErasureCodec();
        auto startRowIndex = GetJournalChunkStartRowIndex(chunk);
        auto readQuorum = chunk->GetReadQuorum();
        auto writeQuorum = chunk->GetWriteQuorum();
        auto replicaLagLimit = chunk->GetReplicaLagLimit();
        auto replicas = GetChunkReplicaDescriptors(chunk);
        auto dynamicConfig = GetDynamicConfig();

        if (replicas.empty()) {
            THROW_ERROR_EXCEPTION("No replicas of chunk %v are known",
                chunkId);
        }

        YT_LOG_DEBUG("Sealing journal chunk (ChunkId: %v, Overlayed: %v, StartRowIndex: %v)",
            chunkId,
            overlayed,
            startRowIndex);

        std::vector<TChunkReplicaDescriptor> abortedReplicas;
        {
            auto future = AbortSessionsQuorum(
                chunkId,
                replicas,
                dynamicConfig->JournalRpcTimeout,
                dynamicConfig->QuorumSessionDelay,
                readQuorum,
                Bootstrap_->GetNodeChannelFactory());
            abortedReplicas = WaitFor(future)
                .ValueOrThrow();
        }

        TChunkQuorumInfo quorumInfo;
        {
            auto future = ComputeQuorumInfo(
                chunkId,
                overlayed,
                codecId,
                readQuorum,
                replicaLagLimit,
                abortedReplicas,
                dynamicConfig->JournalRpcTimeout,
                Bootstrap_->GetNodeChannelFactory());
            quorumInfo = WaitFor(future)
                .ValueOrThrow();
        }

        auto firstOverlayedRowIndex = quorumInfo.FirstOverlayedRowIndex;
        if (firstOverlayedRowIndex && *firstOverlayedRowIndex > startRowIndex) {
            THROW_ERROR_EXCEPTION("Sealing chunk %v would produce row gap",
                chunkId)
                << TErrorAttribute("start_row_index", startRowIndex)
                << TErrorAttribute("first_overlayed_row_index", *quorumInfo.FirstOverlayedRowIndex);
        }

        i64 sealedRowCount = quorumInfo.RowCount;

        TChunkSealInfo chunkSealInfo;
        if (firstOverlayedRowIndex) {
            chunkSealInfo.set_first_overlayed_row_index(*firstOverlayedRowIndex);
        }
        chunkSealInfo.set_row_count(sealedRowCount);
        chunkSealInfo.set_uncompressed_data_size(quorumInfo.UncompressedDataSize);
        chunkSealInfo.set_compressed_data_size(quorumInfo.CompressedDataSize);

        if (quorumInfo.RowCountConfirmedReplicaCount >= writeQuorum &&
            !GetDynamicConfig()->Testing->ForceUnreliableSeal)
        {
            // Fast path: sealed row count can be reliably evaluated.
            YT_LOG_DEBUG("Sealed row count evaluated (ChunkId: %v, SealedRowCount: %v)",
                chunkId,
                sealedRowCount);
        } else {
            // Sealed row count cannot be reliably evaluated. We try to autotomize chunk, if possible,
            // otherwise just seal chunk unreliably and hope for the best.
            YT_LOG_DEBUG("Sealed row count computed unreliably (ChunkId: %v, SealedRowCount: %v, RowCountConfirmedReplicaCount: %v, WriteQuorum: %v)",
                chunkId,
                sealedRowCount,
                quorumInfo.RowCountConfirmedReplicaCount,
                writeQuorum);

            if (!dynamicConfig->EnableChunkAutotomizer) {
                YT_LOG_DEBUG("Chunk automotizer is disabled; sealing chunk unreliably (ChunkId: %v)",
                    chunkId);
            } else if (!overlayed) {
                YT_LOG_DEBUG("Cannot automotize chunk since it is not overlayed; sealing chunk unreliably (ChunkId: %v)",
                    chunkId);
            } else {
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto& chunkAutotomizer = chunkManager->GetChunkAutotomizer();
                if (chunkAutotomizer->IsChunkRegistered(chunkId)) {
                    // Fast path: if chunk is already registered in autotomizer, do not create useless mutation.
                    YT_LOG_DEBUG("Chunk is already registered in autotomizer, skipping (ChunkId: %v)",
                        chunkId);
                } else {
                    auto guaranteedQuorumRowCount = std::max<i64>(sealedRowCount - replicaLagLimit, 0);
                    chunkSealInfo.set_row_count(guaranteedQuorumRowCount);

                    YT_LOG_DEBUG("Autotomizing chunk (ChunkId: %v, GuaranteedQuorumRowCount: %v)",
                        chunkId,
                        guaranteedQuorumRowCount);

                    NProto::TReqAutotomizeChunk request;
                    ToProto(request.mutable_chunk_id(), chunkId);
                    request.mutable_chunk_seal_info()->Swap(&chunkSealInfo);

                    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
                    CreateMutation(hydraManager, request)
                        ->CommitAndLog(Logger);
                }

                // Probably next seal attempt will succeed and will happen before autotomy end.
                if (IsSealNeeded(chunk)) {
                    EnqueueChunk(chunk);
                }

                return;
            }
        }

        YT_LOG_DEBUG("Requesting chunk seal (ChunkId: %v)",
            chunkId);

        {
            TChunkServiceProxy proxy(Bootstrap_->GetLocalRpcChannel());

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            SetSuppressUpstreamSync(&batchReq->Header(), true);
            // COMPAT(shakurov): prefer proto ext (above).
            batchReq->set_suppress_upstream_sync(true);

            auto* req = batchReq->add_seal_chunk_subrequests();
            ToProto(req->mutable_chunk_id(), chunkId);
            req->mutable_info()->Swap(&chunkSealInfo);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Failed to seal chunk %v",
                chunkId);
        }
    }

    bool TryScheduleSealJob(
        IJobSchedulingContext* context,
        TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes)
    {
        auto* chunk = chunkWithIndexes.GetPtr();
        YT_VERIFY(chunk->IsJournal());
        YT_VERIFY(chunk->IsSealed());

        if (!IsObjectAlive(chunk)) {
            return true;
        }

        if (chunk->HasJobs()) {
            return true;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();
        if (!chunkReplicator->ShouldProcessChunk(chunk)) {
            return true;
        }

        // NB: Seal jobs can be started even if chunk refresh is scheduled.

        // This one should not have sequoia replicas.
        if (std::ssize(chunk->StoredReplicas()) < chunk->GetReadQuorum()) {
            return true;
        }

        // NB: Seal jobs are scheduled from seal queues which are filled
        // by chunk replicator, so chunk sealer reuses replicator's job epochs.
        auto jobEpoch = chunkReplicator->GetJobEpoch(chunk);
        YT_VERIFY(jobEpoch != InvalidJobEpoch);

        auto job = New<TSealJob>(
            context->GenerateJobId(),
            jobEpoch,
            context->GetNode(),
            chunkWithIndexes);
        context->ScheduleJob(job);

        YT_LOG_DEBUG("Seal job scheduled "
            "(JobId: %v, JobEpoch: %v, Address: %v, ChunkId: %v)",
            job->GetJobId(),
            job->GetJobEpoch(),
            job->NodeAddress(),
            chunkWithIndexes);

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkSealerPtr CreateChunkSealer(NCellMaster::TBootstrap* bootstrap)
{
    return New<TChunkSealer>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer


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
using namespace NChunkServer::NProto;
using namespace NJournalClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NCellMaster;
using namespace NProfiling;

using NYT::ToProto;
using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

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

    bool FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const override
    {
        if (!IsObjectAlive(Chunk_)) {
            return false;
        }

        auto chunkId = GetChunkIdWithIndexes();

        auto* jobSpecExt = jobSpec->MutableExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext);
        ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkId));
        jobSpecExt->set_codec_id(ToProto(Chunk_->GetErasureCodec()));
        jobSpecExt->set_medium_index(chunkId.MediumIndex);
        jobSpecExt->set_row_count(Chunk_->GetPhysicalSealedRowCount());

        const auto& chunkManager = bootstrap->GetChunkManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        // This is context switch, so we should check that chunk is still alive.
        // TODO(grphil): Do not fetch replicas here (they should be already fetched in chunk sealer).
        auto replicasOrError = chunkReplicaFetcher->GetChunkReplicas(Chunk_);
        if (!replicasOrError.IsOK() || !IsObjectAlive(Chunk_)) {
            return false;
        }

        const auto& replicas = replicasOrError.Value();

        if (Chunk_->GetReadQuorum() > std::ssize(replicas)) {
            return false;
        }

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

        // Outdated replica list may be present in ChunkReplicas.
        for (auto replica : replicas) {
            builder.Add(replica);
            if (auto* locationReplica = replica.As<EStoredReplicaType::ChunkLocation>()) {
                auto* location = locationReplica->AsChunkLocationPtr();
                jobSpecExt->add_source_replicas(ToProto(TNodePtrWithReplicaAndMediumIndex(
                    location->GetNode(),
                    replica.GetReplicaIndex(),
                    location->GetEffectiveMediumIndex())));
            }
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

DECLARE_REFCOUNTED_STRUCT(TSealContext)

struct TSealContext
    : public TRefCounted
{
    TSealContext(
        TChunk* chunk,
        const std::pair<int, TCpuInstant>& scannerPayload,
        TAsyncSemaphoreGuard&& guard)
        : Chunk(chunk)
        , RescheduleCount(scannerPayload.first)
        , EnqueueTime(scannerPayload.second)
        , Guard(std::move(guard))
    { }

    const TEphemeralObjectPtr<TChunk> Chunk;
    const int RescheduleCount;
    const TCpuInstant EnqueueTime;
    const TAsyncSemaphoreGuard Guard;
    std::vector<TChunkReplicaDescriptor> ReplicaDescriptors;
};

DEFINE_REFCOUNTED_TYPE(TSealContext)

////////////////////////////////////////////////////////////////////////////////

class TChunkSealer
    : public IChunkSealer
{
public:
    explicit TChunkSealer(TBootstrap* bootstrap)
        : Config_(bootstrap->GetConfig()->ChunkManager)
        , Bootstrap_(bootstrap)
        , SuccessfulSealCounter_(ChunkServerProfiler().Counter("/chunk_sealer/successful_seals"))
        , UnsuccessfulSealCounter_(ChunkServerProfiler().Counter("/chunk_sealer/unsuccessful_seals"))
        , SuccessfulSealTime_(ChunkServerProfiler().TimeCounter("/chunk_sealer/successful_seal_time"))
        , SealScanner_(std::make_unique<TChunkSealScanner>(
            EChunkScanKind::Seal,
            /*isJournal*/ true))
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TChunkSealer::OnDynamicConfigChanged, MakeWeak(this)));
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

        YT_UNUSED_FUTURE(SealExecutor_->Stop());
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
            EnqueueChunk(chunk, 0, GetCpuInstant());
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

        TCompactVector<
            std::pair<TChunkLocation*, TChunkLocation::TChunkSealQueue::iterator>,
            TypicalChunkLocationCount> locations;
        for (auto location : node->ChunkLocations()) {
            auto& queue = location->ChunkSealQueue();
            if (!queue.empty()) {
                locations.emplace_back(location, queue.begin());
            }
        }
        int activeLocationCount = std::ssize(locations);
        int sealSlots = resourceLimits.seal_slots() - resourceUsage.seal_slots();
        auto badSealSlots = GetDynamicConfig()->MaxMisscheduledSealJobsPerHeartbeat;

        auto canPlanNewSeal = [&] {
            return sealSlots > 0 && badSealSlots > 0;
        };

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        std::vector<TScheduleSealJobContext> scheduleJobContexts;
        std::vector<TEphemeralObjectPtr<TChunk>> chunksToFetchReplicas;
        while (activeLocationCount > 0 && canPlanNewSeal()) {
            for (auto& [location, replicaIterator] : locations) {
                if (!canPlanNewSeal()) {
                    break;
                }

                auto& queue = location->ChunkSealQueue();
                if (replicaIterator == queue.end()) {
                    continue;
                }

                auto chunkIdWithIndex = replicaIterator;
                if (++replicaIterator == queue.end()) {
                    --activeLocationCount;
                }

                auto chunkId = chunkIdWithIndex->first.Id;
                auto* chunk = chunkManager->FindChunk(chunkId);
                if (!IsObjectAlive(chunk)) {
                    queue.erase(chunkIdWithIndex);
                    --badSealSlots;
                    continue;
                }

                if (!chunk->IsRefreshActual()) {
                    queue.erase(chunkIdWithIndex);
                    --badSealSlots;
                    continue;
                }

                scheduleJobContexts.emplace_back(location, chunk, chunkIdWithIndex->first);
                chunksToFetchReplicas.emplace_back(chunk);
                --sealSlots;
            }
        }

        const auto& chunkReplicaFetcher = Bootstrap_->GetChunkManager()->GetChunkReplicaFetcher();
        auto replicas = chunkReplicaFetcher->GetChunkReplicas(chunksToFetchReplicas);

        for (const auto& scheduleJobContext : scheduleJobContexts) {
            if (resourceUsage.seal_slots() >= resourceLimits.seal_slots()) {
                break;
            }

            if (!IsObjectAlive(scheduleJobContext.Location)) {
                continue;
            }

            auto& queue = scheduleJobContext.Location->ChunkSealQueue();
            if (!queue.contains(scheduleJobContext.ChunkIdWithIndex)) {
                continue;
            }

            if (!IsObjectAlive(scheduleJobContext.Chunk)) {
                EraseOrCrash(queue, scheduleJobContext.ChunkIdWithIndex);
                continue;
            }

            if (!scheduleJobContext.Chunk->IsRefreshActual()) {
                EraseOrCrash(queue, scheduleJobContext.ChunkIdWithIndex);
                continue;
            }

            auto onSealFailed = [&] {
                if (auto it = queue.find(scheduleJobContext.ChunkIdWithIndex); it != queue.end()) {
                    ++it->second;
                    if (it->second > GetDynamicConfig()->MaxUnsuccessfulScheduleSealJobAttemptsPerChunkReplica) {
                        YT_LOG_ALERT(
                            "Too many unsuccessful attempts to schedule seal job for chunk replica (ChunkId: %v, ReplicaIndex: %v, FailedSealAttempts: %v)",
                            scheduleJobContext.Chunk->GetId(),
                            scheduleJobContext.ChunkIdWithIndex.ReplicaIndex,
                            it->second);
                    }
                }
            };

            const auto& replicasOrError = GetOrCrash(replicas, scheduleJobContext.Chunk->GetId());
            if (!replicasOrError.IsOK()) {
                YT_LOG_DEBUG(
                    replicasOrError,
                    "Failed to schedule chunk seal job, failed to fetch replicas (ChunkId: %v, ReplicaIndex: %v)",
                    scheduleJobContext.Chunk->GetId(),
                    scheduleJobContext.ChunkIdWithIndex.ReplicaIndex);

                onSealFailed();
                continue;
            }

            auto replicas = replicasOrError.Value();
            auto chunkWithIndexes = TChunkPtrWithReplicaAndMediumIndex(
                scheduleJobContext.Chunk.Get(),
                scheduleJobContext.ChunkIdWithIndex.ReplicaIndex,
                scheduleJobContext.Location->GetEffectiveMediumIndex());
            if (TryScheduleSealJob(context, chunkWithIndexes, replicas)) {
                EraseOrCrash(queue, scheduleJobContext.ChunkIdWithIndex);
            } else {
                onSealFailed();
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
    const TCounter UnsuccessfulSealCounter_;
    const TTimeCounter SuccessfulSealTime_;

    const TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(0);

    TPeriodicExecutorPtr SealExecutor_;

    // Scanner tracks error count.
    using TChunkSealScanner = TChunkScannerWithPayload<std::pair<int, TCpuInstant>>;
    const std::unique_ptr<TChunkSealScanner> SealScanner_;

    bool Enabled_ = true;
    bool Running_ = false;

    struct TScheduleSealJobContext
    {
        TScheduleSealJobContext(TChunkLocation* location, TChunk* chunk, const TChunkIdWithIndex& chunkIdWithIndex)
            : Location(location)
            , Chunk(chunk)
            , ChunkIdWithIndex(chunkIdWithIndex)
        { }

        TEphemeralObjectPtr<TChunkLocation> Location;
        TEphemeralObjectPtr<TChunk> Chunk; // Just to prevent chunk destruction.
        const TChunkIdWithIndex ChunkIdWithIndex;
    };

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

    static bool IsSealNeeded(const TChunk* chunk)
    {
        return
            IsObjectAlive(chunk) &&
            chunk->IsJournal() &&
            chunk->IsConfirmed() &&
            !chunk->IsSealed();
    }

    static bool IsAttached(const TChunk* chunk)
    {
        return chunk->HasParents();
    }

    static bool IsLocked(const TChunk* chunk)
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

    // This check is not safe as some fetched replicas may be already dead.
    bool HasEnoughReplicas(TSealContextPtr sealContext)
    {
        return std::ssize(sealContext->ReplicaDescriptors) >= sealContext->Chunk->GetReadQuorum();
    }

    static bool IsFirstUnsealedInChunkList(const TChunk* chunk)
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

    bool CanBeSealed(const TChunk* chunk)
    {
        if (!IsSealNeeded(chunk)) {
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

    void RescheduleSeal(TSealContextPtr sealContext, bool delayed)
    {
        if (!IsSealNeeded(sealContext->Chunk.Get())) {
            return;
        }

        if (sealContext->RescheduleCount >= GetDynamicConfig()->MaxUnsuccessfulSealAttempts) {
            YT_LOG_ALERT("Too many unsuccessful seal attempts for chunk (ChunkId: %v, AttemptsCount: %v)",
                sealContext->Chunk->GetId(),
                sealContext->RescheduleCount);
        }

        EnqueueChunk(
            sealContext->Chunk.Get(),
            sealContext->RescheduleCount + 1,
            sealContext->EnqueueTime,
            delayed);
    }

    void EnqueueChunk(TChunk* chunk, int errorCount, TCpuInstant enqueueTime, bool delayed = false)
    {
        auto adjustedDelay = delayed
            ? std::make_optional(DurationToCpuDuration(GetDynamicConfig()->ChunkSealBackoffTime))
            : std::nullopt;
        if (!SealScanner_->EnqueueChunk({chunk, {errorCount, enqueueTime}}, adjustedDelay)) {
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

        std::vector<TEphemeralObjectPtr<TChunk>> chunksToFetchReplicas;
        std::vector<TSealContextPtr> pendingSeals;
        int totalCount = 0;
        while (totalCount < GetDynamicConfig()->MaxChunksPerSeal &&
               SealScanner_->HasUnscannedChunk())
        {
            auto guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_);
            if (!guard) {
                break;
            }

            ++totalCount;
            auto [chunk, payload] = SealScanner_->DequeueChunk();
            if (!chunk) {
                continue;
            }

            if (CanBeSealed(chunk)) {
                chunksToFetchReplicas.emplace_back(chunk);
                pendingSeals.emplace_back(New<TSealContext>(chunk, payload, std::move(guard)));
            }
        }

        const auto& chunkReplicaFetcher = Bootstrap_->GetChunkManager()->GetChunkReplicaFetcher();
        auto replicas = chunkReplicaFetcher->GetChunkReplicas(chunksToFetchReplicas);

        for (auto sealContext : pendingSeals) {
            auto chunk = sealContext->Chunk.Get();
            if (!IsSealNeeded(chunk)) {
                continue;
            }
            const auto& replicasOrError = GetOrCrash(replicas, chunk->GetId());
            if (!replicasOrError.IsOK()) {
                YT_LOG_DEBUG(replicasOrError, "Error fetching replicas to seal chunk (ChunkId: %v)",
                    chunk->GetId());

                // We do not increase unsuccessful seal counter here, as it will be counted in unsuccessful replica fetching counters.
                RescheduleSeal(sealContext, /*delayed*/ true);
                continue;
            }

            sealContext->ReplicaDescriptors = GetChunkReplicaDescriptors(chunk, replicasOrError.Value());

            // We need to set replicas for seal context before passing it to CanBeSealed.
            if (CanBeSealed(chunk) && HasEnoughReplicas(sealContext)) {
                YT_UNUSED_FUTURE(BIND(&TChunkSealer::SealChunk, MakeStrong(this), sealContext)
                    .AsyncVia(GetCurrentInvoker())
                    .Run());
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

    void SealChunk(TSealContextPtr sealContext)
    {
        const auto* chunk = sealContext->Chunk.Get();
        if (!CanBeSealed(chunk) || !HasEnoughReplicas(sealContext)) {
            return;
        }
        // The chunk may expire so we have to save chunkId.
        auto chunkId = chunk->GetId();

        try {
            GuardedSealChunk(sealContext);
            SuccessfulSealCounter_.Increment();
            auto totalSealTime = GetCpuInstant() - sealContext->EnqueueTime;
            SuccessfulSealTime_.Add(CpuDurationToDuration(totalSealTime));
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error sealing journal chunk; backing off (ChunkId: %v)",
                chunkId);

            UnsuccessfulSealCounter_.Increment();

            RescheduleSeal(sealContext, /*delayed*/ true);
        }
    }

    void GuardedSealChunk(TSealContextPtr sealContext)
    {
        // NB: Copy all the needed properties into locals. The subsequent code involves yields
        // and the chunk may expire. See YT-8120.
        auto* chunk = sealContext->Chunk.Get();
        auto chunkId = chunk->GetId();
        auto overlayed = chunk->GetOverlayed();
        auto codecId = chunk->GetErasureCodec();
        auto maybeStartRowIndex = GetJournalChunkStartRowIndex(chunk);
        auto readQuorum = chunk->GetReadQuorum();
        auto writeQuorum = chunk->GetWriteQuorum();
        auto replicaLagLimit = chunk->GetReplicaLagLimit();
        auto dynamicConfig = GetDynamicConfig();

        if (sealContext->ReplicaDescriptors.empty()) {
            THROW_ERROR_EXCEPTION("No replicas of chunk %v are known",
                chunkId);
        }

        YT_LOG_DEBUG("Sealing journal chunk (ChunkId: %v, Overlayed: %v, StartRowIndex: %v)",
            chunkId,
            overlayed,
            maybeStartRowIndex);

        std::vector<TChunkReplicaDescriptor> abortedReplicas;
        {
            auto future = AbortSessionsQuorum(
                chunkId,
                sealContext->ReplicaDescriptors,
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
        // NB: |maybeStartRowIndex| can be null here.
        if (firstOverlayedRowIndex && *firstOverlayedRowIndex > maybeStartRowIndex) {
            THROW_ERROR_EXCEPTION("Sealing chunk %v would produce row gap",
                chunkId)
                << TErrorAttribute("start_row_index", maybeStartRowIndex)
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
                    YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
                        ->CommitAndLog(Logger()));
                }

                // Probably next seal attempt will succeed and will happen before autotomy end.
                if (IsSealNeeded(chunk)) {
                    EnqueueChunk(chunk, sealContext->RescheduleCount, sealContext->EnqueueTime);
                }

                return;
            }
        }

        YT_LOG_DEBUG("Requesting chunk seal (ChunkId: %v)",
            chunkId);

        {
            TChunkServiceProxy proxy(Bootstrap_->GetLocalRpcChannel());

            auto req = proxy.SealChunk();
            GenerateMutationId(req);

            ToProto(req->mutable_chunk_id(), chunkId);
            req->mutable_info()->Swap(&chunkSealInfo);

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Failed to seal chunk %v",
                chunkId);
        }
    }

    bool TryScheduleSealJob(
        IJobSchedulingContext* context,
        TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes,
        const TStoredChunkReplicaList& chunkReplicas)
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
        if (std::ssize(chunkReplicas) < chunk->GetReadQuorum()) {
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

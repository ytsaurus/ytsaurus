#include "chunk_sealer.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_tree.h"
#include "chunk_manager.h"
#include "chunk_owner_base.h"
#include "config.h"
#include "helpers.h"
#include "chunk_scanner.h"
#include "job.h"
#include "job_registry.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

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
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NCellMaster;
using namespace NProfiling;

using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkSealer
    : public IChunkSealer
{
public:
    explicit TChunkSealer(TBootstrap* bootstrap)
        : Config_(bootstrap->GetConfig()->ChunkManager)
        , Bootstrap_(bootstrap)
        , SuccessfulSealCounter_(ChunkServerProfilerRegistry.Counter("/chunk_sealer/successful_seals"))
        , UnsuccessfuleSealCounter_(ChunkServerProfilerRegistry.Counter("/chunk_sealer/unsuccessful_seals"))
        , SealExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
            BIND(&TChunkSealer::OnRefresh, MakeWeak(this))))
        , SealScanner_(std::make_unique<TChunkScanner>(
            Bootstrap_->GetObjectManager(),
            EChunkScanKind::Seal,
            true /*isJournal*/))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);
    }

    virtual void Start(TChunk* frontJournalChunk, int journalChunkCount) override
    {
        SealScanner_->Start(frontJournalChunk, journalChunkCount);
        SealExecutor_->Start();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    }

    virtual void Stop() override
    {
        SealExecutor_->Stop();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);
    }

    virtual bool IsEnabled() override
    {
        return Enabled_;
    }

    virtual void ScheduleSeal(TChunk* chunk) override
    {
        YT_ASSERT(chunk->IsAlive());

        if (IsSealNeeded(chunk)) {
            SealScanner_->EnqueueChunk(chunk);
        }
    }

    virtual void OnChunkDestroyed(TChunk* chunk) override
    {
        SealScanner_->OnChunkDestroyed(chunk);
    }

    virtual void OnProfiling(TSensorBuffer* buffer) const override
    {
        buffer->AddGauge("/seal_queue_size", SealScanner_->GetQueueSize());
    }

    // IJobController implementation.
    virtual void ScheduleJobs(IJobSchedulingContext* context) override
    {
        auto* node = context->GetNode();
        const auto& resourceUsage = context->GetNodeResourceUsage();
        const auto& resourceLimits = context->GetNodeResourceLimits();

        int misscheduledSealJobs = 0;

        auto hasSpareSealResources = [&] {
            return
                misscheduledSealJobs < GetDynamicConfig()->MaxMisscheduledSealJobsPerHeartbeat &&
                resourceUsage.seal_slots() < resourceLimits.seal_slots();
        };

        auto& queue = node->ChunkSealQueue();
        auto it = queue.begin();
        while (it != queue.end() && hasSpareSealResources()) {
            auto jt = it++;
            auto chunkWithIndexes = *jt;
            if (TryScheduleSealJob(context, chunkWithIndexes)) {
                queue.erase(jt);
            } else {
                ++misscheduledSealJobs;
            }
        }
    }

    void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks)
    {
        // In Chunk Sealer we don't distinguish between waiting and running jobs.
        OnJobRunning(job, callbacks);
    }

    void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks)
    {
        if (TInstant::Now() - job->GetStartTime() > GetDynamicConfig()->JobTimeout) {
            YT_LOG_WARNING("Job timed out, aborting (JobId: %v, JobType: %v, Address: %v, Duration: %v, ChunkId: %v)",
                job->GetJobId(),
                job->GetType(),
                job->GetNode()->GetDefaultAddress(),
                TInstant::Now() - job->GetStartTime(),
                job->GetChunkIdWithIndexes());

            callbacks->AbortJob(job);
        }
    }

    void OnJobCompleted(const TJobPtr& /*job*/)
    { }

    void OnJobAborted(const TJobPtr& /*job*/)
    { }

    void OnJobFailed(const TJobPtr& /*job*/)
    { }

private:
    const TChunkManagerConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    // Profiling stuff.
    const TCounter SuccessfulSealCounter_;
    const TCounter UnsuccessfuleSealCounter_;

    const TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(0);

    const TPeriodicExecutorPtr SealExecutor_;
    const std::unique_ptr<TChunkScanner> SealScanner_;

    const TCallback<void(TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TChunkSealer::OnDynamicConfigChanged, MakeWeak(this));

    bool Enabled_ = true;


    const TDynamicChunkManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        SealExecutor_->SetPeriod(GetDynamicConfig()->ChunkRefreshPeriod);
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

    static bool HasEnoughReplicas(TChunk* chunk)
    {
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

    static bool CanBeSealed(TChunk* chunk)
    {
        return
            IsSealNeeded(chunk) &&
            HasEnoughReplicas(chunk) &&
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
            auto* chunk = SealScanner_->DequeueChunk();
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
            YT_LOG_INFO("Chunk sealer disabled, see //sys/@config");
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
        auto replicaLagLimit = chunk->GetReplicaLagLimit();
        auto replicas = GetChunkReplicaDescriptors(chunk);
        auto dynamicConfig = GetDynamicConfig();

        int physicalReplicationFactor = 0;
        if (chunk->IsErasure()) {
            auto* codec = NErasure::GetCodec(codecId);
            physicalReplicationFactor = codec->GetTotalPartCount();
        } else {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
            const auto& replication = chunk->GetAggregatedReplication(requisitionRegistry);

            for (const auto& replicationEntry : replication) {
                physicalReplicationFactor += replicationEntry.Policy().GetReplicationFactor();
            }
        }

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
        if (quorumInfo.ResponseCount == physicalReplicationFactor) {
            // Fast path: sealed row count can be reliably evaluated.
            YT_LOG_DEBUG("Sealed row count evaluated (ChunkId: %v, SealedRowCount: %v)",
                chunkId,
                sealedRowCount);
        } else {
            // TODO(gritukan, shakurov): YT-14556, sealed row count cannot be reliably evaluated
            // but we will hope for the best.
            YT_LOG_DEBUG("Sealed row count computed unreliably (ChunkId: %v, SealedRowCount: %v, ResponseCount: %v, PhysicalReplicationFactor: %v)",
                chunkId,
                sealedRowCount,
                quorumInfo.ResponseCount,
                physicalReplicationFactor);
        }

        {
            TChunkServiceProxy proxy(Bootstrap_->GetLocalRpcChannel());

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            auto* req = batchReq->add_seal_chunk_subrequests();
            ToProto(req->mutable_chunk_id(), chunkId);
            if (firstOverlayedRowIndex) {
                req->mutable_info()->set_first_overlayed_row_index(*firstOverlayedRowIndex);
            }
            req->mutable_info()->set_row_count(sealedRowCount);
            req->mutable_info()->set_uncompressed_data_size(quorumInfo.UncompressedDataSize);
            req->mutable_info()->set_compressed_data_size(quorumInfo.CompressedDataSize);
            req->mutable_info()->set_physical_row_count(GetPhysicalChunkRowCount(sealedRowCount, overlayed));

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Failed to seal chunk %v",
                chunkId);
        }
    }

    bool TryScheduleSealJob(
        IJobSchedulingContext* context,
        TChunkPtrWithIndexes chunkWithIndexes)
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

        // NB: Seal jobs can be started even if chunk refresh is scheduled.

        if (std::ssize(chunk->StoredReplicas()) < chunk->GetReadQuorum()) {
            return true;
        }

        auto job = New<TSealJob>(
            context->GenerateJobId(),
            context->GetNode(),
            chunkWithIndexes);
        context->ScheduleJob(job);

        YT_LOG_DEBUG("Seal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
            job->GetJobId(),
            job->GetNode()->GetDefaultAddress(),
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


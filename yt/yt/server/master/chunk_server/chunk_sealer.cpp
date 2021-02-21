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
#include "job_tracker.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/node_tracker_server/node.h>

#include <yt/ytlib/journal_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/session_id.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

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

class TChunkSealer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TChunkManagerConfigPtr config,
        TBootstrap* bootstrap,
        TJobTrackerPtr jobTracker)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , JobTracker_(std::move(jobTracker))
        , SealExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
            BIND(&TImpl::OnRefresh, MakeWeak(this))))
        , SealScanner_(std::make_unique<TChunkScanner>(
            Bootstrap_->GetObjectManager(),
            EChunkScanKind::Seal,
            true /*isJournal*/))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);
        YT_VERIFY(JobTracker_);
    }

    void Start(TChunk* frontJournalChunk, int journalChunkCount)
    {
        SealScanner_->Start(frontJournalChunk, journalChunkCount);
        SealExecutor_->Start();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
        OnDynamicConfigChanged();
    }

    void Stop()
    {
        SealExecutor_->Stop();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);
    }

    bool IsEnabled()
    {
        return Enabled_;
    }

    void ScheduleSeal(TChunk* chunk)
    {
        YT_ASSERT(chunk->IsAlive());

        if (IsSealNeeded(chunk)) {
            SealScanner_->EnqueueChunk(chunk);
        }
    }

    void OnChunkDestroyed(TChunk* chunk)
    {
        SealScanner_->OnChunkDestroyed(chunk);
    }

    void OnProfiling(TSensorBuffer* buffer) const
    {
        buffer->AddGauge("/seal_queue_size", SealScanner_->GetQueueSize());
    }

    void ScheduleJobs(
        TNode* node,
        TNodeResources* resourceUsage,
        const TNodeResources& resourceLimits,
        std::vector<TJobPtr>* jobsToStart)
    {
        int misscheduledSealJobs = 0;

        auto hasSpareSealResources = [&] () {
            return
                misscheduledSealJobs < GetDynamicConfig()->MaxMisscheduledSealJobsPerHeartbeat &&
                resourceUsage->seal_slots() < resourceLimits.seal_slots();
        };

        auto& queue = node->ChunkSealQueue();
        auto it = queue.begin();
        while (it != queue.end() && hasSpareSealResources()) {
            auto jt = it++;
            auto chunkWithIndexes = *jt;
            TJobPtr job;
            if (CreateSealJob(node, chunkWithIndexes, &job)) {
                queue.erase(jt);
            } else {
                ++misscheduledSealJobs;
            }
            JobTracker_->RegisterJob(std::move(job), jobsToStart, resourceUsage);
        }
    }

private:
    const TChunkManagerConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const TJobTrackerPtr JobTracker_;

    const TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(0);

    const TPeriodicExecutorPtr SealExecutor_;
    const std::unique_ptr<TChunkScanner> SealScanner_;

    const TCallback<void(TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this));

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
        return chunk->StoredReplicas().size() >= chunk->GetReadQuorum();
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
                BIND(&TImpl::SealChunk, MakeStrong(this), chunk->GetId(), Passed(std::move(guard)))
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
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error sealing journal chunk; backing off (ChunkId: %v)",
                chunkId);

            TDelayedExecutor::Submit(
                BIND(&TImpl::RescheduleSeal, MakeStrong(this), chunkId)
                    .Via(GetCurrentInvoker()),
                GetDynamicConfig()->ChunkSealBackoffTime);
        }
    }

    static i64 ComputeSealedRowCount(TChunkId chunkId, i64 startRowIndex, const TChunkQuorumInfo& quorumInfo)
    {
        if (!quorumInfo.FirstOverlayedRowIndex) {
            return quorumInfo.RowCount;
        }
        if (startRowIndex < *quorumInfo.FirstOverlayedRowIndex) {
            THROW_ERROR_EXCEPTION("Sealing chunk %v would produce row gap",
                chunkId)
                << TErrorAttribute("start_row_index", startRowIndex)
                << TErrorAttribute("first_overlayed_row_index", *quorumInfo.FirstOverlayedRowIndex);
        }
        return std::max(quorumInfo.RowCount - (startRowIndex - *quorumInfo.FirstOverlayedRowIndex), static_cast<i64>(0));
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
                abortedReplicas,
                dynamicConfig->JournalRpcTimeout,
                Bootstrap_->GetNodeChannelFactory());
            quorumInfo = WaitFor(future)
                .ValueOrThrow();
        }

        {
            TChunkServiceProxy proxy(Bootstrap_->GetLocalRpcChannel());

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            auto* req = batchReq->add_seal_chunk_subrequests();
            ToProto(req->mutable_chunk_id(), chunkId);
            if (quorumInfo.FirstOverlayedRowIndex) {
                req->mutable_info()->set_first_overlayed_row_index(*quorumInfo.FirstOverlayedRowIndex);
            }
            req->mutable_info()->set_row_count(ComputeSealedRowCount(chunkId, startRowIndex, quorumInfo));
            req->mutable_info()->set_uncompressed_data_size(quorumInfo.UncompressedDataSize);
            req->mutable_info()->set_compressed_data_size(quorumInfo.CompressedDataSize);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Failed to seal chunk %v",
                chunkId);
        }

        YT_LOG_DEBUG("Journal chunk sealed (ChunkId: %v)",
            chunkId);
    }

    bool CreateSealJob(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        TJobPtr* job)
    {
        auto* chunk = chunkWithIndexes.GetPtr();
        YT_VERIFY(chunk->IsJournal());
        YT_VERIFY(chunk->IsSealed());

        if (!IsObjectAlive(chunk)) {
            return true;
        }

        if (chunk->IsJobScheduled()) {
            return true;
        }

        // NB: Seal jobs can be started even if chunk refresh is scheduled.

        if (chunk->StoredReplicas().size() < chunk->GetReadQuorum()) {
            return true;
        }

        *job = TJob::CreateSeal(
            JobTracker_->GenerateJobId(),
            chunkWithIndexes,
            node);

        YT_LOG_DEBUG("Seal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
            (*job)->GetJobId(),
            node->GetDefaultAddress(),
            chunkWithIndexes);

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkSealer::TChunkSealer(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap,
    TJobTrackerPtr jobTracker)
    : Impl_(New<TImpl>(config, bootstrap, jobTracker))
{ }

TChunkSealer::~TChunkSealer() = default;

void TChunkSealer::Start(TChunk* frontJournalChunk, int journalChunkCount)
{
    Impl_->Start(frontJournalChunk, journalChunkCount);
}

void TChunkSealer::Stop()
{
    Impl_->Stop();
}

bool TChunkSealer::IsEnabled()
{
    return Impl_->IsEnabled();
}

void TChunkSealer::ScheduleSeal(TChunk* chunk)
{
    Impl_->ScheduleSeal(chunk);
}

void TChunkSealer::OnChunkDestroyed(TChunk* chunk)
{
    Impl_->OnChunkDestroyed(chunk);
}

void TChunkSealer::OnProfiling(TSensorBuffer* buffer) const
{
    Impl_->OnProfiling(buffer);
}

void TChunkSealer::ScheduleJobs(
    TNode* node,
    TNodeResources* resourceUsage,
    const TNodeResources& resourceLimits,
    std::vector<TJobPtr>* jobsToStart)
{
    Impl_->ScheduleJobs(
        node,
        resourceUsage,
        resourceLimits,
        jobsToStart);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer


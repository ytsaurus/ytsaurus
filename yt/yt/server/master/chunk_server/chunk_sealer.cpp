#include "chunk_sealer.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_owner_base.h"
#include "config.h"
#include "helpers.h"
#include "chunk_scanner.h"

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
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NCellMaster;

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
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , SealExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
            BIND(&TImpl::OnRefresh, MakeWeak(this))))
        , SealScanner_(std::make_unique<TChunkScanner>(
            Bootstrap_->GetObjectManager(),
            EChunkScanKind::Seal))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);
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
        YT_ASSERT(chunk->IsJournal());

        if (IsSealNeeded(chunk)) {
            SealScanner_->EnqueueChunk(chunk);
        }
    }

    void OnChunkDestroyed(TChunk* chunk)
    {
        SealScanner_->OnChunkDestroyed(chunk);
    }

    int GetQueueSize() const
    {
        return SealScanner_->GetQueueSize();
    }

private:
    const TChunkManagerConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    const TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(0);

    const TPeriodicExecutorPtr SealExecutor_;
    const std::unique_ptr<TChunkScanner> SealScanner_;

    const TClosure DynamicConfigChangedCallback_ = BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this));

    bool Enabled_ = true;


    const TDynamicChunkManagerConfigPtr& GetDynamicConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager;
    }

    void OnDynamicConfigChanged()
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

    static bool CanBeSealed(TChunk* chunk)
    {
        return
            IsSealNeeded(chunk) &&
            HasEnoughReplicas(chunk) &&
            IsAttached(chunk) &&
            !IsLocked(chunk);
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
            YT_LOG_DEBUG(ex, "Error sealing journal chunk %v; backing off",
                chunkId);

            TDelayedExecutor::Submit(
                BIND(&TImpl::RescheduleSeal, MakeStrong(this), chunkId)
                    .Via(GetCurrentInvoker()),
                GetDynamicConfig()->ChunkSealBackoffTime);
        }
    }

    void GuardedSealChunk(TChunk* chunk)
    {
        ValidateChunkHasReplicas(chunk);

        // NB: Copy all the needed properties into locals. The subsequent code involves yields
        // and the chunk may expire. See YT-8120.
        auto chunkId = chunk->GetId();
        auto codecId = chunk->GetErasureCodec();
        auto readQuorum = chunk->GetReadQuorum();
        auto replicaDescriptors = GetChunkReplicaDescriptors(chunk);
        auto dynamicConfig = GetDynamicConfig();

        YT_LOG_DEBUG("Sealing journal chunk (ChunkId: %v)",
            chunkId);

        {
            auto future = AbortSessionsQuorum(
                chunkId,
                replicaDescriptors,
                dynamicConfig->JournalRpcTimeout,
                readQuorum,
                Bootstrap_->GetNodeChannelFactory());
            WaitFor(future)
                .ThrowOnError();
        }

        TMiscExt miscExt;
        {
            auto future = ComputeQuorumInfo(
                chunkId,
                codecId,
                replicaDescriptors,
                dynamicConfig->JournalRpcTimeout,
                readQuorum,
                Bootstrap_->GetNodeChannelFactory());
            miscExt = WaitFor(future)
                .ValueOrThrow();
            miscExt.set_sealed(true);
        }

        {
            TChunkServiceProxy proxy(Bootstrap_->GetLocalRpcChannel());

            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            auto* req = batchReq->add_seal_chunk_subrequests();
            ToProto(req->mutable_chunk_id(), chunkId);
            *req->mutable_misc() = miscExt;

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Failed to seal chunk %v",
                chunkId);
        }

        YT_LOG_DEBUG("Journal chunk sealed (ChunkId: %v)",
            chunkId);
    }

    void ValidateChunkHasReplicas(TChunk* chunk)
    {
        if (chunk->StoredReplicas().empty()) {
            THROW_ERROR_EXCEPTION("No replicas of chunk %v are known",
                chunk->GetId());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkSealer::TChunkSealer(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
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

int TChunkSealer::GetQueueSize() const
{
    return Impl_->GetQueueSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer


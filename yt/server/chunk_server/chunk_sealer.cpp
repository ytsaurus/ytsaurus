#include "chunk_sealer.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_owner_base.h"
#include "config.h"
#include "helpers.h"
#include "chunk_scanner.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/node_tracker_server/node.h>

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

namespace NYT {
namespace NChunkServer {

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
        , Semaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentChunkSeals))
        , SealExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
            BIND(&TImpl::OnRefresh, MakeWeak(this)),
            Config_->ChunkRefreshPeriod))
        , SealScanner_(std::make_unique<TChunkScanner>(
            Bootstrap_->GetObjectManager(),
            EChunkScanKind::Seal))
    {
        YCHECK(Config_);
        YCHECK(Bootstrap_);
    }

    void Start(TChunk* frontJournalChunk, int journalChunkCount)
    {
        SealScanner_->Start(frontJournalChunk, journalChunkCount);
        SealExecutor_->Start();
    }

    void Stop()
    {
        SealExecutor_->Stop();
    }

    void ScheduleSeal(TChunk* chunk)
    {
        Y_ASSERT(chunk->IsAlive());
        Y_ASSERT(chunk->IsJournal());

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

    const TAsyncSemaphorePtr Semaphore_;

    const TPeriodicExecutorPtr SealExecutor_;
    const std::unique_ptr<TChunkScanner> SealScanner_;


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
        return !chunk->Parents().empty();
    }

    static bool IsLocked(TChunk* chunk)
    {
        for (auto* parent : chunk->Parents()) {
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


    void RescheduleSeal(const TChunkId& chunkId)
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

        LOG_DEBUG("Chunk added to seal queue (ChunkId: %v)",
            chunk->GetId());
    }


    void OnRefresh()
    {
        int totalCount = 0;
        while (totalCount < Config_->MaxChunksPerSeal &&
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

    void SealChunk(
        const TChunkId& chunkId,
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
            LOG_DEBUG(ex, "Error sealing journal chunk %v; backing off",
                chunkId);
            TDelayedExecutor::Submit(
                BIND(&TImpl::RescheduleSeal, MakeStrong(this), chunkId)
                    .Via(GetCurrentInvoker()),
                Config_->ChunkSealBackoffTime);
        }
    }

    void GuardedSealChunk(TChunk* chunk)
    {
        // NB: Copy all the needed properties into locals. The subsequent code involves yields
        // and the chunk may expire. See YT-8120.
        auto chunkId = chunk->GetId();
        auto readQuorum = chunk->GetReadQuorum();
        auto mediumIndex = ComputeChunkMediumIndex(chunk);
        auto replicas = GetChunkReplicas(chunk);
        LOG_DEBUG("Sealing journal chunk (ChunkId: %v)",
            chunkId);

        {
            auto asyncResult = AbortSessionsQuorum(
                TSessionId(chunkId, mediumIndex),
                replicas,
                Config_->JournalRpcTimeout,
                readQuorum,
                Bootstrap_->GetNodeChannelFactory());
            WaitFor(asyncResult)
                .ThrowOnError();
        }

        TMiscExt miscExt;
        {
            auto asyncMiscExt = ComputeQuorumInfo(
                chunkId,
                replicas,
                Config_->JournalRpcTimeout,
                readQuorum,
                Bootstrap_->GetNodeChannelFactory());
            miscExt = WaitFor(asyncMiscExt)
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

        LOG_DEBUG("Journal chunk sealed (ChunkId: %v)",
            chunkId);
    }

    int ComputeChunkMediumIndex(TChunk* chunk)
    {
        auto result = InvalidMediumIndex;
        for (auto replica : chunk->StoredReplicas()) {
            auto mediumIndex = replica.GetMediumIndex();
            if (result != InvalidMediumIndex && result != mediumIndex) {
                THROW_ERROR_EXCEPTION("Journal chunk %v resides on multiple media: %v and %v",
                    chunk->GetId(),
                    result,
                    mediumIndex);
            }
            result = mediumIndex;
        }
        if (result == InvalidMediumIndex) {
            THROW_ERROR_EXCEPTION("No replicas of chunk %v are known",
                chunk->GetId());
        }
        return result;
    }

    std::vector<TNodeDescriptor> GetChunkReplicas(TChunk* chunk)
    {
        std::vector<TNodeDescriptor> replicas;
        for (auto replica : chunk->StoredReplicas()) {
            auto* node = replica.GetPtr();
            replicas.push_back(node->GetDescriptor());
        }
        return replicas;
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

} // namespace NChunkServer
} // namespace NYT


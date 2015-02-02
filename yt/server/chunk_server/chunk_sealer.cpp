#include "stdafx.h"
#include "chunk_sealer.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_owner_base.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/async_semaphore.h>
#include <core/concurrency/scheduler.h>

#include <core/ytree/ypath_client.h>

#include <ytlib/journal_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <ytlib/object_client/helpers.h>

#include <server/node_tracker_server/node.h>

#include <server/cell_master/automaton.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/bootstrap.h>

#include <deque>

namespace NYT {
namespace NChunkServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NObjectClient;
using namespace NJournalClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NCellMaster;

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
        , Semaphore_(Config_->MaxChunkConcurrentSeals)
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        for (const auto& pair : chunkManager->Chunks()) {
            auto* chunk = pair.second;
            if (chunk->IsAlive() && chunk->IsJournal()) {
                MaybeScheduleSeal(chunk);
            }
        }
    }

    void Start()
    {
        YCHECK(!RefreshExecutor_);
        auto hydraFacade = Bootstrap_->GetHydraFacade();
        RefreshExecutor_ = New<TPeriodicExecutor>(
            hydraFacade->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnRefresh, MakeWeak(this)),
            Config_->ChunkRefreshPeriod);
        RefreshExecutor_->Start();
    }

    void Stop()
    {
        if (RefreshExecutor_) {
            RefreshExecutor_->Stop();
        }
    }

    void MaybeScheduleSeal(TChunk* chunk)
    {
        YASSERT(chunk->IsAlive());
        YASSERT(chunk->IsJournal());

        if (IsSealNeeded(chunk)) {
            EnqueueChunk(chunk);
        }
    }

private:
    TChunkManagerConfigPtr Config_;
    TBootstrap* Bootstrap_;

    TAsyncSemaphore Semaphore_;

    TPeriodicExecutorPtr RefreshExecutor_;

    std::deque<TChunk*> SealQueue_;


    static bool IsSealNeeded(TChunk* chunk)
    {
        return
            chunk->IsAlive() &&
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


    void RescheduleSeal(TChunk* chunk)
    {
        if (IsSealNeeded(chunk)) {
            EnqueueChunk(chunk);
        }
        EndDequeueChunk(chunk);
    }

    void EnqueueChunk(TChunk* chunk)
    {
        if (chunk->GetSealScheduled())
            return;

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakRefObject(chunk);

        SealQueue_.push_back(chunk);
        chunk->SetSealScheduled(true);

        LOG_DEBUG("Chunk added to seal queue (ChunkId: %v)",
            chunk->GetId());
    }

    TChunk* BeginDequeueChunk()
    {
        if (SealQueue_.empty()) {
            return nullptr;
        }
        auto* chunk = SealQueue_.front();
        SealQueue_.pop_front();
        chunk->SetSealScheduled(false);
        LOG_DEBUG("Chunk extracted from seal queue (ChunkId: %v)",
            chunk->GetId());
        return chunk;
    }

    void EndDequeueChunk(TChunk* chunk)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakUnrefObject(chunk);
    }


    void OnRefresh()
    {
        int chunksDequeued = 0;
        while (true) {
            auto guard = TAsyncSemaphoreGuard::TryAcquire(&Semaphore_);
            if (!guard)
                return;

            while (true) {
                if (chunksDequeued >= Config_->MaxChunksPerRefresh)
                    return;

                auto* chunk = BeginDequeueChunk();
                if (!chunk)
                    return;

                ++chunksDequeued;

                if (!CanBeSealed(chunk)) {
                    EndDequeueChunk(chunk);
                    continue;
                }

                BIND(&TImpl::SealChunk, MakeStrong(this), chunk, Passed(std::move(guard)))
                    .AsyncVia(GetCurrentInvoker())
                    .Run();
            }

        }
    }

    void SealChunk(
        TChunk* chunk,
        TAsyncSemaphoreGuard /*guard*/)
    {
        try {
            GuardedSealChunk(chunk);
            EndDequeueChunk(chunk);
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error sealing journal chunk %v, backing off",
                chunk->GetId());
            TDelayedExecutor::Submit(
                BIND(&TImpl::RescheduleSeal, MakeStrong(this), chunk)
                    .Via(GetCurrentInvoker()),
                Config_->ChunkSealBackoffTime);
        }
    }

    void GuardedSealChunk(TChunk* chunk)
    {
        if (!CanBeSealed(chunk))
            return;

        LOG_INFO("Sealing journal chunk (ChunkId: %v)", chunk->GetId());

        std::vector<TNodeDescriptor> replicas;
        for (auto nodeWithIndex : chunk->StoredReplicas()) {
            auto* node = nodeWithIndex.GetPtr();
            replicas.push_back(node->GetDescriptor());
        }

        {
            auto asyncResult = AbortSessionsQuorum(
                chunk->GetId(),
                replicas,
                Config_->JournalRpcTimeout,
                chunk->GetReadQuorum());
            WaitFor(asyncResult)
                .ThrowOnError();
        }

        auto req = TChunkYPathProxy::Seal(FromObjectId(chunk->GetId()));
        {
            auto asyncMiscExt = ComputeQuorumInfo(
                chunk->GetId(),
                replicas,
                Config_->JournalRpcTimeout,
                chunk->GetReadQuorum());
            auto miscExt = WaitFor(asyncMiscExt)
                .ValueOrThrow();
            auto* info = req->mutable_info();
            *info = miscExt;
            info->set_sealed(true);
        }

        // NB: Double-check.
        if (!IsObjectAlive(chunk))
            return;

        auto objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();
        auto chunkProxy = objectManager->GetProxy(chunk);
        WaitFor(ExecuteVerb(rootService, req))
            .ThrowOnError();

        LOG_INFO("Journal chunk sealed (ChunkId: %v)", chunk->GetId());
    }


};

////////////////////////////////////////////////////////////////////////////////

TChunkSealer::TChunkSealer(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkSealer::~TChunkSealer()
{ }

void TChunkSealer::Start()
{
    Impl_->Start();
}


void TChunkSealer::Stop()
{
    Impl_->Stop();
}

void TChunkSealer::MaybeScheduleSeal(TChunk* chunk)
{
    Impl_->MaybeScheduleSeal(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT


#include "global_sequoia_chunk_refresher.h"

#include "config.h"
#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/connection.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/composite_compare.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NObjectServer;

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;

using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TGlobalSequoiaChunkRefreshStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("status", &TThis::Status);
    registrar.Parameter("epoch", &TThis::Epoch);
    registrar.Parameter("chunks_processed", &TThis::ChunksProcessed);
    registrar.Parameter("last_processed_chunk_id", &TThis::LastProcessedChunkId);
    registrar.Parameter("refresh_iterations", &TThis::RefreshIterations);
}

////////////////////////////////////////////////////////////////////////////////

class TGlobalSequoiaChunkRefresher
    : public IGlobalSequoiaChunkRefresher
{
public:
    TGlobalSequoiaChunkRefresher(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void AdjustRefresherState() override
    {
        Bootstrap_->VerifyPersistentStateRead();

        const auto& chunkManagerConfig = Bootstrap_->GetDynamicConfig()->ChunkManager;

        if (!Bootstrap_->IsPrimaryMaster() ||
            !Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActiveLeader() ||
            Bootstrap_->GetHydraFacade()->GetHydraManager()->GetReadOnly())
        {
            StopRefresh();
            return;
        }

        if (!chunkManagerConfig->EnableChunkRefresh ||
            !chunkManagerConfig->SequoiaChunkReplicas->Enable ||
            !chunkManagerConfig->SequoiaChunkReplicas->EnableSequoiaChunkRefresh ||
            !chunkManagerConfig->SequoiaChunkReplicas->EnableGlobalSequoiaChunkRefresh)
        {
            StopRefresh();
            return;
        }

        {
            auto guard = Guard(Lock_);

            ChunksBatchSize_.store(chunkManagerConfig->SequoiaChunkReplicas->GlobalSequoiaChunkRefreshBatchSize);
            MaxUnsuccessfulRefreshIterations_ = chunkManagerConfig->SequoiaChunkReplicas->MaxUnsuccessfulGlobalSequoiaChunkRefreshIterations;

            if (Status_.Status == EGlobalSequoiaChunkRefreshStatus::Disabled) {
                YT_LOG_DEBUG("Enabling global Sequoia chunk refresh");
                Status_.Status = EGlobalSequoiaChunkRefreshStatus::Running;
                ++Status_.Epoch;
                Status_.ChunksProcessed = 0;
                Status_.RefreshIterations = 0;
                Status_.LastProcessedChunkId = NullChunkId;
                UnsuccessfulRefreshIterations_ = 0;
            }
        }
        if (!RefreshExecutor_) {
            RefreshExecutor_ = New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                BIND(&TGlobalSequoiaChunkRefresher::RunRefreshIteration, MakeWeak(this)),
                chunkManagerConfig->SequoiaChunkReplicas->GlobalSequoiaChunkRefreshPeriod);
            RefreshExecutor_->Start();
        } else {
            RefreshExecutor_->SetPeriod(chunkManagerConfig->SequoiaChunkReplicas->GlobalSequoiaChunkRefreshPeriod);
        }
    }

    TGlobalSequoiaChunkRefreshStatus GetStatus() const override
    {
        auto guard = Guard(Lock_);

        return Status_;
    }

private:
    TBootstrap* Bootstrap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    std::atomic<int> ChunksBatchSize_ = 0;
    int MaxUnsuccessfulRefreshIterations_ = 0;

    TGlobalSequoiaChunkRefreshStatus Status_;
    std::atomic<bool> IsRefreshRunning_ = false;
    int UnsuccessfulRefreshIterations_ = 0;

    void StopRefresh()
    {
        {
            auto guard = Guard(Lock_);
            if (Status_.Status != EGlobalSequoiaChunkRefreshStatus::Disabled) {
                YT_LOG_DEBUG("Stopping global Sequoia chunk refresh");
                Status_.Status = EGlobalSequoiaChunkRefreshStatus::Disabled;
            }
        }
        if (RefreshExecutor_) {
            YT_UNUSED_FUTURE(RefreshExecutor_->Stop());
            RefreshExecutor_.Reset();
        }
    }

    void RunRefreshIteration()
    {
        if (IsRefreshRunning_.exchange(true)) {
            return;
        }
        auto finallyGuard = Finally([&] {
            IsRefreshRunning_.store(false);
        });

        i64 currentEpoch;
        TChunkId lastProcessedChunkId;
        {
            auto guard = Guard(Lock_);
            if (Status_.Status == EGlobalSequoiaChunkRefreshStatus::Completed ||
                Status_.Status == EGlobalSequoiaChunkRefreshStatus::Aborted ||
                Status_.Status == EGlobalSequoiaChunkRefreshStatus::Disabled)
            {
                return;
            }
            ++Status_.RefreshIterations;
            currentEpoch = Status_.Epoch;
            lastProcessedChunkId = Status_.LastProcessedChunkId;
        }

        YT_LOG_DEBUG("Starting global Sequoia chunk refresh iteration");

        auto chunksOrError = FetchNextChunksBatch(lastProcessedChunkId);
        if (!chunksOrError.IsOK()) {
            auto guard = Guard(Lock_);

            ++UnsuccessfulRefreshIterations_;
            auto logLevel = UnsuccessfulRefreshIterations_ > MaxUnsuccessfulRefreshIterations_ ?
                NLogging::ELogLevel::Alert :
                NLogging::ELogLevel::Debug;
            YT_LOG_EVENT(
                Logger,
                logLevel,
                chunksOrError,
                "Failed to fetch chunks for global Sequoia chunk refresh (UnsuccessfulIterations: %v)",
                UnsuccessfulRefreshIterations_);
            return;
        }
        auto chunks = std::move(chunksOrError).Value();

        {
            auto guard = Guard(Lock_);
            if (currentEpoch != Status_.Epoch) {
                return;
            }
            if (chunks.empty()) {
                YT_LOG_DEBUG("Global Sequoia chunk refresh is completed");
                Status_.Status = EGlobalSequoiaChunkRefreshStatus::Completed;
                UnsuccessfulRefreshIterations_ = 0;
                return;
            }
        }

        YT_LOG_DEBUG("Fetched chunks for global Sequoia chunk refresh (FetchedChunksCount: %v)", chunks.size());

        // We store some statistics because chunks will be moved to separate thread.
        auto lastRefreshedChunkId = chunks.back().Key.ChunkId;
        auto chunksSize = chunks.size();

        auto addToRefreshQueueResult = WaitFor(Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->StartTransaction(
                ESequoiaTransactionType::GlobalRefresh,
                {.CellTag = Bootstrap_->GetCellTag()})
            .Apply(BIND([chunks = std::move(chunks), cellId = Bootstrap_->GetCellId()] (const ISequoiaTransactionPtr& transaction)
            {
                for (const auto& chunk : chunks) {
                    transaction->WriteRow(CellTagFromId(chunk.Key.ChunkId), NRecords::TChunkRefreshQueue{
                        .TabletIndex = GetChunkShardIndex(chunk.Key.ChunkId),
                        .ChunkId = chunk.Key.ChunkId,
                        .ConfirmationTime = TInstant::Now(),
                    });
                };

                YT_LOG_DEBUG(
                    "Adding chunks to refresh queue during global Sequoia chunk refresh iteration (ChunksAddedToRefreshQueue: %v)",
                    chunks.size());

                NApi::TTransactionCommitOptions commitOptions{
                    .StronglyOrdered = false,
                };
                WaitFor(transaction->Commit(commitOptions))
                    .ThrowOnError();
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())));

        if (!addToRefreshQueueResult.IsOK()) {
            auto guard = Guard(Lock_);

            ++UnsuccessfulRefreshIterations_;
            auto logLevel = UnsuccessfulRefreshIterations_ > MaxUnsuccessfulRefreshIterations_ ?
                NLogging::ELogLevel::Alert :
                NLogging::ELogLevel::Debug;
            YT_LOG_EVENT(
                Logger,
                logLevel,
                addToRefreshQueueResult,
                "Failed to add chunks to refresh queue during global Sequoia chunk refresh (UnsuccessfulIterations: %v)",
                UnsuccessfulRefreshIterations_);
            return;
        }

        {
            auto guard = Guard(Lock_);
            if (currentEpoch != Status_.Epoch) {
                return;
            }

            Status_.LastProcessedChunkId = lastRefreshedChunkId;
            Status_.ChunksProcessed += chunksSize;
            UnsuccessfulRefreshIterations_ = 0;

            YT_LOG_DEBUG("Finished global Sequoia chunk refresh iteration (LastProcessedChunkId: %v, TotalChunksAddedToRefreshQueue: %v)",
                Status_.LastProcessedChunkId,
                Status_.ChunksProcessed);
        }


    }

    TErrorOr<std::vector<NRecords::TChunkReplicas>> FetchNextChunksBatch(TChunkId lastProcessedChunkId)
    {
        TSelectRowsQuery query = {
            .OrderBy = {
                "chunk_id_hash",
                "chunk_id"
            },
            .Limit = ChunksBatchSize_.load()
        };

        if (lastProcessedChunkId != NullObjectId) {
            query.WhereConjuncts = {
                Format("(chunk_id_hash, chunk_id) > (%v, %Qv)",
                    GetObjectIdFingerprint(lastProcessedChunkId),
                    lastProcessedChunkId)
            };
        }

        return WaitFor(Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->SelectRows<NRecords::TChunkReplicas>(query));
    }
};

////////////////////////////////////////////////////////////////////////////////

IGlobalSequoiaChunkRefresherPtr CreateGlobalSequoiaChunkRefresher(NCellMaster::TBootstrap* bootstrap)
{
    return New<TGlobalSequoiaChunkRefresher>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

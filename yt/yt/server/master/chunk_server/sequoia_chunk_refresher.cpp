#include "sequoia_chunk_refresher.h"

#include "chunk_manager.h"
#include "chunk_replica_fetcher.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/connection.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>
#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/composite_compare.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NObjectServer;

using namespace NChunkClient;
using namespace NIncumbentClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;

using namespace NYson;
using namespace NProfiling;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TSequoiaChunkRefresherShardStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("active", &TThis::Active);
    registrar.Parameter("epoch", &TThis::Epoch);

    registrar.Parameter("global_refresh_status", &TThis::GlobalRefreshStatus);
    registrar.Parameter("global_refresh_chunks_processed", &TThis::GlobalRefreshChunksProcessed);
    registrar.Parameter("global_refresh_last_processed_chunk_id", &TThis::GlobalRefreshLastProcessedChunkId);
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaChunkRefresherStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("shards", &TThis::Shards);

    registrar.Parameter("sequoia_chunk_refresh_enabled", &TThis::SequoiaChunkRefreshEnabled);
    registrar.Parameter("sequoia_chunk_refresh_epoch", &TThis::SequoiaChunkRefreshEpoch);

    registrar.Parameter("global_refresh_enabled", &TThis::GlobalRefreshEnabled);
    registrar.Parameter("global_refresh_epoch", &TThis::GlobalRefreshEpoch);
    registrar.Parameter("global_refresh_iterations", &TThis::GlobalRefreshIterations);
    registrar.Parameter("global_refresh_chunks_processed", &TThis::GlobalRefreshChunksProcessed);

    registrar.Parameter("location_refresh_enabled", &TThis::LocationRefreshEnabled);
    registrar.Parameter("awaiting_refresh_location_count", &TThis::AwaitingRefreshLocationCount);
}

////////////////////////////////////////////////////////////////////////////////

class TSequoiaChunkRefresher
    : public ISequoiaChunkRefresher
{
public:
    TSequoiaChunkRefresher(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void AdjustRefresherState() override
    {
        VerifyPersistentStateRead();

        const auto& chunkManagerConfig = GetDynamicConfig();
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();

        auto guard = Guard(Lock_);

        if (!hydraManager->IsActive() ||
            hydraManager->GetReadOnly())
        {
            UpdateShards(guard, /*deactivateAllShards*/ true);
            StopSequoiaChunkRefresh(guard);
            StopGlobalRefresh(guard);
            StopLocationRefresh(guard);
            return;
        }

        UpdateShards(guard, /*deactivateAllShards*/ false);

        if (!chunkManagerConfig->SequoiaChunkReplicas->Enable ||
            !chunkManagerConfig->SequoiaChunkReplicas->EnableSequoiaChunkRefresh)
        {
            StopSequoiaChunkRefresh(guard);
            StopGlobalRefresh(guard);
            StopLocationRefresh(guard);
            return;
        }

        if (!chunkManagerConfig->Testing->DisableSequoiaChunkRefresh) {
            // We do not check if replicator refresh is enabled, so we may not refresh some chunks.
            // When replicator refresh will be enabled, we will process all unrefreshed chunks with global refresh.
            UpdateAndStartSequoiaChunkRefresh(guard, chunkManagerConfig->SequoiaChunkReplicas);
        } else {
            StopSequoiaChunkRefresh(guard);
        }

        if (chunkManagerConfig->SequoiaChunkReplicas->EnableLocationRefresh) {
            UpdateAndStartLocationRefresh(guard, chunkManagerConfig->SequoiaChunkReplicas);
        } else {
            StopLocationRefresh(guard);
        }

        if (chunkManagerConfig->EnableChunkRefresh &&
            chunkManagerConfig->SequoiaChunkReplicas->EnableGlobalSequoiaChunkRefresh)
        {
            UpdateAndStartGlobalRefresh(guard, chunkManagerConfig->SequoiaChunkReplicas);
        } else {
            StopGlobalRefresh(guard);
        }
    }

    void RefreshNode(const TNode* node) override
    {
        VerifyPersistentStateRead();

        auto guard = Guard(Lock_);

        if (!Status_.LocationRefreshEnabled) {
            YT_LOG_DEBUG(
                "Sequoia refresh will not occur for node as Sequoia location refresh is disabled (NodeId: %v, NodeAddress: %v)",
                node->GetId(),
                node->GetDefaultAddress());
        }

        for (const auto& location : node->ChunkLocations()) {
            LocationsAwaitingRefresh_.push_back(TLocationAwaitingRefresh{
                .NodeId = node->GetId(),
                .LocationIndex = location->GetIndex(),
                .FailedAttempts = 0,
            });
            YT_LOG_DEBUG("Scheduling Sequoia location refresh (NodeId: %v, NodeAddress: %v, LocationIndex: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                location->GetIndex());
        }

        const auto& sequoiaReplicasConfig = GetDynamicConfig()->SequoiaChunkReplicas;
        if (std::ssize(LocationsAwaitingRefresh_) > sequoiaReplicasConfig->MaxLocationsAwaitingRefresh) {
            if (!Status_.GlobalRefreshEnabled) {
                YT_LOG_ALERT(
                    "Too many locations awaiting Sequoia location refresh, can not trigger global Sequoia chunk refresh "
                    "LocationsAwaitingRefresh: %v, MaxLocationsAwaitingRefresh: %v",
                    LocationsAwaitingRefresh_.size(),
                    sequoiaReplicasConfig->MaxLocationsAwaitingRefresh);
            } else {
                YT_LOG_WARNING(
                    "Too many locations awaiting Sequoia location refresh, will trigger global Sequoia chunk refresh "
                    "LocationsAwaitingRefresh: %v, MaxLocationsAwaitingRefresh: %v",
                    LocationsAwaitingRefresh_.size(),
                    sequoiaReplicasConfig->MaxLocationsAwaitingRefresh);
                StartNewGlobalRefreshEpoch(guard);
            }
        }
    }

    TSequoiaChunkRefresherStatus GetStatus() const override
    {
        auto guard = Guard(Lock_);

        auto status = Status_;

        status.AwaitingRefreshLocationCount = LocationsAwaitingRefresh_.size();

        status.GlobalRefreshChunksProcessed = 0;
        for (const auto& shard : status.Shards) {
            status.GlobalRefreshChunksProcessed += shard.GlobalRefreshChunksProcessed;
        }
        return status;
    }

    void OnProfiling(TSensorBuffer* buffer) const override
    {
        auto status = GetStatus();

        buffer->AddGauge("/sequoia_global_refresh_total_chunks_processed", status.GlobalRefreshChunksProcessed);

        int refreshedShards = 0;
        int activeShards = 0;
        for (int shardIndex = 0; shardIndex < std::ssize(status.Shards); ++shardIndex) {
            const auto& shard = status.Shards[shardIndex];
            if (shard.GlobalRefreshStatus != EGlobalSequoiaChunkRefreshShardStatus::Disabled) {
                TWithTagGuard tagGuard(buffer, "shard_index", std::to_string(shardIndex));
                buffer->AddGauge("/sequoia_global_refresh_chunks_processed", shard.GlobalRefreshChunksProcessed);

                ++activeShards;
                if (shard.GlobalRefreshStatus == EGlobalSequoiaChunkRefreshShardStatus::Completed) {
                    ++refreshedShards;
                }
            }
        }

        buffer->AddGauge("/sequoia_global_refresh_active_shards", activeShards);
        buffer->AddGauge("/sequoia_global_refresh_refreshed_shards", refreshedShards);
        buffer->AddGauge("/sequoia_global_refresh_unrefreshed_shards", activeShards - refreshedShards);

        buffer->AddGauge("/sequoia_location_refresh_awaiting_refresh_locations", status.AwaitingRefreshLocationCount);
    }

private:
    TBootstrap* Bootstrap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TSequoiaChunkRefresherStatus Status_;

    NConcurrency::TPeriodicExecutorPtr SequoiaChunkRefreshExecutor_;
    std::atomic<bool> IsSequoiaChunkRefreshRunning_ = false;
    int SequoiaChunkCountToFetchFromRefreshQueue_ = 0;
    int MaxUnsuccessfulSequoiaChunkRefreshIterations_ = 0;
    int UnsuccessfulSequoiaChunkRefreshIterations_ = 0;

    NConcurrency::TPeriodicExecutorPtr GlobalRefreshExecutor_;
    std::atomic<bool> IsGlobalRefreshRunning_ = false;
    int GlobalRefreshChunksBatchSize_ = 0;
    int MaxUnsuccessfulGlobalRefreshIterations_ = 0;
    int UnsuccessfulGlobalRefreshIterations_ = 0;

    NConcurrency::TPeriodicExecutorPtr LocationRefreshExecutor_;
    std::atomic<bool> IsLocationRefreshRunning_ = false;
    int MaxConcurrentLocationsToRefresh_ = 0;
    int MaxUnsuccessfulLocationRefreshAttempts_ = 0;

    struct TLocationAwaitingRefresh
    {
        TNodeId NodeId;
        TChunkLocationIndex LocationIndex;
        int FailedAttempts = 0;
    };
    std::deque<TLocationAwaitingRefresh> LocationsAwaitingRefresh_;

    TDynamicChunkManagerConfigPtr GetDynamicConfig() const
    {
        return Bootstrap_->GetDynamicConfig()->ChunkManager;
    }

    void UpdateShards(
        const TGuard<NThreading::TSpinLock>&,
        bool deactivateAllShards)
    {
        const auto& incumbentManager = Bootstrap_->GetIncumbentManager();

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            auto shouldBeActive = !deactivateAllShards && incumbentManager->HasIncumbency(EIncumbentType::ChunkReplicator, shardIndex);

            if (Status_.Shards[shardIndex].Active != shouldBeActive) {
                if (shouldBeActive) {
                    Status_.Shards[shardIndex].Active = true;
                    Status_.Shards[shardIndex].GlobalRefreshStatus = EGlobalSequoiaChunkRefreshShardStatus::Running;
                } else {
                    Status_.Shards[shardIndex].Active = false;
                    Status_.Shards[shardIndex].GlobalRefreshStatus = EGlobalSequoiaChunkRefreshShardStatus::Disabled;
                }
                ++Status_.Shards[shardIndex].Epoch;

                Status_.Shards[shardIndex].GlobalRefreshChunksProcessed = 0;
                Status_.Shards[shardIndex].GlobalRefreshLastProcessedChunkId = NullChunkId;

                YT_LOG_DEBUG("Sequoia chunk refresher shard updated (ShardIndex: %v, Active: %v, Epoch: %v)",
                    shardIndex,
                    Status_.Shards[shardIndex].Active,
                    Status_.Shards[shardIndex].Epoch);
            }
        }
    }

    void UpdateAndStartSequoiaChunkRefresh(
        const TGuard<NThreading::TSpinLock>&,
        TDynamicSequoiaChunkReplicasConfigPtr sequoiaReplicasConfig)
    {
        VerifyPersistentStateRead();

        SequoiaChunkCountToFetchFromRefreshQueue_ = sequoiaReplicasConfig->SequoiaChunkCountToFetchFromRefreshQueue;
        MaxUnsuccessfulSequoiaChunkRefreshIterations_ = sequoiaReplicasConfig->MaxUnsuccessfulSequoiaChunkRefreshIterations;

        if (!Status_.SequoiaChunkRefreshEnabled) {
            YT_LOG_DEBUG("Starting Sequoia chunk refresh");
            Status_.SequoiaChunkRefreshEnabled = true;
            ++Status_.SequoiaChunkRefreshEpoch;
            UnsuccessfulSequoiaChunkRefreshIterations_ = 0;
        }

        if (!SequoiaChunkRefreshExecutor_) {
            SequoiaChunkRefreshExecutor_ = New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                BIND(&TSequoiaChunkRefresher::RunSequoiaChunkRefreshIteration, MakeWeak(this)),
                sequoiaReplicasConfig->SequoiaChunkRefreshPeriod);
            SequoiaChunkRefreshExecutor_->Start();
        } else {
            SequoiaChunkRefreshExecutor_->SetPeriod(sequoiaReplicasConfig->SequoiaChunkRefreshPeriod);
        }
    }

    void StopSequoiaChunkRefresh(const TGuard<NThreading::TSpinLock>&)
    {
        if (Status_.SequoiaChunkRefreshEnabled) {
            YT_LOG_DEBUG("Stopping Sequoia chunk refresh");
            Status_.SequoiaChunkRefreshEnabled = false;
        }

        if (SequoiaChunkRefreshExecutor_) {
            YT_UNUSED_FUTURE(SequoiaChunkRefreshExecutor_->Stop());
            SequoiaChunkRefreshExecutor_.Reset();
        }
    }

    void UpdateAndStartGlobalRefresh(
        const TGuard<NThreading::TSpinLock>& guard,
        TDynamicSequoiaChunkReplicasConfigPtr sequoiaReplicasConfig)
    {
        VerifyPersistentStateRead();

        GlobalRefreshChunksBatchSize_ = sequoiaReplicasConfig->GlobalSequoiaChunkRefreshBatchSize;
        MaxUnsuccessfulGlobalRefreshIterations_ = sequoiaReplicasConfig->MaxUnsuccessfulGlobalSequoiaChunkRefreshIterations;

        if (!Status_.GlobalRefreshEnabled) {
            YT_LOG_DEBUG("Enabling global Sequoia chunk refresh");
            StartNewGlobalRefreshEpoch(guard);
        }

        if (!GlobalRefreshExecutor_) {
            GlobalRefreshExecutor_ = New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                BIND(&TSequoiaChunkRefresher::RunGlobalRefreshIteration, MakeWeak(this)),
                sequoiaReplicasConfig->GlobalSequoiaChunkRefreshPeriod);
            GlobalRefreshExecutor_->Start();
        } else {
            GlobalRefreshExecutor_->SetPeriod(sequoiaReplicasConfig->GlobalSequoiaChunkRefreshPeriod);
        }
    }

    void StartNewGlobalRefreshEpoch(const TGuard<NThreading::TSpinLock>&)
    {
        Status_.GlobalRefreshEnabled = true;
        ++Status_.GlobalRefreshEpoch;
        Status_.GlobalRefreshIterations = 0;

        for (auto& shard : Status_.Shards) {
            if (shard.Active) {
                shard.GlobalRefreshStatus = EGlobalSequoiaChunkRefreshShardStatus::Running;
            }
            shard.GlobalRefreshChunksProcessed = 0;
            shard.GlobalRefreshLastProcessedChunkId = NullChunkId;
        }

        // Global refresh will refresh all locations.
        LocationsAwaitingRefresh_.clear();

        UnsuccessfulGlobalRefreshIterations_ = 0;
    }

    void StopGlobalRefresh(const TGuard<NThreading::TSpinLock>&)
    {
        if (Status_.GlobalRefreshEnabled) {
            YT_LOG_DEBUG("Stopping global Sequoia chunk refresh");
            Status_.GlobalRefreshEnabled = false;

            for (auto& shard : Status_.Shards) {
                if (shard.Active) {
                    shard.GlobalRefreshStatus = EGlobalSequoiaChunkRefreshShardStatus::Stopped;
                }
                shard.GlobalRefreshChunksProcessed = 0;
                shard.GlobalRefreshLastProcessedChunkId = NullChunkId;
            }
        }

        if (GlobalRefreshExecutor_) {
            YT_UNUSED_FUTURE(GlobalRefreshExecutor_->Stop());
            GlobalRefreshExecutor_.Reset();
        }
    }

    void UpdateAndStartLocationRefresh(
        const TGuard<NThreading::TSpinLock>&,
        TDynamicSequoiaChunkReplicasConfigPtr sequoiaReplicasConfig)
    {
        MaxConcurrentLocationsToRefresh_ = sequoiaReplicasConfig->MaxConcurrentLocationsToRefresh;
        MaxUnsuccessfulLocationRefreshAttempts_ = sequoiaReplicasConfig->MaxUnsuccessfulLocationRefreshAttempts;

        if (!Status_.LocationRefreshEnabled) {
            YT_LOG_DEBUG("Starting Sequoia location refresh");
            Status_.LocationRefreshEnabled = true;
        }

        if (!LocationRefreshExecutor_) {
            LocationRefreshExecutor_ = New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                BIND(&TSequoiaChunkRefresher::RunLocationRefreshIteration, MakeWeak(this)),
                sequoiaReplicasConfig->LocationRefreshPeriod);
            LocationRefreshExecutor_->Start();
        } else {
            LocationRefreshExecutor_->SetPeriod(sequoiaReplicasConfig->LocationRefreshPeriod);
        }
    }

    void StopLocationRefresh(const TGuard<NThreading::TSpinLock>&)
    {
        if (Status_.LocationRefreshEnabled) {
            YT_LOG_DEBUG("Stopping Sequoia location refresh");
            Status_.LocationRefreshEnabled = false;
        }

        if (LocationRefreshExecutor_) {
            YT_UNUSED_FUTURE(LocationRefreshExecutor_->Stop());
            LocationRefreshExecutor_.Reset();
        }
    }

    bool ShouldAbortRefreshIterationCausedByShardsChange(
        const TGuard<NThreading::TSpinLock>&,
        const std::array<TSequoiaChunkRefresherShardStatus, ChunkShardCount>& shards,
        const std::string& executorName)
    {
        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            if (shards[shardIndex].Active != Status_.Shards[shardIndex].Active) {
                YT_LOG_DEBUG(
                    "Aborting iteration, shard active mismatch "
                    "(ExecutorName: %Qv, ShardIndex: %v, RefreshIterationShardActive: %v, ActualShardActive: %v)",
                    executorName,
                    shardIndex,
                    shards[shardIndex].Active,
                    Status_.Shards[shardIndex].Active);
                return true;
            }
            if (shards[shardIndex].Epoch != Status_.Shards[shardIndex].Epoch) {
                YT_LOG_DEBUG(
                    "Aborting iteration, shard epoch mismatch "
                    "(ExecutorName: %Qv,ShardIndex: %v, RefreshIterationEpoch: %v, ActualShardEpoch: %v)",
                    executorName,
                    shardIndex,
                    shards[shardIndex].Epoch,
                    Status_.Shards[shardIndex].Epoch);
                return true;
            }
        }
        return false;
    }

    void RunSequoiaChunkRefreshIteration()
    {
        if (IsSequoiaChunkRefreshRunning_.exchange(true)) {
            return;
        }
        auto finallyGuard = Finally([&] {
            IsSequoiaChunkRefreshRunning_.store(false);
        });

        i64 epoch;
        std::array<TSequoiaChunkRefresherShardStatus, ChunkShardCount> shards;
        i64 limit;
        {
            auto guard = Guard(Lock_);
            if (!Status_.SequoiaChunkRefreshEnabled) {
                return;
            }

            epoch = Status_.SequoiaChunkRefreshEpoch;
            shards = Status_.Shards;
            limit = SequoiaChunkCountToFetchFromRefreshQueue_;
        }

        YT_LOG_DEBUG("Sequoia chunk refresh iteration started");

        std::vector<TFuture<std::vector<NRecords::TChunkRefreshQueue>>> getChunksFutures;
        std::vector<int> indices;

        const auto& chunkReplicaFetcher = Bootstrap_->GetChunkManager()->GetChunkReplicaFetcher();

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            if (shards[shardIndex].Active) {
                getChunksFutures.push_back(chunkReplicaFetcher->GetChunksToRefresh(shardIndex, limit));
                indices.push_back(shardIndex);
            }
        }

        auto shouldAbortRefreshIteration = [this, this_ = MakeStrong(this)] (
            const TGuard<NThreading::TSpinLock>& guard,
            int epoch,
            const auto& shards)
        {
            if (!Status_.SequoiaChunkRefreshEnabled) {
                YT_LOG_DEBUG("Aborting Sequoia chunk refresh iteration, Sequoia chunk refresh is disabled");
                return true;
            }

            if (Status_.SequoiaChunkRefreshEpoch != epoch) {
                YT_LOG_DEBUG(
                    "Aborting Sequoia chunk refresh iteration, epoch mismatch "
                    "(RefreshIterationEpoch: %v, ActualEpoch: %v)",
                    epoch,
                    Status_.SequoiaChunkRefreshEpoch);
                return true;
            }

            return ShouldAbortRefreshIterationCausedByShardsChange(guard, shards, "Sequoia chunk refresh");
        };

        auto allSetResult = WaitFor(AllSet(getChunksFutures));

        try {
            if (!allSetResult.IsOK()) {
                THROW_ERROR_EXCEPTION("Error getting chunks to refresh") << std::move(allSetResult);
            }

            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            // We should have state not older than the state at which chunks were added to refresh queue.
            auto leaderSyncResult = WaitFor(hydraManager->SyncWithLeader());
            if (!leaderSyncResult.IsOK()) {
                THROW_ERROR_EXCEPTION("Error syncing with leader") << std::move(leaderSyncResult);
            }

            const auto& results = allSetResult.Value();
            std::vector<TChunkId> chunkIdsToRefresh;
            THashMap<int, i64> indexToTrimmedRowCount;
            for (const auto& [result, shardIndex] : Zip(results, indices)) {
                if (!result.IsOK()) {
                    THROW_ERROR_EXCEPTION(
                        "Error getting chunks to refresh from shard %v",
                        shardIndex)
                        << std::move(result);
                }

                const auto& refreshRecords = result.Value();
                YT_LOG_DEBUG("Fetched chunks for Sequoia chunk refresh (ShardIndex: %v, ChunkCount: %v)",
                    shardIndex,
                    refreshRecords.size());
                for (const auto& refreshRecord : refreshRecords) {
                    chunkIdsToRefresh.push_back(refreshRecord.ChunkId);
                }

                if (!refreshRecords.empty()) {
                    indexToTrimmedRowCount[shardIndex] = refreshRecords.back().RowIndex + 1;
                }
            }
            SortUnique(chunkIdsToRefresh);

            YT_LOG_DEBUG("Start refreshing chunks during Sequoia chunk refresh (ChunkCount: %v)",
                chunkIdsToRefresh.size());

            auto refreshChunks = BIND([
                chunkIdsToRefresh = std::move(chunkIdsToRefresh),
                shouldAbortRefreshIteration = shouldAbortRefreshIteration,
                epoch = epoch,
                shards = shards,
                this,
                this_ = MakeStrong(this)]
            {
                if (!Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActive()) {
                    THROW_ERROR_EXCEPTION("Hydra is not active");
                }

                {
                    auto guard = Guard(Lock_);
                    if (shouldAbortRefreshIteration(guard, epoch, shards)) {
                        THROW_ERROR_EXCEPTION("Aborting Sequoia chunk refresh iteration");
                    }
                }

                NProto::TReqTopUpSequoiaChunkPurgatory topUpSequoiaChunkPurgatoryRequest;

                const auto& chunkManager = Bootstrap_->GetChunkManager();

                for (auto chunkId : chunkIdsToRefresh) {
                    auto* chunk = chunkManager->FindChunk(chunkId);
                    if (IsObjectAlive(chunk)) {
                        chunkManager->ScheduleChunkRefresh(chunk);
                    } else {
                        ToProto(topUpSequoiaChunkPurgatoryRequest.add_chunk_ids(), chunkId);
                    }
                }

                if (topUpSequoiaChunkPurgatoryRequest.chunk_ids_size() > 0) {
                    auto mutation = chunkManager->CreateTopUpSequoiaChunkPurgatoryMutation(topUpSequoiaChunkPurgatoryRequest);
                    mutation->SetAllowLeaderForwarding(true);
                    WaitFor(mutation->CommitAndLog(Logger()))
                        .ThrowOnError();
                }
            });
            WaitFor(refreshChunks
                .AsyncVia(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ChunkManager))
                .Run())
                .ThrowOnError();

            // Now all chunks are added to refresh queue.
            // Chunk shards were active at the time chunks were added to refresh queue.
            // Chunks were fetched before refresh, so it is safe to trim refresh tabled, even if incumbents are changed.
            YT_LOG_DEBUG("Trimming refresh queues during Sequoia chunk refresh");

            TSequoiaTablePathDescriptor descriptor{
                .Table = ESequoiaTable::ChunkRefreshQueue,
                .MasterCellTag = Bootstrap_->GetCellTag(),
            };
            std::vector<TFuture<void>> trimFutures;
            trimFutures.reserve(indexToTrimmedRowCount.size());
            for (auto [index, trimmedRowCount] : indexToTrimmedRowCount) {
                YT_LOG_DEBUG("Trimming table during Sequoia chunk refresh (ShardIndex: %v, TrimmedRowCount: %v)",
                    index,
                    trimmedRowCount);
                // Index is both replicator shard index and tablet index.
                trimFutures.push_back(Bootstrap_
                    ->GetSequoiaConnection()
                    ->CreateClient(NRpc::GetRootAuthenticationIdentity())
                    ->TrimTable(descriptor, index, trimmedRowCount));
            }
            auto trimResult = WaitFor(AllSucceeded(trimFutures));
            if (!trimResult.IsOK()) {
                THROW_ERROR_EXCEPTION("Error trimming Sequoia refresh queue") << std::move(trimResult);
            }

            UnsuccessfulSequoiaChunkRefreshIterations_ = 0;
            YT_LOG_DEBUG("Finished Sequoia chunk refresh iteration");
        } catch (const std::exception& ex) {
            auto guard = Guard(Lock_);
            if (shouldAbortRefreshIteration(guard, epoch, shards)) {
                return;
            }

            ++UnsuccessfulSequoiaChunkRefreshIterations_;
            auto logLevel = UnsuccessfulSequoiaChunkRefreshIterations_ > MaxUnsuccessfulSequoiaChunkRefreshIterations_ ?
                NLogging::ELogLevel::Alert :
                NLogging::ELogLevel::Debug;
            YT_LOG_EVENT(
                Logger,
                logLevel,
                ex,
                "Sequoia chunk refresh iteration failed (PreviousUnsuccessfulIterations: %v)",
                UnsuccessfulSequoiaChunkRefreshIterations_);
        }
    }

    void RunGlobalRefreshIteration()
    {
        if (IsGlobalRefreshRunning_.exchange(true)) {
            return;
        }
        auto finallyGuard = Finally([&] {
            IsGlobalRefreshRunning_.store(false);
        });

        i64 epoch;
        std::array<TSequoiaChunkRefresherShardStatus, ChunkShardCount> shards;
        int batchSize;
        {
            auto guard = Guard(Lock_);
            if (!Status_.GlobalRefreshEnabled) {
                return;
            }

            ++Status_.GlobalRefreshIterations;
            epoch = Status_.GlobalRefreshEpoch;
            shards = Status_.Shards;
            batchSize = GlobalRefreshChunksBatchSize_;
        }

        YT_LOG_DEBUG("Starting global Sequoia chunk refresh iteration");

        std::vector<int> shardIndexes;
        std::vector<TFuture<std::vector<NRecords::TChunkReplicas>>> chunksFutures;

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            if (shards[shardIndex].Active && shards[shardIndex].GlobalRefreshStatus == EGlobalSequoiaChunkRefreshShardStatus::Running) {
                shardIndexes.push_back(shardIndex);
            }
        }

        if (shardIndexes.empty()) {
            YT_LOG_DEBUG("All shards are refreshed, skipping global Sequoia chunk refresh iteration");
            return;
        }

        if (batchSize == 0) {
            YT_LOG_DEBUG("Global sequoia chunk refresh has zero chunk batch size, skipping global Sequoia chunk refresh iteration");
            return;
        }

        batchSize = std::max(1, batchSize / static_cast<int>(shardIndexes.size()));

        for (auto shardIndex : shardIndexes) {
            chunksFutures.push_back(FetchNextChunksBatch(
                shardIndex,
                shards[shardIndex].GlobalRefreshLastProcessedChunkId,
                batchSize));
        }

        auto shouldAbortRefreshIteration = [&] (const TGuard<NThreading::TSpinLock>& guard) {
            if (!Status_.GlobalRefreshEnabled) {
                YT_LOG_DEBUG("Aborting global Sequoia chunk refresh iteration, global Sequoia chunk refresh is disabled");
                return true;
            }

            if (epoch != Status_.GlobalRefreshEpoch) {
                YT_LOG_DEBUG(
                    "Aborting global Sequoia chunk refresh iteration, epoch mismatch "
                    "(RefreshIterationEpoch: %v, ActualEpoch: %v)",
                    epoch,
                    Status_.GlobalRefreshEpoch);
                return true;
            }

            return ShouldAbortRefreshIterationCausedByShardsChange(guard, shards, "global Sequoia chunk refresh");
        };

        auto allSetResult = WaitFor(AllSet(chunksFutures));

        try {
            if (!allSetResult.IsOK()) {
                THROW_ERROR_EXCEPTION("Error getting chunks")
                    << std::move(allSetResult);
            }

            THashMap<int, std::vector<NRecords::TChunkReplicas>> chunks;
            THashMap<int, i64> fetchedChunkCount;
            THashMap<int, TChunkId> lastProcessedChunkId;

            for (auto&& [shardIndex, chunksOrError] : Zip(shardIndexes, std::move(allSetResult).Value())) {
                if (!chunksOrError.IsOK()) {
                    // TODO(grphil): Maybe continue refresh for shards that are fetched.
                    THROW_ERROR_EXCEPTION("Error getting chunks for shard %v", shardIndex)
                        << std::move(chunksOrError);
                }
                chunks[shardIndex] = std::move(chunksOrError).Value();
                fetchedChunkCount[shardIndex] = chunks[shardIndex].size();
                if (fetchedChunkCount[shardIndex] > 0) {
                    lastProcessedChunkId[shardIndex] = chunks[shardIndex].back().Key.ChunkId;
                }

                YT_LOG_DEBUG("Fetched chunks for global Sequoia chunk refresh (ShardIndex: %v, FetchedChunkCount: %v)",
                    shardIndex,
                    fetchedChunkCount[shardIndex]);
            }

            YT_LOG_DEBUG("Adding chunks to refresh queue during global Sequoia chunk refresh");

            auto addToRefreshQueueResult = WaitFor(Bootstrap_
                ->GetSequoiaConnection()
                ->CreateClient(NRpc::GetRootAuthenticationIdentity())
                ->StartTransaction(
                    ESequoiaTransactionType::GlobalRefresh,
                    {.CellTag = Bootstrap_->GetCellTag()})
                .Apply(BIND([
                    chunks = std::move(chunks),
                    cellTag = Bootstrap_->GetCellTag()
                ] (const ISequoiaTransactionPtr& transaction) {
                    auto now = TInstant::Now();
                    for (const auto& [shardIndex, shardChunks] : chunks) {
                        for (const auto& chunk : shardChunks) {
                            transaction->WriteRow(cellTag, NRecords::TChunkRefreshQueue{
                                .TabletIndex = shardIndex,
                                .ChunkId = chunk.Key.ChunkId,
                                .ConfirmationTime = now,
                            });
                        };
                    }

                    NApi::TTransactionCommitOptions commitOptions{
                        .StronglyOrdered = false,
                    };
                    WaitFor(transaction->Commit(commitOptions))
                        .ThrowOnError();
                })
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())));

            if (!addToRefreshQueueResult.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to add chunks to refresh queue")
                    << std::move(addToRefreshQueueResult);
            }

            YT_LOG_DEBUG("Added chunks to refresh queue during global Sequoia chunk refresh");

            auto guard = Guard(Lock_);
            if (shouldAbortRefreshIteration(guard)) {
                return;
            }

            i64 totalRefreshedChunkCount = 0;
            for (auto shardIndex : shardIndexes) {
                Status_.Shards[shardIndex].GlobalRefreshChunksProcessed += fetchedChunkCount[shardIndex];
                totalRefreshedChunkCount += fetchedChunkCount[shardIndex];
                if (fetchedChunkCount[shardIndex] > 0) {
                    Status_.Shards[shardIndex].GlobalRefreshLastProcessedChunkId = lastProcessedChunkId[shardIndex];
                    YT_LOG_DEBUG(
                        "Finished global Sequoia chunk refresh iteration for shard "
                        "(ShardIndex: %v, ProcessedChunkCount: %v, LastProcessedChunkId: %v)",
                        shardIndex,
                        fetchedChunkCount[shardIndex],
                        lastProcessedChunkId[shardIndex]);
                } else {
                    Status_.Shards[shardIndex].GlobalRefreshStatus = EGlobalSequoiaChunkRefreshShardStatus::Completed;
                    YT_LOG_DEBUG("Global Sequoia chunk refresh for shard is complete (ShardIndex: %v)",
                        shardIndex);
                }
            }

            UnsuccessfulGlobalRefreshIterations_ = 0;
            YT_LOG_DEBUG("Finished global Sequoia chunk refresh iteration (TotalRefreshedChunkCount: %v)",
                totalRefreshedChunkCount);
        } catch (const std::exception& ex) {
            auto guard = Guard(Lock_);

            if (shouldAbortRefreshIteration(guard)) {
                return;
            }

            ++UnsuccessfulGlobalRefreshIterations_;
            auto logLevel = UnsuccessfulGlobalRefreshIterations_ > MaxUnsuccessfulGlobalRefreshIterations_ ?
                NLogging::ELogLevel::Alert :
                NLogging::ELogLevel::Debug;
            YT_LOG_EVENT(
                Logger,
                logLevel,
                ex,
                "Global Sequoia chunk refresh iteration failed (PreviousUnsuccessfulIterations: %v)",
                UnsuccessfulGlobalRefreshIterations_);
        }
    }

    TFuture<std::vector<NRecords::TChunkReplicas>> FetchNextChunksBatch(
        int shardIndex,
        TChunkId lastProcessedChunkId,
        int batchSize)
    {
        TSelectRowsQuery query = {
            .WhereConjuncts = {
                Format("cell_tag = %v", Bootstrap_->GetCellTag()),
                Format("shard_index = %v", shardIndex),
            },
            .OrderBy = {
                "cell_tag",
                "shard_index",
                "chunk_id_hash",
                "chunk_id"
            },
            .Limit = batchSize,
        };

        if (lastProcessedChunkId != NullObjectId) {
            query.WhereConjuncts.push_back(
                Format("(chunk_id_hash, chunk_id) > (%v, %Qv)",
                    GetObjectIdFingerprint(lastProcessedChunkId),
                    lastProcessedChunkId));
        }

        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->SelectRows<NRecords::TChunkReplicas>(query);
    }

    void RunLocationRefreshIteration()
    {
        if (IsLocationRefreshRunning_.exchange(true)) {
            return;
        }
        auto finallyGuard = Finally([&] {
            IsLocationRefreshRunning_.store(false);
        });

        std::vector<TLocationAwaitingRefresh> locations;

        {
            auto guard = Guard(Lock_);

            if (!Status_.LocationRefreshEnabled) {
                return;
            }

            while (!LocationsAwaitingRefresh_.empty() && std::ssize(locations) < MaxConcurrentLocationsToRefresh_) {
                locations.push_back(std::move(LocationsAwaitingRefresh_.front()));
                LocationsAwaitingRefresh_.pop_front();
            }

            if (locations.empty()) {
                return;
            }

            YT_LOG_DEBUG(
                "Starting Sequoia location refresh iteration (LocationsToRefreshCount: %v, LocationsAwaitingRefreshCount: %v)",
                locations.size(),
                LocationsAwaitingRefresh_.size());
        }

        std::vector<TFuture<std::vector<NRecords::TLocationReplicas>>> locationReplicasFutures;
        const auto& chunkReplicaFetcher = Bootstrap_->GetChunkManager()->GetChunkReplicaFetcher();

        for (const auto& location : locations) {
            locationReplicasFutures.push_back(
                chunkReplicaFetcher->GetSequoiaLocationReplicasWithoutSequoiaChecks(location.NodeId, location.LocationIndex));
        }

        auto rescheduleLocationsRefresh = [&](
            const TGuard<NThreading::TSpinLock>& guard,
            bool increaseFailedAttempts) {
            for (auto& location : locations) {
                if (increaseFailedAttempts) {
                    ++location.FailedAttempts;

                    if (location.FailedAttempts > MaxUnsuccessfulLocationRefreshAttempts_) {
                        if (Status_.GlobalRefreshEnabled) {
                            StartNewGlobalRefreshEpoch(guard);
                            YT_LOG_WARNING("Sequoia location refresh failed for location, will trigger new global Sequoia chunk refresh "
                                "(NodeId: %v, LocationIndex: %v, FailedAttempts: %v, MaxFailedAttemptsAllowed: %v)",
                                location.NodeId,
                                location.LocationIndex,
                                location.FailedAttempts,
                                MaxUnsuccessfulLocationRefreshAttempts_);
                            return;
                        }
                        YT_LOG_ALERT("Sequoia location refresh failed for location, can not trigger new global Sequoia chunk refresh "
                            "(NodeId: %v, LocationIndex: %v, FailedAttempts: %v, MaxFailedAttemptsAllowed: %v)",
                            location.NodeId,
                            location.LocationIndex,
                            location.FailedAttempts,
                            MaxUnsuccessfulLocationRefreshAttempts_);
                    }
                }

                YT_LOG_DEBUG("Rescheduling Sequoia location refresh for location (NodeId: %v, LocationIndex: %v, FailedAttempts: %v)",
                    location.NodeId,
                    location.LocationIndex,
                    location.FailedAttempts);

                LocationsAwaitingRefresh_.push_front(std::move(location));
            }
        };

        auto replicasOrError = WaitFor(AllSet(std::move(locationReplicasFutures)));

        std::vector<std::vector<NRecords::TLocationReplicas>> replicas;
        std::array<bool, ChunkShardCount> isShardActive;
        {
            auto guard = Guard(Lock_);

            if (!Status_.LocationRefreshEnabled) {
                rescheduleLocationsRefresh(guard, /*increaseFailedAttempts*/ false);
                return;
            }

            if (!replicasOrError.IsOK()) {
                YT_LOG_DEBUG(
                    replicasOrError,
                    "Failed to fetch replicas for Sequoia location refresh, will reschedule locations refresh");
                rescheduleLocationsRefresh(guard, /*increaseFailedAttempts*/ true);
                return;
            }

            for (auto&& locationReplicasOrError : std::move(replicasOrError).Value()) {
                if (!locationReplicasOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        locationReplicasOrError,
                        "Failed to fetch location replicas for Sequoia location refresh, will reschedule locations refresh");

                    // TODO(grphil): Maybe we should reschedule refresh only for filed locations and execute refresh for the rest?
                    rescheduleLocationsRefresh(guard, /*increaseFailedAttempts*/ true);
                    return;
                }
                replicas.push_back(std::move(locationReplicasOrError).Value());
            }

            for (int i = 0; i < ChunkShardCount; ++i) {
                isShardActive[i] = Status_.Shards[i].GlobalRefreshStatus != EGlobalSequoiaChunkRefreshShardStatus::Disabled;
            }
        }

        YT_LOG_DEBUG("Fetched replicas for Sequoia location refresh");

        auto addToRefreshQueueResult = WaitFor(Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->StartTransaction(
                ESequoiaTransactionType::LocationRefresh,
                {.CellTag = Bootstrap_->GetCellTag()})
            .Apply(BIND([
                replicas = std::move(replicas),
                cellTag = Bootstrap_->GetCellTag(),
                isShardActive = std::move(isShardActive)
            ] (const ISequoiaTransactionPtr& transaction) {
                auto now = TInstant::Now();
                int chunksAddedToRefreshQueue = 0;

                for (const auto& locationReplicas : replicas) {
                    for (const auto& replica : locationReplicas) {
                        int shardIndex = GetChunkShardIndex(replica.Key.ChunkId);
                        if (!isShardActive[shardIndex]) {
                            continue;
                        }
                        transaction->WriteRow(cellTag, NRecords::TChunkRefreshQueue{
                            .TabletIndex = shardIndex,
                            .ChunkId = replica.Key.ChunkId,
                            .ConfirmationTime = now,
                        });
                        ++chunksAddedToRefreshQueue;
                    }
                };

                YT_LOG_DEBUG(
                    "Adding chunks to refresh queue during Sequoia location refresh iteration (ChunksAddedToRefreshQueue: %v)",
                    chunksAddedToRefreshQueue);

                NApi::TTransactionCommitOptions commitOptions{
                    .StronglyOrdered = false,
                };
                WaitFor(transaction->Commit(commitOptions))
                    .ThrowOnError();
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())));

        {
            auto guard = Guard(Lock_);
            if (!Status_.LocationRefreshEnabled) {
                rescheduleLocationsRefresh(guard, /*increaseFailedAttempts*/ false);
                return;
            }

            if (!addToRefreshQueueResult.IsOK()) {
                YT_LOG_DEBUG(
                    addToRefreshQueueResult,
                    "Failed to add chunks to refresh queue during Sequoia location refresh iteration, will reschedule locations refresh");
                rescheduleLocationsRefresh(guard, /*increaseFailedAttempts*/ true);
            }

            for (const auto& location : LocationsAwaitingRefresh_) {
                YT_LOG_DEBUG(
                    "Finished location refresh during Sequoia location refresh iteration (NodeId: %v, LocationIndex: %v)",
                    location.NodeId,
                    location.LocationIndex);
            }

            YT_LOG_DEBUG("Finished Sequoia location refresh iteration iteration (RefreshedLocationCount: %v)",
                locations.size());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaChunkRefresherPtr CreateSequoiaChunkRefresher(NCellMaster::TBootstrap* bootstrap)
{
    return New<TSequoiaChunkRefresher>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#include "sequoia_replicas_modifier.h"

#include "chunk_manager.h"
#include "chunk_replica.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/ytlib/sequoia_client/connection.h>
#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>

#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/unapproved_chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;

using namespace NSequoiaClient;

using namespace NDataNodeTrackerClient::NProto;
using namespace NChunkClient::NProto;

using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;

using NYT::FromProto;

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaReplicasModifier
    : public ISequoiaReplicasModifier
{
public:
    TSequoiaReplicasModifier(
        TSequoiaReplicaModificationProfile& profile,
        ESequoiaTransactionType transactionType,
        TBootstrap* bootstrap,
        const TDynamicChunkManagerConfigPtr& config)
        : TransactionType_(transactionType)
        , Config_(CopySequoiaChunkReplicasConfig(config->SequoiaChunkReplicas))
        , Bootstrap_(bootstrap)
        , Profile_(profile)
    { }

    void AddRequest(
        std::unique_ptr<TReqModifyReplicas> request) override
    {
        if (ReplaceLocationRequest_) {
            YT_TLOG_ALERT_AND_THROW("Sequoia replicas modifier can not have both replace and modify requests");
        }
        IsIncrementalHeartbeat_ &= request->is_incremental_heartbeat();
        ReplicaCount_ += request->added_chunks_size() + request->removed_chunks_size();
        Requests_.push_back(std::move(request));
    }

    void AddRequest(
        std::unique_ptr<TReqReplaceLocationReplicas> request) override
    {
        if (!Requests_.empty()) {
            YT_TLOG_ALERT_AND_THROW("Replace location request must be unique in sequoia replicas modifier");
        }
        ReplicaCount_ += request->chunks_size();
        ReplaceLocationRequest_ = std::move(request);
        IsIncrementalHeartbeat_ = false;
        IsValidationHeartbeat_ = true;
    }

    TFuture<void> ModifyReplicas() override
    {
        auto result = BIND(&TSequoiaReplicasModifier::DoModifyReplicas, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkManager))
            .Run();

        if (Config_->EnableInGhostMode) {
            YT_VERIFY(!Config_->Enable);
            return MakeFuture<void>(TError());
        } else {
            return result;
        }
    }

private:
    const ESequoiaTransactionType TransactionType_;
    const TDynamicSequoiaChunkReplicasConfigPtr Config_;

    TBootstrap* const Bootstrap_;
    TSequoiaReplicaModificationProfile& Profile_;

    ISequoiaTransactionPtr Transaction_;

    NProfiling::TWallTimer Timer_;

    std::vector<std::unique_ptr<TReqModifyReplicas>> Requests_;
    std::unique_ptr<TReqReplaceLocationReplicas> ReplaceLocationRequest_;

    int ReplicaCount_ = 0;

    struct TReplicaList
    {
        std::vector<TChunkReplicaWithLocationIndexAndState> AddedReplicas;
        std::vector<TChunkReplicaWithLocationIndexAndState> RemovedReplicas;
    };

    THashSet<TChunkId> ChunksWithMediumChange_;
    THashMap<TChunkId, TReplicaList> ModifiedReplicas_;

    bool IsIncrementalHeartbeat_ = true;
    bool IsValidationHeartbeat_ = false;

    static constexpr size_t ChunkSampleSizeOnValidationFail = 10;

    void ProfileTime(ESequoiaReplicaModificationPhase phase)
    {
        Profile_.PhaseTime[phase].Record(
            Timer_.GetElapsedTime().SecondsFloat());
        Timer_.Restart();
    }

    void DoModifyReplicas()
    {
        VerifyPersistentStateRead();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto guard = chunkManager->AcquireModifySequoiaReplicasSemaphoreGuard(TransactionType_, ReplicaCount_);

        ProfileTime(ESequoiaReplicaModificationPhase::SemaphoreWait);

        if (Config_->SleepDurationBeforeSequoiaReplicaModifications) {
            TDelayedExecutor::WaitForDuration(*Config_->SleepDurationBeforeSequoiaReplicaModifications);
        }

        Timer_.Restart();
        Profile_.StartedCount.Increment(1);
        Profile_.StartedReplicaCount.Increment(ReplicaCount_);

        auto result = WaitFor(Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->StartTransaction(
                TransactionType_,
                {.CellTag = Bootstrap_->GetCellTag()})
            .Apply(BIND(&TSequoiaReplicasModifier::ExecuteModifyReplicas, MakeStrong(this))
                .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())));

        if (result.IsOK()) {
            Profile_.FinishedSuccessfullyCount.Increment(1);
            Profile_.FinishedSuccessfullyReplicaCount.Increment(ReplicaCount_);
        } else {
            Profile_.FinishedWithErrorCount.Increment(1);
            Profile_.FinishedWithErrorReplicaCount.Increment(ReplicaCount_);
            YT_LOG_TRACE(
                result,
                "Sequoia replica modification finished with error (TransactionType: %v, ReplicaCount: %v)",
                TransactionType_,
                ReplicaCount_);
            result.ThrowOnError();
        }
    }

    void ExecuteModifyReplicas(const ISequoiaTransactionPtr& transaction)
    {
        Start(transaction);

        if (ReplaceLocationRequest_) {
            ProcessReplaceLocationRequest();
        } else {
            ProcessModifyReplicasRequests();
        }
    }

    void ProcessModifyReplicasRequests()
    {
        if (Requests_.empty()) {
            YT_TLOG_ALERT_AND_THROW("No requests for sequoia replicas modifier");
        }
        GatherModifiedAddedChunkReplicas();
        GatherModifiedRemovedReplicas();
        WriteRowsAndAddTransactionActions();
        Finish();
    }

    void ProcessReplaceLocationRequest()
    {
        auto modifyRequest = std::make_unique<TReqModifyReplicas>();
        modifyRequest->set_node_id(ReplaceLocationRequest_->node_id());
        modifyRequest->set_caused_by_node_disposal(ReplaceLocationRequest_->caused_by_node_disposal());
        modifyRequest->set_caused_by_validation(ReplaceLocationRequest_->is_validation());
        Requests_.push_back(std::move(modifyRequest));

        GatherReplacedLocationReplicasDifference();
        if (CheckIfRequestShouldBeAborted()) {
            return;
        }
        WriteRowsAndAddTransactionActions();
        Finish();
    }

    void Start(const ISequoiaTransactionPtr& transaction)
    {
        Transaction_ = transaction;

        ProfileTime(ESequoiaReplicaModificationPhase::StartTransaction);
    }

    template <typename TChunkInfo>
    void GatherModifiedChunkReplica(TNodeId nodeId, const TChunkInfo& chunkInfo)
    {
        constexpr bool chunkAdded = std::is_same_v<TChunkInfo, TChunkAddInfo>;

        auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
        auto chunkId = chunkIdWithIndex.Id;
        auto locationIndex = FromProto<TChunkLocationIndex>(chunkInfo.location_index());

        if (chunkInfo.caused_by_medium_change()) {
            ChunksWithMediumChange_.insert(chunkId);
            // We need 2 different messages to be able to override some of them to debug.
            if constexpr (chunkAdded) {
                YT_TLOG_TRACE("Addition of Sequoia replica is caused by medium change")
                    .With("ChunkId", chunkId)
                    .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                    .With("LocationIndex", locationIndex);
            } else {
                YT_TLOG_TRACE("Removal of Sequoia replica is caused by medium change")
                    .With("ChunkId", chunkId)
                    .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                    .With("LocationIndex", locationIndex);
            }

            return;
        }

        auto replica = TChunkReplicaWithLocationIndexAndState(
            nodeId,
            chunkIdWithIndex.ReplicaIndex,
            locationIndex);

        if constexpr (chunkAdded) {
            replica.ReplicaState = GetAddedChunkReplicaState(chunkId, chunkInfo);
            ModifiedReplicas_[chunkId].AddedReplicas.push_back(replica);
        } else {
            ModifiedReplicas_[chunkId].RemovedReplicas.push_back(replica);
        }

        // We need 4 different messages to be able to override some of them to debug.
        if (IsIncrementalHeartbeat_) {
            if constexpr (chunkAdded) {
                YT_TLOG_TRACE("Sequoia replica is being added during incremental heartbeat")
                    .With("ChunkId", chunkId)
                    .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                    .With("LocationIndex", locationIndex)
                    .With("NodeId", nodeId);
            } else {
                YT_TLOG_TRACE("Sequoia replica is being removed during incremental heartbeat")
                    .With("ChunkId", chunkId)
                    .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                    .With("LocationIndex", locationIndex)
                    .With("NodeId", nodeId);
            }
        } else {
            if constexpr (chunkAdded) {
                YT_TLOG_TRACE("Sequoia replica is being added during full heartbeat")
                    .With("ChunkId", chunkId)
                    .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                    .With("LocationIndex", locationIndex)
                    .With("NodeId", nodeId)
                    .With("IsValidationHeartbeat", IsValidationHeartbeat_);
            } else {
                YT_TLOG_TRACE("Sequoia replica is being removed during full heartbeat")
                    .With("ChunkId", chunkId)
                    .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                    .With("LocationIndex", locationIndex)
                    .With("NodeId", nodeId)
                    .With("IsValidationHeartbeat", IsValidationHeartbeat_);
            }
        }
    }

    void GatherModifiedAddedChunkReplicas()
    {
        for (const auto& request : Requests_) {
            auto nodeId = FromProto<TNodeId>(request->node_id());
            for (const auto& chunkInfo : request->added_chunks()) {
                GatherModifiedChunkReplica(nodeId, chunkInfo);
            }
        }

        ProfileTime(ESequoiaReplicaModificationPhase::GatherModifiedAddedReplicas);
    }

    std::vector<NRecords::TLocationReplicasKey> CollectRemovedReplicasKeys()
    {
        std::vector<NRecords::TLocationReplicasKey> removedReplicasKeys;
        for (const auto& request : Requests_) {
            auto nodeId = FromProto<TNodeId>(request->node_id());

            for (const auto& chunkInfo : request->removed_chunks()) {
                auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
                auto chunkId = chunkIdWithIndex.Id;

                auto locationIndex = FromProto<TChunkLocationIndex>(chunkInfo.location_index());

                if (chunkInfo.caused_by_medium_change()) {
                    // Chunks with medium change will be processed later.
                    continue;
                }

                NRecords::TLocationReplicasKey locationReplicaKey{
                    .CellTag = Bootstrap_->GetCellTag(),
                    .NodeId = nodeId,
                    .LocationIndex = locationIndex,
                    .ChunkId = chunkId,
                    .ReplicaIndex = static_cast<i8>(chunkIdWithIndex.ReplicaIndex)
                };
                removedReplicasKeys.push_back(locationReplicaKey);
                // We need 2 messages to be able to override some of them to debug.
                if (IsIncrementalHeartbeat_) {
                    YT_TLOG_TRACE("Removed Sequoia replica is being added during incremental heartbeat")
                        .With("ChunkId", chunkId)
                        .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                        .With("LocationIndex", locationIndex)
                        .With("NodeId", nodeId);
                } else {
                    YT_TLOG_TRACE("Removed Sequoia replica is being added during full heartbeat")
                        .With("ChunkId", chunkId)
                        .With("ReplicaIndex", chunkIdWithIndex.ReplicaIndex)
                        .With("LocationIndex", locationIndex)
                        .With("NodeId", nodeId);
                }
            }
        }

        ProfileTime(ESequoiaReplicaModificationPhase::ParseRemovedReplicas);

        return removedReplicasKeys;
    }

    std::vector<std::optional<NRecords::TLocationReplicas>> LookupRemovedReplicas()
    {
        auto replicasFuture = Transaction_->LookupRows(CollectRemovedReplicasKeys());
        auto removedReplicasOrError = WaitFor(replicasFuture);

        ProfileTime(ESequoiaReplicaModificationPhase::LookupRemovedLocationReplicas);

        ThrowOnSequoiaReplicasError(removedReplicasOrError, Config_->RetriableErrorCodes);

        return removedReplicasOrError.ValueOrThrow();
    }

    void GatherModifiedRemovedReplicas()
    {
        auto removedReplicas = LookupRemovedReplicas();

        std::vector<TSequoiaChunkReplica> replicasToRemove;
        for (const auto& replica : removedReplicas) {
            if (replica) {
                replicasToRemove.emplace_back(
                    replica->Key.ChunkId,
                    replica->Key.ReplicaIndex,
                    replica->Key.NodeId,
                    replica->Key.LocationIndex);
            }
        }
        std::ranges::sort(replicasToRemove);

        for (const auto& request : Requests_) {
            auto nodeId = FromProto<TNodeId>(request->node_id());

            for (const auto& chunkInfo : request->removed_chunks()) {
                auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));

                if (!std::ranges::binary_search(replicasToRemove, TSequoiaChunkReplica(
                    chunkIdWithIndex.Id,
                    chunkIdWithIndex.ReplicaIndex,
                    nodeId,
                    FromProto<TChunkLocationIndex>(chunkInfo.location_index()))))
                {
                    continue;
                }

                GatherModifiedChunkReplica(nodeId, chunkInfo);
            }
        }

        ProfileTime(ESequoiaReplicaModificationPhase::GatherModifiedRemovedReplicas);
    }

    std::vector<NRecords::TLocationReplicas> LookupExistingReplicasInReplacedLocation()
    {
        auto nodeId = FromProto<TNodeId>(ReplaceLocationRequest_->node_id());

        auto replacedLocationReplicasFuture = Transaction_->SelectRows<NRecords::TLocationReplicas>(BuildSelectLocationSequoiaReplicasQuery(
            Bootstrap_->GetCellTag(),
            nodeId,
            FromProto<TChunkLocationIndex>(ReplaceLocationRequest_->location_index())));

        auto existingReplicasInReplacedLocationOrError = WaitFor(replacedLocationReplicasFuture);

        ProfileTime(ESequoiaReplicaModificationPhase::LookupExistingReplicasInReplacedLocation);

        ThrowOnSequoiaReplicasError(existingReplicasInReplacedLocationOrError, Config_->RetriableErrorCodes);

        return existingReplicasInReplacedLocationOrError.ValueOrThrow();
    }

    inline bool ShouldProcessAddedReplicaOnMaster(const TChunkIdWithIndex& chunkIdWithIndex)
    {
        auto chunkSequoiaConfig = GetChunkSequoiaConfig(chunkIdWithIndex.Id, Config_);
        return chunkSequoiaConfig.StoreSequoiaReplicasOnMaster;
    }

    void GatherReplacedLocationReplicasDifference()
    {
        if (Requests_.size() != 1) {
            YT_TLOG_ALERT_AND_THROW("Invalid requests count in sequoia replicas modifier for location replacement")
                .With("RequestsCount", Requests_.size());
        }

        auto existingReplicas = LookupExistingReplicasInReplacedLocation();

        THashMap<TChunkIdWithIndex, EChunkReplicaState> existingReplicaStates;
        existingReplicaStates.reserve(existingReplicas.size());

        auto locationIndex = ReplaceLocationRequest_->location_index();

        for (const auto& replica : existingReplicas) {
            EmplaceOrCrash(
                existingReplicaStates,
                TChunkIdWithIndex(replica.Key.ChunkId, replica.Key.ReplicaIndex),
                replica.ReplicaState);
        }

        int changedReplicas = 0;
        int addedReplicas = 0;

        auto nodeId = FromProto<TNodeId>(ReplaceLocationRequest_->node_id());
        auto* modifyReplicasRequest = Requests_[0].get();

        for (const auto& chunkInfo : ReplaceLocationRequest_->chunks()) {
            auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));

            if (auto it = existingReplicaStates.find(chunkIdWithIndex); it != existingReplicaStates.end()) {
                auto replicaState = it->second;
                existingReplicaStates.erase(it);

                if (replicaState == GetAddedChunkReplicaState(chunkIdWithIndex.Id, chunkInfo)) {
                    continue;
                }
                // Replicas with changed states should be processed the same way as added replicas.
                ++changedReplicas;
            } else {
                ++addedReplicas;
            }
            GatherModifiedChunkReplica(nodeId, chunkInfo);

            if (ShouldProcessAddedReplicaOnMaster(chunkIdWithIndex)) {
                modifyReplicasRequest->add_added_chunks()->CopyFrom(chunkInfo);
            }
        }

        // We need to remove all existing replicas that were not reported, which means that existingReplicaStates set still contains them.
        for (const auto& replica : existingReplicas) {
            auto chunkIdWithIndex = TChunkIdWithIndex(replica.Key.ChunkId, replica.Key.ReplicaIndex);
            if (existingReplicaStates.contains(chunkIdWithIndex)) {
                auto* chunkInfo = modifyReplicasRequest->add_removed_chunks();

                ToProto(chunkInfo->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndex));
                chunkInfo->set_location_index(locationIndex);
                chunkInfo->set_caused_by_medium_change(false);
                GatherModifiedChunkReplica(nodeId, *chunkInfo);
            }
        }

        YT_TLOG_DEBUG("Gathered replaced location Sequoia replicas difference")
            .With("NodeId", nodeId)
            .With("LocationIndex", locationIndex)
            .With("AddedReplicas", addedReplicas)
            .With("RemovedReplicas", modifyReplicasRequest->removed_chunks_size())
            .With("ChangedReplicas", changedReplicas);

        ProfileTime(ESequoiaReplicaModificationPhase::GatherReplacedLocationReplicasDifference);
    }

    bool CheckIfRequestShouldBeAborted()
    {
        if (!ReplaceLocationRequest_->is_validation()) {
            return false;
        }
        if (Config_->EnableInGhostMode) {
            return false;
        }

        auto nodeId = FromProto<TNodeId>(ReplaceLocationRequest_->node_id());

        if (!ModifiedReplicas_.empty()) {
            YT_TLOG_ALERT("Sequoia replicas validation failed")
                .With("NodeId", nodeId)
                .With("LocationIndex", ReplaceLocationRequest_->location_index())
                .With("ChunkDifferenceSize", ModifiedReplicas_.size());
            auto modifiedReplicasSample = ModifiedReplicas_ | std::views::take(ChunkSampleSizeOnValidationFail);
            for (const auto& [chunkId, chunkModifiedReplicas] : modifiedReplicasSample) {
                YT_TLOG_DEBUG("Found chunk replicas mismatch during Sequoia replicas validation")
                    .With("NodeId", nodeId)
                    .With("LocationIndex", ReplaceLocationRequest_->location_index())
                    .With("ChunkId", chunkId)
                    .With("ReplicasToAddCount", chunkModifiedReplicas.AddedReplicas.size())
                    .With("ReplicasToRemoveCount", chunkModifiedReplicas.RemovedReplicas.size());
            }
            if (Config_->FixSequoiaReplicasIfReplicaValidationFailed) {
                YT_TLOG_DEBUG("Will fix Sequoia replicas on validation failure");
                return false;
            }
        } else {
            YT_TLOG_DEBUG("Validated Sequoia replicas for location")
                .With("NodeId", nodeId)
                .With("LocationIndex", ReplaceLocationRequest_->location_index());
        }

        return true;
    }

    void WriteRowsAndAddTransactionActions()
    {
        for (const auto& [chunkId, chunkModifiedReplicas] : ModifiedReplicas_) {
            NRecords::TChunkReplicas chunkReplicas{
                .Key = BuildChunkReplicasRecordKey(chunkId),
                .StoredReplicas = GetReplicasYson(chunkModifiedReplicas.AddedReplicas, chunkModifiedReplicas.RemovedReplicas),
                .LastSeenReplicas = GetReplicasListYson(chunkModifiedReplicas.AddedReplicas),
            };

            // We need 2 messages to be able to override some of them to debug.
            if (IsIncrementalHeartbeat_) {
                YT_TLOG_DEBUG("Sequoia chunk replicas changed during incremental heartbeat")
                    .With("ChunkId", chunkId)
                    .With(
                        "StoredReplicasDiff",
                        MakeFormattableView(chunkModifiedReplicas.AddedReplicas, TChunkReplicaWithLocationIndexAndStateFormatter()))
                    .With(
                        "LastSeenReplicasDiff",
                        MakeFormattableView(chunkModifiedReplicas.RemovedReplicas, TChunkReplicaWithLocationIndexAndStateFormatter()));
            } else {
                YT_TLOG_TRACE("Sequoia chunk replicas changed during full heartbeat")
                    .With("ChunkId", chunkId)
                    .With(
                        "StoredReplicasDiff",
                        MakeFormattableView(chunkModifiedReplicas.AddedReplicas, TChunkReplicaWithLocationIndexAndStateFormatter()))
                    .With(
                        "LastSeenReplicasDiff",
                        MakeFormattableView(chunkModifiedReplicas.RemovedReplicas, TChunkReplicaWithLocationIndexAndStateFormatter()))
                    .With("IsValidationHeartbeat", IsValidationHeartbeat_);
            }

            YT_VERIFY(chunkModifiedReplicas.AddedReplicas.size() + chunkModifiedReplicas.RemovedReplicas.size() > 0);
            Transaction_->WriteRow(
                chunkReplicas,
                NTableClient::ELockType::SharedWrite,
                NTableClient::EValueFlags::Aggregate);

            for (const auto& addedReplica : chunkModifiedReplicas.AddedReplicas) {
                NRecords::TLocationReplicas locationReplica{
                    .Key = {
                        .CellTag = Bootstrap_->GetCellTag(),
                        .NodeId = addedReplica.NodeId,
                        .LocationIndex = addedReplica.LocationIndex,
                        .ChunkId = chunkId,
                        .ReplicaIndex = static_cast<i8>(addedReplica.ReplicaIndex),
                    },
                    .ReplicaState = addedReplica.ReplicaState,
                };
                Transaction_->WriteRow(locationReplica);
            }

            for (const auto& removedReplica : chunkModifiedReplicas.RemovedReplicas) {
                NRecords::TLocationReplicasKey locationReplicaKey{
                    .CellTag = Bootstrap_->GetCellTag(),
                    .NodeId = removedReplica.NodeId,
                    .LocationIndex = removedReplica.LocationIndex,
                    .ChunkId = chunkId,
                    .ReplicaIndex = static_cast<i8>(removedReplica.ReplicaIndex),
                };
                Transaction_->DeleteRow(locationReplicaKey);
            }
            if (Config_->EnableSequoiaChunkRefresh) {
                NRecords::TChunkRefreshQueue refreshQueueEntry{
                    .TabletIndex = GetChunkShardIndex(chunkId),
                    .ChunkId = chunkId,
                    .ConfirmationTime = TInstant::Now(),
                };
                Transaction_->WriteRow(Bootstrap_->GetCellTag(), refreshQueueEntry);
            }
        }

        if (Config_->EnableSequoiaChunkRefresh) {
            for (auto chunkId : ChunksWithMediumChange_) {
                NRecords::TChunkRefreshQueue refreshQueueEntry{
                    .TabletIndex = GetChunkShardIndex(chunkId),
                    .ChunkId = chunkId,
                    .ConfirmationTime = TInstant::Now(),
                };
                Transaction_->WriteRow(Bootstrap_->GetCellTag(), refreshQueueEntry);
            }
        }

        for (auto& request : Requests_) {
            // We always clean master request to avoid additional work in automaton thread.
            auto addedChunksEndIt = std::remove_if(
                request->mutable_added_chunks()->begin(),
                request->mutable_added_chunks()->end(),
                [&] (const TChunkAddInfo& chunkAddInfo) {
                    auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkAddInfo.chunk_id()));
                    return !ShouldProcessAddedReplicaOnMaster(chunkIdWithIndex);
                });
            request->mutable_added_chunks()->erase(addedChunksEndIt, request->mutable_added_chunks()->end());

            // We should always process all removed chunks on master if request was not caused by node disposal.
            if (request->caused_by_node_disposal()) {
                auto removedChunksEndIt = std::remove_if(
                    request->mutable_removed_chunks()->begin(),
                    request->mutable_removed_chunks()->end(),
                    [&] (const TChunkRemoveInfo& chunkRemoveInfo) {
                        auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkRemoveInfo.chunk_id()));

                        auto chunkSequoiaConfig = GetChunkSequoiaConfig(chunkIdWithIndex.Id, Config_);

                        return !chunkSequoiaConfig.ProcessRemovedSequoiaReplicasOnMaster;
                    });
                request->mutable_removed_chunks()->erase(removedChunksEndIt, request->mutable_removed_chunks()->end());
            }
        }

        if (Config_->Enable) {
            if (Requests_.size() == 1) {
                // COMPAT(grphil)
                Transaction_->AddTransactionAction(
                    Bootstrap_->GetCellTag(),
                    NTransactionClient::MakeTransactionActionData(*Requests_[0]));
            } else {
                TReqModifyReplicasBatch batchRequest;
                for (auto& request : Requests_) {
                    *batchRequest.add_requests() = std::move(*request);
                }

                Transaction_->AddTransactionAction(
                    Bootstrap_->GetCellTag(),
                    NTransactionClient::MakeTransactionActionData(batchRequest));
            }
        }


        ProfileTime(ESequoiaReplicaModificationPhase::WriteRowsAndAddTransactionActions);
    }

    void Finish()
    {
        NProto::TReqPromoteLastCommitTimestamp promoteCommitTimestampRequest;
        Transaction_->AddTransactionAction(
            Bootstrap_->GetCellTag(),
            NTransactionClient::MakeTransactionActionData(promoteCommitTimestampRequest));

        Transaction_->AddBarrierTags({NApi::NNative::SequoiaReplicasOrderingTag});
        Transaction_->AddStrongOrderingTags({NApi::NNative::SequoiaReplicasOrderingTag});
        NApi::TTransactionCommitOptions commitOptions{
            .CoordinatorCellId = Bootstrap_->GetCellId(),
            .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
        };

        auto result = WaitFor(Transaction_->Commit(std::move(commitOptions)));

        ProfileTime(ESequoiaReplicaModificationPhase::CommitTransaction);

        ThrowOnSequoiaReplicasError(result, Config_->RetriableErrorCodes);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaReplicasModifierPtr CreateSequoiaReplicasModifier(
    TSequoiaReplicaModificationProfile& modificationProfile,
    ESequoiaTransactionType transactionType,
    TBootstrap* bootstrap,
    const TDynamicChunkManagerConfigPtr& config)
{
    auto replicasModifier = New<TSequoiaReplicasModifier>(
        modificationProfile,
        transactionType,
        bootstrap,
        config);

    return replicasModifier;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

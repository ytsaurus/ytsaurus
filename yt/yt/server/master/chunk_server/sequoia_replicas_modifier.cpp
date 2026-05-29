#include "sequoia_replicas_modifier.h"

#include "chunk_replica.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

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

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

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
        , EnableSequoiaChunkRefresh_(config->SequoiaChunkReplicas->EnableSequoiaChunkRefresh)
        , ProcessRemovedSequoiaReplicasOnMaster_(config->SequoiaChunkReplicas->ProcessRemovedSequoiaReplicasOnMaster)
        , StoreSequoiaReplicasOnMaster_(config->SequoiaChunkReplicas->StoreSequoiaReplicasOnMaster)
        , ClearMasterRequest_(config->SequoiaChunkReplicas->ClearMasterRequest)
        , FixSequoiaReplicasIfReplicaValidationFailed_(config->SequoiaChunkReplicas->FixSequoiaReplicasIfReplicaValidationFailed)
        , RetriableErrorCodes_(config->SequoiaChunkReplicas->RetriableErrorCodes)
        , Bootstrap_(bootstrap)
        , Profile_(profile)
    { }

    void AddRequest(
        std::unique_ptr<TReqModifyReplicas> request) override
    {
        if (ReplaceLocationRequest_) {
            YT_LOG_ALERT_AND_THROW("Sequoia replicas modifier can not have both replace and modify requests");
        }
        Requests_.push_back(std::move(request));
    }

    void AddRequest(
        std::unique_ptr<TReqReplaceLocationReplicas> request) override
    {
        if (!Requests_.empty()) {
            YT_LOG_ALERT_AND_THROW("Replace location request must be unique in sequoia replicas modifier");
        }
        ReplaceLocationRequest_ = std::move(request);
    }

    TFuture<void> ModifyReplicas() override
    {
        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->StartTransaction(
                TransactionType_,
                {.CellTag = Bootstrap_->GetCellTag()})
            .Apply(BIND(&TSequoiaReplicasModifier::DoModifyReplicas, MakeStrong(this))
                .AsyncVia(TDispatcher::Get()->GetHeavyInvoker()));
    }

private:
    const ESequoiaTransactionType TransactionType_;
    const bool EnableSequoiaChunkRefresh_;
    const bool ProcessRemovedSequoiaReplicasOnMaster_;
    const bool StoreSequoiaReplicasOnMaster_;
    const bool ClearMasterRequest_;
    const bool FixSequoiaReplicasIfReplicaValidationFailed_;
    const std::vector<TErrorCode> RetriableErrorCodes_;

    TBootstrap* const Bootstrap_;
    TSequoiaReplicaModificationProfile& Profile_;

    ISequoiaTransactionPtr Transaction_;

    NProfiling::TWallTimer Timer_;

    std::vector<std::unique_ptr<TReqModifyReplicas>> Requests_;
    std::unique_ptr<TReqReplaceLocationReplicas> ReplaceLocationRequest_;

    struct TReplicaList
    {
        std::vector<TChunkReplicaWithLocationIndex> AddedReplicas;
        std::vector<TChunkReplicaWithLocationIndex> RemovedReplicas;
    };

    THashSet<TChunkId> ChunksWithMediumChange_;
    THashMap<TChunkId, TReplicaList> ModifiedReplicas_;

    static constexpr size_t ChunkSampleSizeOnValidationFail = 10;

    void DoModifyReplicas(const ISequoiaTransactionPtr& transaction)
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
            YT_LOG_ALERT_AND_THROW("No requests for sequoia replicas modifier");
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

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::StartTransaction].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
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
            YT_LOG_TRACE("%v of Sequoia replica is caused by medium change (ChunkId: %v, ReplicaIndex: %v, LocationIndex: %v)",
                chunkAdded ? "Addition" : "Removal",
                chunkId,
                chunkIdWithIndex.ReplicaIndex,
                locationIndex);
            return;
        }

        auto replica = TChunkReplicaWithLocationIndex(
            nodeId,
            chunkIdWithIndex.ReplicaIndex,
            locationIndex);

        if constexpr (chunkAdded) {
            ModifiedReplicas_[chunkId].AddedReplicas.push_back(replica);
        } else {
            ModifiedReplicas_[chunkId].RemovedReplicas.push_back(replica);
        }

        YT_LOG_TRACE("Sequoia replica is being %v (ChunkId: %v, ReplicaIndex: %v, LocationIndex: %v, NodeId: %v)",
            chunkAdded ? "added" : "removed",
            chunkId,
            chunkIdWithIndex.ReplicaIndex,
            locationIndex,
            nodeId);
    }

    void GatherModifiedAddedChunkReplicas()
    {
        for (const auto& request : Requests_) {
            auto nodeId = FromProto<TNodeId>(request->node_id());
            for (const auto& chunkInfo : request->added_chunks()) {
                GatherModifiedChunkReplica(nodeId, chunkInfo);
            }
        }

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::GatherModifiedAddedReplicas].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
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
                YT_LOG_TRACE("Preparing removed Sequoia replicas keys (ChunkId: %v, ReplicaIndex: %v, LocationIndex: %v, NodeId: %v)",
                    chunkId,
                    chunkIdWithIndex.ReplicaIndex,
                    locationIndex,
                    nodeId);
            }
        }

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::ParseRemovedReplicas].Add(Timer_.GetElapsedTime());
        Timer_.Restart();

        return removedReplicasKeys;
    }

    std::vector<std::optional<NRecords::TLocationReplicas>> LookupRemovedReplicas()
    {
        auto replicasFuture = Transaction_->LookupRows(CollectRemovedReplicasKeys());
        auto removedReplicasOrError = WaitFor(replicasFuture);

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::LookupRemovedLocationReplicas].Add(Timer_.GetElapsedTime());
        Timer_.Restart();

        ThrowOnSequoiaReplicasError(removedReplicasOrError, RetriableErrorCodes_);

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

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::GatherModifiedRemovedReplicas].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    std::vector<NRecords::TLocationReplicas> LookupExistingReplicasInReplacedLocation()
    {
        auto nodeId = FromProto<TNodeId>(ReplaceLocationRequest_->node_id());

        auto replacedLocationReplicasFuture = Transaction_->SelectRows<NRecords::TLocationReplicas>(BuildSelectLocationSequoiaReplicasQuery(
            Bootstrap_->GetCellTag(),
            nodeId,
            FromProto<TChunkLocationIndex>(ReplaceLocationRequest_->location_index())));

        auto existingReplicasInReplacedLocationOrError = WaitFor(replacedLocationReplicasFuture);

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::LookupExistingReplicasInReplacedLocation].Add(Timer_.GetElapsedTime());
        Timer_.Restart();

        ThrowOnSequoiaReplicasError(existingReplicasInReplacedLocationOrError, RetriableErrorCodes_);

        return existingReplicasInReplacedLocationOrError.ValueOrThrow();
    }

    void GatherReplacedLocationReplicasDifference()
    {
        if (Requests_.size() != 1) {
            YT_LOG_ALERT_AND_THROW(
                "Invalid requests count in sequoia replicas modifier for location replacement (RequestsCount: %v)",
                Requests_.size());
        }

        auto existingReplicas = LookupExistingReplicasInReplacedLocation();

        THashSet<TChunkIdWithIndex> existingReplicasSet;
        existingReplicasSet.reserve(existingReplicas.size());

        auto locationIndex = ReplaceLocationRequest_->location_index();
        auto nodeId = FromProto<TNodeId>(ReplaceLocationRequest_->node_id());
        auto* modifyReplicasRequest = Requests_[0].get();

        for (const auto& replica : existingReplicas) {
            existingReplicasSet.emplace(replica.Key.ChunkId, replica.Key.ReplicaIndex);
        }


        for (const auto& chunkInfo : ReplaceLocationRequest_->chunks()) {
            auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
            if (!existingReplicasSet.contains(chunkIdWithIndex)) {
                GatherModifiedChunkReplica(nodeId, chunkInfo);

                // Chunk may be needed for master chunk refresh.
                modifyReplicasRequest->add_added_chunks()->CopyFrom(chunkInfo);
            } else {
                existingReplicasSet.erase(chunkIdWithIndex);
            }
        }

        // We need to remove all existing replicas that were not reported, which means that existingReplicasSet set still contains them.
        for (const auto& replica : existingReplicas) {
            auto chunkIdWithIndex = TChunkIdWithIndex(replica.Key.ChunkId, replica.Key.ReplicaIndex);

            if (existingReplicasSet.contains(chunkIdWithIndex)) {
                auto* chunkInfo = modifyReplicasRequest->add_removed_chunks();

                ToProto(chunkInfo->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndex));
                chunkInfo->set_location_index(locationIndex);
                chunkInfo->set_caused_by_medium_change(false);
                GatherModifiedChunkReplica(nodeId, *chunkInfo);
            }
        }

        YT_LOG_DEBUG(
            "Gathered replaced location Sequoia replicas difference (NodeId: %v, LocationIndex: %v, AddedReplicas: %v, RemovedReplicas: %v)",
            nodeId,
            locationIndex,
            modifyReplicasRequest->added_chunks_size(),
            modifyReplicasRequest->removed_chunks_size());

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::GatherReplacedLocationReplicasDifference].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    bool CheckIfRequestShouldBeAborted()
    {
        if (!ReplaceLocationRequest_->is_validation()) {
            return false;
        }

        auto nodeId = FromProto<TNodeId>(ReplaceLocationRequest_->node_id());

        if (!ModifiedReplicas_.empty()) {
            YT_LOG_ALERT(
                "Sequoia replicas validation failed (NodeId: %v, LocationIndex: %v, ChunkDifferenceSize: %v)",
                nodeId,
                ReplaceLocationRequest_->location_index(),
                ModifiedReplicas_.size());
            auto modifiedReplicasSample = ModifiedReplicas_ | std::views::take(ChunkSampleSizeOnValidationFail);
            for (const auto& [chunkId, chunkModifiedReplicas] : modifiedReplicasSample) {
                YT_LOG_DEBUG(
                    "Found chunk replicas mismatch during Sequoia replicas validation "
                    "(NodeId: %v, LocationIndex: %v, ChunkId: %v, ReplicasToAddCount: %v, ReplicasToRemoveCount: %v)",
                    nodeId,
                    ReplaceLocationRequest_->location_index(),
                    chunkId,
                    chunkModifiedReplicas.AddedReplicas.size(),
                    chunkModifiedReplicas.RemovedReplicas.size());
            }
            if (FixSequoiaReplicasIfReplicaValidationFailed_) {
                YT_LOG_DEBUG("Will fix Sequoia replicas on validation failure");
                return false;
            }
        } else {
            YT_LOG_DEBUG(
                "Validated Sequoia replicas for location (NodeId: %v, LocationIndex: %v)",
                nodeId,
                ReplaceLocationRequest_->location_index());
        }

        return true;
    }

    void WriteRowsAndAddTransactionActions()
    {
        for (const auto& [chunkId, chunkModifiedReplicas] : ModifiedReplicas_) {
            NRecords::TChunkReplicas chunkReplicas{
                .Key = {
                    .ChunkId = chunkId,
                },
                .StoredReplicas = GetReplicasYson(chunkModifiedReplicas.AddedReplicas, chunkModifiedReplicas.RemovedReplicas),
                .LastSeenReplicas = GetReplicasListYson(chunkModifiedReplicas.AddedReplicas),
            };

            YT_LOG_TRACE("Sequoia chunk replicas changed (ChunkId: %v, StoredReplicasDiff: %v, LastSeenReplicasDiff: %v)",
                chunkId,
                MakeFormattableView(chunkModifiedReplicas.AddedReplicas, TChunkReplicaWithLocationIndexFormatter()),
                MakeFormattableView(chunkModifiedReplicas.RemovedReplicas, TChunkReplicaWithLocationIndexFormatter()));

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
                        .ReplicaIndex = addedReplica.ReplicaIndex,
                    },
                    .Fake = true,
                };
                Transaction_->WriteRow(locationReplica);
            }

            for (const auto& removedReplica : chunkModifiedReplicas.RemovedReplicas) {
                NRecords::TLocationReplicasKey locationReplicaKey{
                    .CellTag = Bootstrap_->GetCellTag(),
                    .NodeId = removedReplica.NodeId,
                    .LocationIndex = removedReplica.LocationIndex,
                    .ChunkId = chunkId,
                    .ReplicaIndex = removedReplica.ReplicaIndex,
                };
                Transaction_->DeleteRow(locationReplicaKey);
            }
            if (EnableSequoiaChunkRefresh_) {
                NRecords::TChunkRefreshQueue refreshQueueEntry{
                    .TabletIndex = GetChunkShardIndex(chunkId),
                    .ChunkId = chunkId,
                    .ConfirmationTime = TInstant::Now(),
                };
                Transaction_->WriteRow(Bootstrap_->GetCellTag(), refreshQueueEntry);
            }
        }

        if (EnableSequoiaChunkRefresh_) {
            for (auto chunkId : ChunksWithMediumChange_) {
                NRecords::TChunkRefreshQueue refreshQueueEntry{
                    .TabletIndex = GetChunkShardIndex(chunkId),
                    .ChunkId = chunkId,
                    .ConfirmationTime = TInstant::Now(),
                };
                Transaction_->WriteRow(Bootstrap_->GetCellTag(), refreshQueueEntry);
            }
        }

        if (ClearMasterRequest_) {
            for (auto& request : Requests_) {
                if (!StoreSequoiaReplicasOnMaster_) {
                    request->mutable_added_chunks()->Clear();
                }
                if (!ProcessRemovedSequoiaReplicasOnMaster_ && request->caused_by_node_disposal()) {
                    request->mutable_removed_chunks()->Clear();
                }
            }
        }

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


        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::WriteRowsAndAddTransactionActions].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    void Finish()
    {
        NApi::TTransactionCommitOptions commitOptions{
            .CoordinatorCellId = Bootstrap_->GetCellId(),
            .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
            .StronglyOrdered = true,
        };

        auto result = WaitFor(Transaction_->Commit(commitOptions));

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::CommitTransaction].Add(Timer_.GetElapsedTime());

        ThrowOnSequoiaReplicasError(result, RetriableErrorCodes_);
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

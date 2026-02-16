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

    void SetModifyReplicasRequest(std::unique_ptr<TReqModifyReplicas>&& request)
    {
        Request_ = std::move(request);
    }

    void SetReplaceLocationReplicasRequest(std::unique_ptr<TReqReplaceLocationReplicas>&& request)
    {
        ReplaceLocationRequest_ = std::move(request);
    }

    TFuture<TRspModifyReplicas> ModifyReplicas() override
    {
        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->StartTransaction(
                TransactionType_,
                {.CellTag = Bootstrap_->GetCellTag()})
            .Apply(BIND([this, this_ = MakeStrong(this)] (const ISequoiaTransactionPtr& transaction) mutable {
                if (Request_) {
                    return DoModifyReplicas(transaction);
                } else {
                    return DoReplaceLocationReplicas(transaction);
                }
            }).AsyncVia(TDispatcher::Get()->GetHeavyInvoker()));
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

    std::unique_ptr<TReqModifyReplicas> Request_;
    std::unique_ptr<TReqReplaceLocationReplicas> ReplaceLocationRequest_;
    ISequoiaTransactionPtr Transaction_;

    NProfiling::TWallTimer Timer_;

    TNodeId NodeId_;

    struct TReplicaList
    {
        std::vector<TChunkReplicaWithLocationIndex> AddedReplicas;
        std::vector<TChunkReplicaWithLocationIndex> RemovedReplicas;
    };

    THashSet<TChunkId> ChunksWithMediumChange_;
    THashMap<TChunkId, TReplicaList> ModifiedReplicas_;

    static constexpr size_t ChunkSampleSizeOnValidationFail = 10;

    TRspModifyReplicas DoModifyReplicas(const ISequoiaTransactionPtr& transaction)
    {
        YT_VERIFY(Request_ && !ReplaceLocationRequest_);
        Start(transaction);
        GatherModifiedAddedChunkReplicas();
        GatherModifiedRemovedReplicas();
        WriteRowsAndAddTransactionActions();
        return Finish();
    }

    TRspModifyReplicas DoReplaceLocationReplicas(const ISequoiaTransactionPtr& transaction)
    {
        YT_VERIFY(ReplaceLocationRequest_ && !Request_);

        Request_ = std::make_unique<TReqModifyReplicas>();
        Request_->set_node_id(ReplaceLocationRequest_->node_id());
        *Request_->mutable_dead_chunk_ids() = std::move(*ReplaceLocationRequest_->mutable_dead_chunk_ids());

        Start(transaction);
        GatherReplacedLocationReplicasDifference();
        if (CheckIfRequestShouldBeAborted()) {
            return TRspModifyReplicas();
        }
        WriteRowsAndAddTransactionActions();
        return Finish();
    }

    void Start(const ISequoiaTransactionPtr& transaction)
    {
        Transaction_ = transaction;
        NodeId_ = FromProto<TNodeId>(Request_->node_id());

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::StartTransaction].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    template <typename TChunkInfo>
    void GatherModifiedChunkReplica(const TChunkInfo& chunkInfo)
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

        TChunkReplicaWithLocationIndex replica(
            NodeId_,
            chunkIdWithIndex.ReplicaIndex,
            locationIndex);

        if constexpr (chunkAdded) {
            ModifiedReplicas_[chunkId].AddedReplicas.push_back(replica);
        } else {
            ModifiedReplicas_[chunkId].RemovedReplicas.push_back(replica);
        }

        YT_LOG_TRACE("%v Sequoia replica (ChunkId: %v, ReplicaIndex: %v, LocationIndex: %v)",
            chunkAdded ? "Adding" : "Removing",
            chunkId,
            chunkIdWithIndex.ReplicaIndex,
            locationIndex);
    }

    void GatherModifiedAddedChunkReplicas()
    {
        for (const auto& chunkInfo : Request_->added_chunks()) {
            GatherModifiedChunkReplica(chunkInfo);
        }

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::GatherModifiedAddedReplicas].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    std::vector<NRecords::TLocationReplicasKey> CollectRemovedReplicasKeys()
    {
        std::vector<NRecords::TLocationReplicasKey> removedReplicasKeys;
        for (const auto& chunkInfo : Request_->removed_chunks()) {
            auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
            auto chunkId = chunkIdWithIndex.Id;

            auto locationIndex = FromProto<TChunkLocationIndex>(chunkInfo.location_index());

            if (chunkInfo.caused_by_medium_change()) {
                // Chunks with medium change will be processed later
                continue;
            }

            NRecords::TLocationReplicasKey locationReplicaKey{
                .CellTag = Bootstrap_->GetCellTag(),
                .NodeId = NodeId_,
                .LocationIndex = locationIndex,
                .ChunkId = chunkId,
                .ReplicaIndex = chunkIdWithIndex.ReplicaIndex
            };
            removedReplicasKeys.push_back(locationReplicaKey);
            YT_LOG_TRACE("Preparing removed Sequoia replicas keys (ChunkId: %v, ReplicaIndex: %v, LocationIndex: %v)",
                chunkId,
                chunkIdWithIndex.ReplicaIndex,
                locationIndex);
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

        THashSet<TChunkIdWithIndex> chunksWithReplicas;
        for (const auto& replica : removedReplicas) {
            if (replica) {
                chunksWithReplicas.emplace(replica->Key.ChunkId, replica->Key.ReplicaIndex);
            }
        }

        for (const auto& chunkInfo : Request_->removed_chunks()) {
            auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));

            if (!chunksWithReplicas.contains(chunkIdWithIndex)) {
                continue;
            }

            GatherModifiedChunkReplica(chunkInfo);
        }

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::GatherModifiedRemovedReplicas].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    std::vector<NRecords::TLocationReplicas> LookupExistingReplicasInReplacedLocation()
    {
        auto replacedLocationReplicasFuture = Transaction_->SelectRows<NRecords::TLocationReplicas>(BuildSelectLocationSequoiaReplicasQuery(
            Bootstrap_->GetCellTag(),
            NodeId_,
            FromProto<TChunkLocationIndex>(ReplaceLocationRequest_->location_index())));

        auto existingReplicasInReplacedLocationOrError = WaitFor(replacedLocationReplicasFuture);

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::LookupExistingReplicasInReplacedLocation].Add(Timer_.GetElapsedTime());
        Timer_.Restart();

        ThrowOnSequoiaReplicasError(existingReplicasInReplacedLocationOrError, RetriableErrorCodes_);

        return existingReplicasInReplacedLocationOrError.ValueOrThrow();
    }

    void GatherReplacedLocationReplicasDifference()
    {
        auto existingReplicas = LookupExistingReplicasInReplacedLocation();

        THashSet<TChunkIdWithIndex> existingReplicasSet;
        existingReplicasSet.reserve(existingReplicas.size());

        auto locationIndex = ReplaceLocationRequest_->location_index();

        for (const auto& replica : existingReplicas) {
            existingReplicasSet.emplace(replica.Key.ChunkId, replica.Key.ReplicaIndex);
        }

        for (const auto& chunkInfo : ReplaceLocationRequest_->chunks()) {
            auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
            if (!existingReplicasSet.contains(chunkIdWithIndex)) {
                GatherModifiedChunkReplica(chunkInfo);

                // Chunk may be needed for master chunk refresh.
                Request_->add_added_chunks()->CopyFrom(chunkInfo);
            } else {
                existingReplicasSet.erase(chunkIdWithIndex);
            }
        }

        // We need to remove all existing replicas that were not reported, which means that existingReplicasSet set still contains them.
        for (const auto& replica : existingReplicas) {
            auto chunkIdWithIndex = TChunkIdWithIndex(replica.Key.ChunkId, replica.Key.ReplicaIndex);
            if (existingReplicasSet.contains(chunkIdWithIndex)) {
                TChunkRemoveInfo chunkInfo;
                ToProto(chunkInfo.mutable_chunk_id(), EncodeChunkId(chunkIdWithIndex));
                chunkInfo.set_location_index(locationIndex);
                chunkInfo.set_caused_by_medium_change(false);
                GatherModifiedChunkReplica(chunkInfo);

                // Chunk may be needed for master chunk refresh.
                Request_->add_removed_chunks()->CopyFrom(chunkInfo);
            }
        }

        YT_LOG_DEBUG(
            "Gathered replaced location Sequoia replicas difference (NodeId: %v, LocationIndex: %v, AddedReplicas: %v, RemovedReplicas: %v)",
            NodeId_,
            locationIndex,
            Request_->added_chunks_size(),
            Request_->removed_chunks_size());

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::GatherReplacedLocationReplicasDifference].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    bool CheckIfRequestShouldBeAborted()
    {
        if (!ReplaceLocationRequest_->is_validation()) {
            return false;
        }

        if (!ModifiedReplicas_.empty()) {
            YT_LOG_ALERT(
                "Sequoia replicas validation failed (NodeId: %v, LocationIndex: %v, ChunkDifferenceSize: %v)",
                NodeId_,
                ReplaceLocationRequest_->location_index(),
                ModifiedReplicas_.size());
            auto modifiedReplicasSample = ModifiedReplicas_ | std::views::take(ChunkSampleSizeOnValidationFail);
            for (const auto& [chunkId, chunkModifiedReplicas] : modifiedReplicasSample) {
                YT_LOG_DEBUG(
                    "Found chunk replicas mismatch during Sequoia replicas validation "
                    "(NodeId: %v, LocationIndex: %v, ChunkId: %v, ReplicasToAddCount: %v, ReplicasToRemoveCount: %v)",
                    NodeId_,
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
                NodeId_,
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
            YT_LOG_TRACE("Sequoia Chunk replicas changed (ChunkId: %v, StoredReplicasDiff: %v, LastSeenReplicasDiff: %v)",
                chunkId,
                chunkReplicas.StoredReplicas,
                chunkReplicas.LastSeenReplicas);

            YT_VERIFY(chunkModifiedReplicas.AddedReplicas.size() + chunkModifiedReplicas.RemovedReplicas.size() > 0);
            Transaction_->WriteRow(
                chunkReplicas,
                NTableClient::ELockType::SharedWrite,
                NTableClient::EValueFlags::Aggregate);

            for (const auto& addedReplica : chunkModifiedReplicas.AddedReplicas) {
                NRecords::TLocationReplicas locationReplica{
                    .Key = {
                        .CellTag = Bootstrap_->GetCellTag(),
                        .NodeId = NodeId_,
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
                    .NodeId = NodeId_,
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

        // If we do not need replicas on master, we can make request more lightweight.
        if (ClearMasterRequest_) {
            if (!StoreSequoiaReplicasOnMaster_) {
                Request_->mutable_added_chunks()->Clear();
            }
            if (!ProcessRemovedSequoiaReplicasOnMaster_ && Request_->caused_by_node_disposal()) {
                Request_->mutable_removed_chunks()->Clear();
            }
        }

        Transaction_->AddTransactionAction(
            Bootstrap_->GetCellTag(),
            NTransactionClient::MakeTransactionActionData(*Request_));

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::WriteRowsAndAddTransactionActions].Add(Timer_.GetElapsedTime());
        Timer_.Restart();
    }

    TRspModifyReplicas Finish()
    {
        NApi::TTransactionCommitOptions commitOptions{
            .CoordinatorCellId = Bootstrap_->GetCellId(),
            .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
            .StronglyOrdered = true,
        };

        auto result = WaitFor(Transaction_->Commit(commitOptions));

        Profile_.CumulativeTime[ESequoiaReplicaModificationPhase::CommitTransaction].Add(Timer_.GetElapsedTime());

        ThrowOnSequoiaReplicasError(result, RetriableErrorCodes_);

        // TODO(aleksandra-zh): add ally replica info.
        TRspModifyReplicas response;
        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaReplicasModifierPtr CreateSequoiaReplicasModifier(
    std::unique_ptr<TReqModifyReplicas> request,
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

    replicasModifier->SetModifyReplicasRequest(std::move(request));
    return replicasModifier;
}

ISequoiaReplicasModifierPtr CreateSequoiaLocationReplicasReplacer(
    std::unique_ptr<NDataNodeTrackerClient::NProto::TReqReplaceLocationReplicas> request,
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

    replicasModifier->SetReplaceLocationReplicasRequest(std::move(request));
    return replicasModifier;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

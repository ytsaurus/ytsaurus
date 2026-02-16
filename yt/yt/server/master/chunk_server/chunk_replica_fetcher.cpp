#include "chunk_replica_fetcher.h"
#include "public.h"
#include "private.h"
#include "chunk.h"
#include "chunk_location.h"
#include "config.h"
#include "data_node_tracker.h"
#include "domestic_medium.h"
#include "chunk_manager.h"
#include "helpers.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/ytlib/sequoia_client/connection.h>
#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>

#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/unapproved_chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NSequoiaClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

    std::vector<TSequoiaChunkReplica> ParseReplicas(
    const auto& replicaRecords,
    const auto& extractReplicas)
{
    std::vector<TSequoiaChunkReplica> replicas;
    for (const auto& replicaRecord : replicaRecords) {
        if (!replicaRecord) {
            continue;
        }

        auto chunkId = replicaRecord->Key.ChunkId;
        auto extractedReplicas = extractReplicas(*replicaRecord);

        ParseChunkReplicas(
            extractedReplicas,
            [&] (const TParsedChunkReplica& parsedReplica) {
                replicas.push_back({
                    .ChunkId = chunkId,
                    .ReplicaIndex = parsedReplica.ReplicaIndex,
                    .NodeId = parsedReplica.NodeId,
                    .LocationIndex = parsedReplica.LocationIndex,
                });
            });
    }

    return replicas;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaFetcher
    : public IChunkReplicaFetcher
{
public:
    explicit TChunkReplicaFetcher(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    bool CanHaveSequoiaReplicas(TChunkId chunkId, int probability) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (IsJournalChunkId(chunkId)) {
            return false;
        }

        return static_cast<int>(EntropyFromId(chunkId) % 100) < probability;
    }

    bool CanHaveSequoiaReplicas(TChunkId chunkId) const override
    {
        VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return false;
        }

        auto probability = config->ReplicasPercentage;
        return CanHaveSequoiaReplicas(chunkId, probability);
    }


    TFuture<std::vector<NRecords::TLocationReplicas>> GetSequoiaLocationReplicas(
        TNodeId nodeId,
        TChunkLocationIndex locationIndex) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return MakeFuture<std::vector<NRecords::TLocationReplicas>>({});
        }
        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->SelectRows<NRecords::TLocationReplicas>(BuildSelectLocationSequoiaReplicasQuery(
                Bootstrap_->GetCellTag(),
                nodeId,
                locationIndex
            )).Apply(BIND([retriableErrorCodes] (const TErrorOr<std::vector<NRecords::TLocationReplicas>>& result) {
                ThrowOnSequoiaReplicasError(result, retriableErrorCodes);
                return result;
            }));
    }

    TFuture<std::vector<NRecords::TLocationReplicas>> GetSequoiaNodeReplicas(TNodeId nodeId) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return MakeFuture<std::vector<NRecords::TLocationReplicas>>({});
        }
        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->SelectRows<NRecords::TLocationReplicas>({
                .WhereConjuncts = {
                    Format("cell_tag = %v", Bootstrap_->GetCellTag()),
                    Format("node_id = %v", nodeId),
                }
            }).Apply(BIND([retriableErrorCodes] (const TErrorOr<std::vector<NRecords::TLocationReplicas>>& result) {
                ThrowOnSequoiaReplicasError(result, retriableErrorCodes);
                return result;
            }));
    }

    TFuture<std::vector<TNodeId>> GetLastSeenReplicas(const TEphemeralObjectPtr<TChunk>& chunk) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        auto chunkId = chunk->GetId();
        auto isErasure = chunk->IsErasure();

        auto masterReplicas = chunk->LastSeenReplicas();
        std::vector<TNodeId> replicas(masterReplicas.begin(), masterReplicas.end());

        if (isErasure && std::ssize(replicas) < ::NErasure::MaxTotalPartCount) {
            if (!replicas.empty()) {
                YT_LOG_ALERT("Last seen replicas count stored on master is weird (ChunkId: %v, ReplicaCount: %v)",
                    chunkId,
                    std::ssize(replicas));
            }
            replicas.resize(::NErasure::MaxTotalPartCount);
        }

        if (!CanHaveSequoiaReplicas(chunkId) || !GetDynamicConfig()->FetchReplicasFromSequoia) {
            return MakeFuture(replicas);
        }

        return DoGetSequoiaLastSeenReplicas(chunkId)
            .Apply(BIND([replicas = std::move(replicas), isErasure] (const std::vector<TSequoiaChunkReplica>& sequoiaReplicas) mutable {
                if (isErasure) {
                    YT_VERIFY(std::ssize(replicas) == ::NErasure::MaxTotalPartCount);
                    for (const auto& replica : sequoiaReplicas) {
                        replicas[replica.ReplicaIndex] = replica.NodeId;
                    }
                } else {
                    for (const auto& replica : sequoiaReplicas) {
                        replicas.push_back(replica.NodeId);
                    }
                    SortUnique(replicas);
                }

                return replicas;
            }));
    }

    TErrorOr<TStoredChunkReplicaList> GetChunkReplicas(
        const TEphemeralObjectPtr<TChunk>& chunk,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        // This is so stupid.
        std::vector<TEphemeralObjectPtr<TChunk>> chunks;
        chunks.emplace_back(chunk.Get());
        auto result = GetChunkReplicas(chunks, includeUnapproved);
        return GetOrCrash(result, chunk->GetId());
    }
    TChunkToStoredChunkReplicaList GetChunkReplicas(
        const std::vector<TEphemeralObjectPtr<TChunk>>& chunks,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        auto sequoiaChunkIds = FilterSequoiaChunkIds(chunks);

        auto validate = GetDynamicConfig()->ValidateSequoiaReplicasFetch;
        auto fetchReplicasFromSequoia = GetDynamicConfig()->FetchReplicasFromSequoia;

        // COMPAT(grphil)
        auto includeNonOnlineReplicas = GetBootstrap()->GetConfigManager()->GetConfig()->ChunkManager->AlwaysFetchNonOnlineReplicas;

        // Fastpath.
        if (!fetchReplicasFromSequoia || sequoiaChunkIds.empty()) {
            TChunkToStoredChunkReplicaList result;
            for (const auto& chunk : chunks) {
                result.emplace(chunk->GetId(), chunk->GetStoredReplicaList(includeNonOnlineReplicas));
            }
            return result;
        }

        THashMap<TChunkId, std::vector<TSequoiaChunkReplica>> masterReplicasInSequoiaSkin;
        THashMap<TChunkId, std::vector<TSequoiaChunkReplica>> unapprovedMasterReplicasInSequoiaSkin;
        // We only need these for validation.
        // If validation is disabled master replicas will be fetched below (in CombineReplicas).
        if (validate) {
            for (const auto& chunk : chunks) {
                auto masterReplicas = chunk->GetStoredReplicaList(includeNonOnlineReplicas);
                std::vector<TSequoiaChunkReplica> replicas;
                std::vector<TSequoiaChunkReplica> unapprovedReplicas;
                for (const auto& masterReplica : masterReplicas) {
                    TSequoiaChunkReplica replica;
                    replica.ChunkId = chunk->GetId();
                    replica.ReplicaIndex = masterReplica.GetReplicaIndex();
                    replica.NodeId = masterReplica.GetNodeId();
                    replica.ReplicaState = masterReplica.GetReplicaState();
                    if (auto* locationReplica = masterReplica.As<EStoredReplicaType::ChunkLocation>()) {
                        // NB: InvalidChunkLocationIndex will be used as default for offshore media.
                        replica.LocationIndex = locationReplica->GetChunkLocationIndex();
                        auto* location = locationReplica->AsChunkLocationPtr();
                        if (location->HasUnapprovedReplica(TChunkPtrWithReplicaIndex(chunk.Get(), masterReplica.GetReplicaIndex()))) {
                            unapprovedReplicas.push_back(replica);
                        } else {
                            replicas.push_back(replica);
                        }
                    } else {
                        replicas.push_back(replica);
                    }
                }

                YT_LOG_TRACE("Fetched master replicas (ChunkId: %v, MasterReplicas: %v, UnapprovedMasterReplicas: %v)",
                    chunk->GetId(),
                    replicas,
                    unapprovedReplicas);

                // Chunks can have duplicates.
                masterReplicasInSequoiaSkin.emplace(chunk->GetId(), std::move(replicas));
                unapprovedMasterReplicasInSequoiaSkin.emplace(chunk->GetId(), std::move(unapprovedReplicas));
            }
        }

        const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        auto timestamp = transactionSupervisor->GetLastCoordinatorCommitTimestamp();
        if (!validate || !timestamp) {
            timestamp = NTransactionClient::SyncLastCommittedTimestamp;
        }

        // Let's not fetch unapproved replicas for validation.
        includeUnapproved = !validate && (includeUnapproved || GetDynamicConfig()->AlwaysIncludeUnapprovedReplicas);
        auto sequoiaReplicasOrError = WaitForFast(DoGetOnlySequoiaChunkReplicas(sequoiaChunkIds, includeUnapproved, validate, timestamp));

        // We can only validate replicas when they are stored on master, so do not bother merging and take master replicas,
        // as there are three possible cases:
        // - the two lists are the same (and then there is no need to merge)
        // - there are more master replicas (again we can just take master replicas then)
        // - there are more Sequoia replicas (and this is a bug, so lets alert it, take master replicas and investigate later)
        if (validate) {
            ValidateSequoiaReplicaFetch(sequoiaChunkIds, masterReplicasInSequoiaSkin, sequoiaReplicasOrError, timestamp);

            // Put unapproved replicas back.
            for (const auto& [chunkId, unapprovedMasterReplicas] : unapprovedMasterReplicasInSequoiaSkin) {
                auto& masterReplicas = masterReplicasInSequoiaSkin[chunkId];
                masterReplicas.insert(masterReplicas.end(), unapprovedMasterReplicas.begin(), unapprovedMasterReplicas.end());
            }

            TChunkToStoredChunkReplicaList result;
            for (const auto& [chunkId, replicas] : masterReplicasInSequoiaSkin) {
                EmplaceOrCrash(result, chunkId, FilterAliveReplicas(replicas));
            }

            return result;
        }

        // This will fetch stored master replicas again.
        return CombineReplicas(chunks, sequoiaReplicasOrError, sequoiaChunkIds, includeNonOnlineReplicas);
    }

    void ValidateSequoiaReplicaFetch(
        const std::vector<TChunkId>& sequoiaChunkIds,
        THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>& masterReplicasInSequoiaSkin,
        TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicasOrError,
        TTimestamp commitTimestamp) const
    {
        VerifyPersistentStateRead();

        if (!sequoiaReplicasOrError.IsOK()) {
            YT_LOG_DEBUG(sequoiaReplicasOrError, "Cannot validate Sequoia replicas correspondence");
            return;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellTag = multicellManager->GetCellTag();

        auto allowExtraMasterReplicas = GetDynamicConfig()->AllowExtraMasterReplicasDuringValidation;

        auto& sequoiaReplicas = sequoiaReplicasOrError.Value();
        for (auto chunkId : sequoiaChunkIds) {
            if (CellTagFromId(chunkId) != cellTag) {
                YT_LOG_DEBUG("Skipping foreign chunk during Sequoia replica validation (ChunkId: %v)",
                    chunkId);
                continue;
            }

            auto masterIt = masterReplicasInSequoiaSkin.find(chunkId);
            if (masterIt == masterReplicasInSequoiaSkin.end()) {
                YT_LOG_ALERT("Chunk is not present in master replicas (ChunkId: %v)",
                chunkId);
                continue;
            }
            auto& masterReplicas = masterIt->second;
            auto sequoiaIt = sequoiaReplicas.find(chunkId);
            if (sequoiaIt == sequoiaReplicas.end()) {
                YT_LOG_ALERT("Chunk is not present in Sequoia replicas (ChunkId: %v)",
                chunkId);
                continue;
            }

            auto& sequoiaReplicas = sequoiaIt->second;
            YT_LOG_TRACE("Fetched Sequoia replicas (ChunkId: %v, SequoiaReplicas: %v)",
                chunkId,
                sequoiaReplicas);

            // Can contain same approved and unapproved replicas.
            SortUniqueBy(sequoiaReplicas, [] (const auto& replica) {
                return replica;
            });
            std::sort(masterReplicas.begin(), masterReplicas.end());

            YT_LOG_TRACE("Validating chunk replicas (ChunkId: %v, MasterReplicas: %v, SequoiaReplicas: %v, CommitTimestamp: %v)",
                chunkId,
                masterReplicas,
                sequoiaReplicas,
                commitTimestamp);

            if (masterReplicas != sequoiaReplicas) {
                if (!allowExtraMasterReplicas) {
                    YT_LOG_ALERT("Master and Sequoia replicas differ (ChunkId: %v, MasterReplicas: %v, SequoiaReplicas: %v, CommitTimestamp: %v)",
                        chunkId,
                        masterReplicas,
                        sequoiaReplicas,
                        commitTimestamp);
                } else {
                    for (auto sequoiaReplica : sequoiaReplicas) {
                        if (std::find(masterReplicas.begin(), masterReplicas.end(), sequoiaReplica) == masterReplicas.end()) {
                            YT_LOG_ALERT("Extra Sequoia replica found (ChunkId: %v, MasterReplicas: %v, ExtraSequoiaReplicas: %v, CommitTimestamp: %v)",
                                chunkId,
                                masterReplicas,
                                sequoiaReplica,
                                commitTimestamp);
                        }
                    }
                }
            }
        }
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetChunkReplicasAsync(
        TEphemeralObjectPtr<TChunk> chunk,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        auto chunkId = chunk->GetId();

        // This is so stupid.
        std::vector<TEphemeralObjectPtr<TChunk>> chunks;
        chunks.emplace_back(std::move(chunk));
        return GetChunkReplicasAsync(std::move(chunks), includeUnapproved)
            .Apply(BIND([chunkId] (const THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>>& sequoiaReplicas) {
                return GetOrCrash(sequoiaReplicas, chunkId)
                    .ValueOrThrow();
            }));
    }
    TFuture<THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>>> GetChunkReplicasAsync(
        std::vector<TEphemeralObjectPtr<TChunk>> chunks,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>> result;
        for (const auto& chunk : chunks) {
            // We may have non-online replicas here, they will be filtered in FilterAliveReplicas.
            auto masterReplicas = chunk->GetStoredReplicaList(/*includeNonOnlineReplicas*/ true);
            std::vector<TSequoiaChunkReplica> replicas;
            for (const auto& masterReplica : masterReplicas) {
                TSequoiaChunkReplica replica;
                replica.ChunkId = chunk->GetId();
                replica.ReplicaIndex = masterReplica.GetReplicaIndex();
                replica.NodeId = masterReplica.GetNodeId();
                replica.ReplicaState = masterReplica.GetReplicaState();
                if (auto* locationReplica = masterReplica.As<EStoredReplicaType::ChunkLocation>()) {
                    // NB: InvalidChunkLocationIndex will be used as default for offshore media.
                    replica.LocationIndex = locationReplica->GetChunkLocationIndex();
                }
                replicas.push_back(replica);
            }
            EmplaceOrCrash(result, chunk->GetId(), replicas);
        }

        auto sequoiaChunkIds = FilterSequoiaChunkIds(chunks);
        if (sequoiaChunkIds.empty()) {
            return MakeFuture(std::move(result));
        }

        return DoGetOnlySequoiaChunkReplicas(sequoiaChunkIds, includeUnapproved)
            .Apply(BIND([sequoiaChunkIds = std::move(sequoiaChunkIds), result = std::move(result)] (const TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicasOrError) mutable {
                if (!sequoiaReplicasOrError.IsOK()) {
                    for (auto chunkId : sequoiaChunkIds) {
                        EmplaceOrCrash(result, chunkId, TError(sequoiaReplicasOrError));
                    }
                    return result;
                }

                for (auto& [chunkId, replicas] : sequoiaReplicasOrError.Value()) {
                    auto it = GetIteratorOrCrash(result, chunkId);
                    auto& allReplicas = it->second.Value();
                    allReplicas.insert(allReplicas.end(), replicas.begin(), replicas.end());

                    SortUniqueBy(allReplicas, [] (const auto& replica) {
                        return replica;
                    });
                }
                return result;
            }));
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetApprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        TTimestamp timestamp) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.ChunkId, idMapping.StoredReplicas};
        return DoGetSequoiaReplicas(
            chunkIds,
            columnFilter,
            timestamp,
            nullptr,
            retriableErrorCodes,
            [] (const NRecords::TChunkReplicas& replicaRecord) {
                return replicaRecord.StoredReplicas;
            });
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetApprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        ISequoiaTransactionPtr transaction) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.ChunkId, idMapping.StoredReplicas};
        return DoGetSequoiaReplicas(
            chunkIds,
            columnFilter,
            TTimestamp(), // We do not need timestamp for transaction requests.
            transaction,
            {},
            [] (const NRecords::TChunkReplicas& replicaRecord) {
                return replicaRecord.StoredReplicas;
            });
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetUnapprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        TTimestamp timestamp) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return MakeFuture<std::vector<TSequoiaChunkReplica>>({});
        }

        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        std::vector<NRecords::TUnapprovedChunkReplicasKey> recordKeys;
        recordKeys.reserve(chunkIds.size());
        for (auto chunkId : chunkIds) {
            NRecords::TUnapprovedChunkReplicasKey chunkReplicasKey{
                .ChunkId = chunkId,
            };
            recordKeys.push_back(chunkReplicasKey);
        }

        const auto& idMapping = NRecords::TUnapprovedChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.ChunkId, idMapping.StoredReplicas, idMapping.ConfirmationTime};

        auto lastOKConfirmationTime = TInstant::Now() - Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ReplicaApproveTimeout;

        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->LookupRows<NRecords::TUnapprovedChunkReplicasKey>(recordKeys, columnFilter, timestamp)
            .Apply(BIND([retriableErrorCodes, lastOKConfirmationTime] (const TErrorOr<std::vector<std::optional<NRecords::TUnapprovedChunkReplicas>>>& replicaRecordsOrError) {
                ThrowOnSequoiaReplicasError(replicaRecordsOrError, retriableErrorCodes);
                const auto& replicaRecords = replicaRecordsOrError.ValueOrThrow();
                std::vector<std::optional<NRecords::TUnapprovedChunkReplicas>> okReplicaRecords;
                okReplicaRecords.reserve(replicaRecords.size());
                for (const auto& replicaRecord : replicaRecords) {
                    if (!replicaRecord) {
                        continue;
                    }
                    if (replicaRecord->ConfirmationTime < lastOKConfirmationTime) {
                        continue;
                    }
                    okReplicaRecords.push_back(replicaRecord);
                }
                return ParseReplicas(okReplicaRecords, [] (const NRecords::TUnapprovedChunkReplicas& replicaRecord) {
                    return replicaRecord.StoredReplicas;
                });
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    TFuture<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>> GetOnlySequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        bool includeUnapproved,
        bool force) const override
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        std::vector<TChunkId> sequoiaChunkIds;
        for (auto chunkId : chunkIds) {
            if (CanHaveSequoiaReplicas(chunkId)) {
                sequoiaChunkIds.push_back(chunkId);
            }
        }
        SortUnique(sequoiaChunkIds);

        return DoGetOnlySequoiaChunkReplicas(sequoiaChunkIds, includeUnapproved, force);
    }

    TFuture<std::vector<NRecords::TChunkRefreshQueue>> GetChunksToRefresh(int replicatorShard, int limit) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!GetDynamicConfig()->Enable) {
            return MakeFuture<std::vector<NRecords::TChunkRefreshQueue>>({});
        }
        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->SelectRows<NRecords::TChunkRefreshQueue>(
                Bootstrap_->GetCellTag(),
                {
                    .WhereConjuncts = {
                        Format("[$tablet_index] = %v", replicatorShard),
                    },
                    .Limit = limit,
                }
            )
            .Apply(BIND([retriableErrorCodes] (const TErrorOr<std::vector<NRecords::TChunkRefreshQueue>>& chunkRecordsOrError) {
                ThrowOnSequoiaReplicasError(chunkRecordsOrError, retriableErrorCodes);
                return chunkRecordsOrError.ValueOrThrow();
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

private:
    TBootstrap* const Bootstrap_;

    TFuture<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>> DoGetOnlySequoiaChunkReplicas(
        const std::vector<TChunkId>& sequoiaChunkIds,
        bool includeUnapproved,
        bool force = false,
        TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) const
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        THashMap<TChunkId, std::vector<TSequoiaChunkReplica>> result;
        for (auto chunkId : sequoiaChunkIds) {
            result[chunkId] = std::vector<TSequoiaChunkReplica>();
        }

        // Force is used for chunk attributes.
        if (!GetDynamicConfig()->FetchReplicasFromSequoia && !force) {
            return MakeFuture(std::move(result));
        }

        auto unapprovedReplicasFuture = includeUnapproved
            ? GetUnapprovedSequoiaChunkReplicas(sequoiaChunkIds, timestamp)
            : MakeFuture<std::vector<TSequoiaChunkReplica>>({});
        auto replicasFuture = GetApprovedSequoiaChunkReplicas(sequoiaChunkIds, timestamp);
        std::vector futures({replicasFuture, unapprovedReplicasFuture});
        return AllSucceeded(futures)
            .Apply(BIND([result = std::move(result)] (const std::vector<std::vector<TSequoiaChunkReplica>>& sequoiaReplicas) mutable {
                for (const auto& replicas : sequoiaReplicas) {
                    for (const auto& replica : replicas) {
                        auto chunkId = replica.ChunkId;
                        result[chunkId].push_back(replica);
                    }
                }

                return result;
            }));
    }

    std::vector<TChunkId> FilterSequoiaChunkIds(const std::vector<TEphemeralObjectPtr<TChunk>>& chunks) const
    {
        std::vector<TChunkId> sequoiaChunkIds;
        for (const auto& chunk : chunks) {
            if (CanHaveSequoiaReplicas(chunk->GetId())) {
                sequoiaChunkIds.push_back(chunk->GetId());
            }
        }

        SortUnique(sequoiaChunkIds);

        return sequoiaChunkIds;
    }

    const TDynamicSequoiaChunkReplicasConfigPtr& GetDynamicConfig() const
    {
        VerifyPersistentStateRead();

        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas;
    }

    TStoredChunkReplicaList FilterAliveReplicas(const std::vector<TSequoiaChunkReplica>& replicas) const override
    {
        VerifyPersistentStateRead();

        // COMPAT(grphil)
        auto includeNonOnlineReplicas = GetBootstrap()->GetConfigManager()->GetConfig()->ChunkManager->AlwaysFetchNonOnlineReplicas;

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        TStoredChunkReplicaList aliveReplicas;
        for (const auto& replica : replicas) {
            auto chunkId = replica.ChunkId;
            auto locationIndex = replica.LocationIndex;
            auto* location = dataNodeTracker->FindChunkLocationByIndex(locationIndex);
            if (!IsObjectAlive(location)) {
                YT_LOG_ERROR("Found Sequoia chunk replica with a non-existent location (ChunkId: %v, LocationIndex: %v)",
                    chunkId,
                    locationIndex);
                continue;
            }
            auto node = location->GetNode();
            if (!IsObjectAlive(node)) {
                YT_LOG_ERROR("Found Sequoia chunk replica with a location not bound to any node (ChunkId: %v, LocationIndex: %v)",
                    chunkId,
                    locationIndex);
                continue;
            }

            if (!includeNonOnlineReplicas && node->GetLocalState() != ENodeState::Online) {
                YT_LOG_TRACE("Found Sequoia chunk replica on non-online node, ignoring replica (ChunkId: %v, NodeAddress: %v, NodeState: %v)",
                    chunkId,
                    node->GetDefaultAddress(),
                    node->GetLocalState());
                continue;
            }
            aliveReplicas.emplace_back(TAugmentedStoredChunkReplicaPtr(location, replica.ReplicaIndex, replica.ReplicaState));
        }
        return aliveReplicas;
    }

    // COMPAT(grphil): includeNonOnlineReplicas
    TChunkToStoredChunkReplicaList CombineReplicas(
        const std::vector<TEphemeralObjectPtr<TChunk>>& chunks,
        const TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicasOrError,
        const std::vector<TChunkId>& sequoiaChunkIds,
        bool includeNonOnlineReplicas) const
    {
        VerifyPersistentStateRead();

        TChunkToStoredChunkReplicaList result;
        if (!sequoiaReplicasOrError.IsOK()) {
            for (auto chunkId : sequoiaChunkIds) {
                EmplaceOrCrash(result, chunkId, TError(sequoiaReplicasOrError));
            }
        } else {
            for (auto& [chunkId, replicas] : sequoiaReplicasOrError.Value()) {
                EmplaceOrCrash(result, chunkId, FilterAliveReplicas(replicas));
            }
        }

        for (const auto& chunk : chunks) {
            auto filteredMasterReplicas = chunk->GetStoredReplicaList(includeNonOnlineReplicas);
            auto [it, inserted] = result.emplace(chunk->GetId(), filteredMasterReplicas);

            if (inserted) {
                continue;
            }

            if (!it->second.IsOK()) {
                continue;
            }

            auto& replicas = it->second.Value();
            replicas.insert(replicas.end(), filteredMasterReplicas.begin(), filteredMasterReplicas.end());

            SortUniqueBy(replicas, [] (const auto& replica) {
                auto replicaIndex = replica.GetReplicaIndex();
                auto nodeId = replica.GetNodeId();
                auto locationUuid = replica.GetLocationUuid();
                return std::tuple(replicaIndex, nodeId, locationUuid);
            });
        }

        return result;
    }

    TFuture<std::vector<TSequoiaChunkReplica>> DoGetSequoiaReplicas(
        const std::vector<TChunkId>& chunkIds,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp,
        ISequoiaTransactionPtr transaction,
        const std::vector<TErrorCode> retriableErrorCodes,
        const std::function<NYson::TYsonString(const NRecords::TChunkReplicas&)>& extractReplicas) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        std::vector<NRecords::TChunkReplicasKey> keys;
        for (auto chunkId : chunkIds) {
            NRecords::TChunkReplicasKey chunkReplicasKey{
                .ChunkId = chunkId,
            };
            keys.push_back(chunkReplicasKey);
        }

        TFuture<std::vector<std::optional<NRecords::TChunkReplicas>>> replicaRecordsFuture;
        if (!transaction) {
            replicaRecordsFuture = Bootstrap_
                ->GetSequoiaConnection()
                ->CreateClient(NRpc::GetRootAuthenticationIdentity())
                ->LookupRows<NRecords::TChunkReplicasKey>(keys, columnFilter, timestamp);
        } else {
            replicaRecordsFuture = transaction->LookupRows<NRecords::TChunkReplicasKey>(keys, columnFilter);
        }

        return replicaRecordsFuture
            .Apply(BIND([extractReplicas, retriableErrorCodes] (const TErrorOr<std::vector<std::optional<NRecords::TChunkReplicas>>>& replicaRecordsOrError) {
                ThrowOnSequoiaReplicasError(replicaRecordsOrError, retriableErrorCodes);
                const auto& replicas = replicaRecordsOrError.ValueOrThrow();
                return ParseReplicas(replicas, extractReplicas);
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    TFuture<std::vector<TSequoiaChunkReplica>> DoGetSequoiaLastSeenReplicas(TChunkId chunkId) const
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.ChunkId, idMapping.LastSeenReplicas};
        return DoGetSequoiaReplicas(
            {chunkId},
            columnFilter,
            NTransactionClient::SyncLastCommittedTimestamp,
            /*transaction*/ nullptr,
            retriableErrorCodes,
            [] (const NRecords::TChunkReplicas& replicaRecord) {
                return replicaRecord.LastSeenReplicas;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaFetcherPtr CreateChunkReplicaFetcher(NCellMaster::TBootstrap* bootstrap)
{
    return New<TChunkReplicaFetcher>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

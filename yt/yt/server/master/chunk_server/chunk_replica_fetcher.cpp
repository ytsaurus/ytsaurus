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
                    .ReplicaState = parsedReplica.ReplicaState,
                });
            });
    }

    return replicas;
}

void KeepFirstStateOfDuplicatedReplicas(std::vector<TSequoiaChunkReplica>& replicas)
{
    StableSortUniqueBy(
        replicas,
        [] (const auto& replica) {
            return std::tie(replica.ChunkId, replica.ReplicaIndex, replica.NodeId, replica.LocationIndex);
        });
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TReplicaFetchState)

struct TReplicaFetchState
    : public TRefCounted
{
    TDynamicSequoiaChunkReplicasConfigPtr ConfigForValidation;

    THashMap<TChunkId, std::vector<TSequoiaChunkReplica>> MasterReplicas;
    THashMap<TChunkId, std::vector<TSequoiaChunkReplica>> ApprovedMasterReplicasForPendingValidationChunks;

    std::vector<TChunkId> ChunkIdsToFetchReplicasFromSequoia;
    std::vector<TChunkId> ChunkIdsToFetchUnapprovedReplicasFromSequoia;

    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
};

DEFINE_REFCOUNTED_TYPE(TReplicaFetchState);

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaFetcher
    : public IChunkReplicaFetcher
{
public:
    explicit TChunkReplicaFetcher(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

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
                YT_LOG_DEBUG("Found Sequoia chunk replica with a non-existent location (ChunkId: %v, LocationIndex: %v)",
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

        return GetSequoiaLocationReplicasWithoutSequoiaChecks(nodeId, locationIndex)
            .Apply(BIND([retriableErrorCodes] (const TErrorOr<std::vector<NRecords::TLocationReplicas>>& result) {
                ThrowOnSequoiaReplicasError(result, retriableErrorCodes);
                return result;
            }));
    }

    TFuture<std::vector<NRecords::TLocationReplicas>> GetSequoiaLocationReplicasWithoutSequoiaChecks(
        TNodeId nodeId,
        NNodeTrackerClient::TChunkLocationIndex locationIndex) const override
    {
        return Bootstrap_
            ->GetSequoiaConnection()
            ->CreateClient(NRpc::GetRootAuthenticationIdentity())
            ->SelectRows<NRecords::TLocationReplicas>(BuildSelectLocationSequoiaReplicasQuery(
                Bootstrap_->GetCellTag(),
                nodeId,
                locationIndex
            ));
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

        auto chunkSequoiaConfig = GetChunkSequoiaConfig(chunkId, GetDynamicConfig());
        if (!chunkSequoiaConfig.FetchReplicasFromSequoia) {
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

        auto state = New<TReplicaFetchState>();
        FilterChunkIdsToFetchFromSequoia(
            chunks | std::views::transform([] (const auto& chunk) {
                return chunk->GetId();
            }),
            state,
            includeUnapproved,
            /*disableValidation*/ false,
            /*force*/ false);

        // Fastpath.
        if (state->ChunkIdsToFetchReplicasFromSequoia.empty()) {
            TChunkToStoredChunkReplicaList result;
            for (const auto& chunk : chunks) {
                result.emplace(chunk->GetId(), chunk->GetStoredReplicaList(/*includeNonOnlineReplicas*/ false));
            }
            return result;
        }

        // In previous version of Sequoia replica fetcher, we had two scenarios:
        // - Fetch with validation:
        // In that case all replicas were fetched from master before Sequoia fetch.
        // - Fetch without validation:
        // In that case all replicas were fetched from master after Sequoia fetch.
        // For better consistency, now all master replicas are fetched before Sequoia fetch.
        FetchReplicasFromMaster(chunks, state);
        FinishReplicaFetchStateInitialization(state);

        auto replicas = WaitForFast(FetchReplicasFromSequoia(state)
            .Apply(BIND([state, this, this_ = MakeStrong(this)] (const TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicas) {
                return CombineAndValidateReplicas(state, sequoiaReplicas);
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())))
            .ValueOrThrow();

        TChunkToStoredChunkReplicaList result;
        for (const auto& [chunkId, replicasOrError] : replicas) {
            if (replicasOrError.IsOK()) {
                EmplaceOrCrash(result, chunkId, FilterAliveReplicas(replicasOrError.Value()));
            } else {
                EmplaceOrCrash(result, chunkId, TError(replicasOrError));
            }
        }
        return result;
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

        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        auto state = New<TReplicaFetchState>();
        FilterChunkIdsToFetchFromSequoia(
            chunks | std::views::transform([] (const auto& chunk) {
                return chunk->GetId();
            }),
            state,
            includeUnapproved,
            /*disableValidation*/ false,
            /*force*/ false);

        FetchReplicasFromMaster(chunks, state);

        // Fastpath.
        if (state->ChunkIdsToFetchReplicasFromSequoia.empty()) {
            THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>> result;
            for (auto&& [chunkId, replicas] : std::move(state->MasterReplicas)) {
                EmplaceOrCrash(result, chunkId, std::move(replicas));
            }
            return MakeFuture(std::move(result));
        }

        FinishReplicaFetchStateInitialization(state);

        // In previous version of Sequoia replica fetcher no validation was performed during async replica fetch.
        // Now we validate all Sequoia replicas that require validation the same way as it is done for GetChunkReplicas.
        return FetchReplicasFromSequoia(state)
            .Apply(BIND([state, this, this_ = MakeStrong(this)] (const TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicas) {
                return CombineAndValidateReplicas(state, sequoiaReplicas);
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
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

        auto state = New<TReplicaFetchState>();
        FilterChunkIdsToFetchFromSequoia(
            chunkIds,
            state,
            includeUnapproved,
            /*disableValidation*/ true,
            force);

        FinishReplicaFetchStateInitialization(state);

        // We keep the old behaviour here that for non-Sequoia chunk ids we will have no entry in result hash map.
        return FetchReplicasFromSequoia(state);
    }

    TFuture<std::vector<NRecords::TChunkRefreshQueue>> GetChunksToRefresh(int replicatorShard, int limit) const override
    {
        YT_VERIFY(!HasMutationContext());

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
            );
    }

private:
    TBootstrap* const Bootstrap_;

    void FilterChunkIdsToFetchFromSequoia(
        const auto& chunkIds,
        TReplicaFetchStatePtr state,
        bool includeUnapproved,
        bool disableValidation,
        bool force) const
    {
        VerifyPersistentStateRead();

        auto config = GetDynamicConfig();
        if (!config->Enable) {
            return;
        }

        for (const auto& chunkId : chunkIds) {
            auto chunkSequoiaConfig = GetChunkSequoiaConfig(chunkId, config);

            if (chunkSequoiaConfig.StoreInSequoia &&
                (chunkSequoiaConfig.FetchReplicasFromSequoia || force))
            {
                state->ChunkIdsToFetchReplicasFromSequoia.push_back(chunkId);
                if (includeUnapproved &&
                    (disableValidation || !chunkSequoiaConfig.ValidateSequoiaReplicasFetch))
                {
                    // If validation is enabled, we will eventually return replicas that are stored in master.
                    // We will validate only approved replicas, so we do not need to fetch unapproved replicas.
                    state->ChunkIdsToFetchUnapprovedReplicasFromSequoia.push_back(chunkId);
                }
            }
        }

        SortUnique(state->ChunkIdsToFetchReplicasFromSequoia);
        SortUnique(state->ChunkIdsToFetchUnapprovedReplicasFromSequoia);
    }

    void FetchReplicasFromMaster(
        const std::vector<TEphemeralObjectPtr<TChunk>>& chunks,
        TReplicaFetchStatePtr state) const
    {
        VerifyPersistentStateRead();

        // For every chunk we fetch all master replicas to state->MasterReplicas;
        // We always include unapproved master-stored replicas.

        // For chunks that require Sequoia fetch and validation we add approved replicas to state->ApprovedMasterReplicasForPendingValidationChunks

        for (const auto& chunk : chunks) {
            auto convertToSequoiaReplica = [&] (TAugmentedStoredChunkReplicaPtr masterReplica) {
                TSequoiaChunkReplica replica{
                    .ChunkId = chunk->GetId(),
                    .ReplicaIndex = masterReplica.GetReplicaIndex(),
                    .NodeId = masterReplica.GetNodeId(),
                    .ReplicaState = masterReplica.GetReplicaState()
                };
                if (auto* locationReplica = masterReplica.As<EStoredReplicaType::ChunkLocation>()) {
                    // NB: InvalidChunkLocationIndex will be used as default for offshore media.
                    replica.LocationIndex = locationReplica->GetChunkLocationIndex();
                }
                return replica;
            };

            // Chunks can have duplicates.
            if (state->MasterReplicas.contains(chunk->GetId())) {
                continue;
            }

            auto& replicas = state->MasterReplicas[chunk->GetId()];
            for (const auto& masterReplica : chunk->GetStoredReplicaList(/*includeNonOnlineReplicas*/ false)) {
                replicas.push_back(convertToSequoiaReplica(masterReplica));
            }

            YT_LOG_TRACE("Fetched master replicas for chunk (ChunkId: %v, Replicas: %v)",
                chunk->GetId(),
                replicas);

            auto chunkSequoiaConfig = GetChunkSequoiaConfig(chunk->GetId(), GetDynamicConfig());
            if (chunkSequoiaConfig.ValidateSequoiaReplicasFetch) {
                auto& approvedReplicas = state->ApprovedMasterReplicasForPendingValidationChunks[chunk->GetId()];
                // Chunks can have duplicates.
                approvedReplicas.clear();
                for (const auto& masterReplica : chunk->GetStoredReplicaList(/*includeNonOnlineReplicas*/ false)) {
                    if (auto* locationReplica = masterReplica.As<EStoredReplicaType::ChunkLocation>()) {
                        auto* location = locationReplica->AsChunkLocationPtr();
                        if (location->HasUnapprovedReplica(TChunkPtrWithReplicaIndex(chunk.Get(), masterReplica.GetReplicaIndex()))) {
                            continue;
                        }
                    }
                    approvedReplicas.push_back(convertToSequoiaReplica(masterReplica));
                }

                YT_LOG_TRACE("Fetched approved master replicas for chunk pending Sequoia fetch and validation (ChunkId: %v, ApprovedMasterReplicas: %v)",
                    chunk->GetId(),
                    approvedReplicas);
            }
        }
    }

    void FinishReplicaFetchStateInitialization(TReplicaFetchStatePtr state) const
    {
        if (!state->ApprovedMasterReplicasForPendingValidationChunks.empty()) {
            // If ApprovedMasterReplicasForPendingValidationChunks is not empty, we need to validate replicas for some chunks.
            // In this case we need Sequoia state at the time of last coordinator commit timestamp.

            const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
            state->Timestamp = transactionSupervisor->GetLastCoordinatorCommitTimestamp();

            if (state->Timestamp == NTransactionClient::NullTimestamp) {
                state->Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
            }

            // Validation will be executed in separate thread, so we need to copy Sequoia replicas config.
            state->ConfigForValidation = CopySequoiaChunkReplicasConfig(GetDynamicConfig());
        }
        // If no validation is needed, the default value of NTransactionClient::SyncLastCommittedTimestamp will be used for timestamp.
    }

    TFuture<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>> FetchReplicasFromSequoia(
        TReplicaFetchStatePtr state) const
    {
        YT_VERIFY(!HasMutationContext());
        VerifyPersistentStateRead();

        THashMap<TChunkId, std::vector<TSequoiaChunkReplica>> result;
        for (auto chunkId : state->ChunkIdsToFetchReplicasFromSequoia) {
            // Chunks may have no Sequoia replicas, so we should initialize result with empty list initially.
            result[chunkId] = std::vector<TSequoiaChunkReplica>();
        }

        if (state->ChunkIdsToFetchReplicasFromSequoia.empty()) {
            return MakeFuture(result);
        }

        auto hasUnapprovedReplicas = !state->ChunkIdsToFetchUnapprovedReplicasFromSequoia.empty();

        auto unapprovedReplicasFuture = hasUnapprovedReplicas
            ? GetUnapprovedSequoiaChunkReplicas(state->ChunkIdsToFetchUnapprovedReplicasFromSequoia, state->Timestamp)
            : MakeFuture<std::vector<TSequoiaChunkReplica>>({});
        auto replicasFuture = GetApprovedSequoiaChunkReplicas(state->ChunkIdsToFetchReplicasFromSequoia, state->Timestamp);

        // It is important to keep approved replicas before unapproved.
        std::vector futures({replicasFuture, unapprovedReplicasFuture});
        return AllSucceeded(futures)
            .Apply(BIND([hasUnapprovedReplicas, result = std::move(result)] (const std::vector<std::vector<TSequoiaChunkReplica>>& sequoiaReplicas) mutable {
                for (const auto& replicas : sequoiaReplicas) {
                    for (const auto& replica : replicas) {
                        auto chunkId = replica.ChunkId;
                        result[chunkId].push_back(replica);
                    }
                }

                if (hasUnapprovedReplicas) {
                    for (auto& [chunkId, replicas] : result) {
                        // Unapproved replicas are added later than approved replicas.
                        KeepFirstStateOfDuplicatedReplicas(replicas);
                    }
                }

                return result;
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>> CombineAndValidateReplicas(
        TReplicaFetchStatePtr state,
        const TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicasOrError) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>> result;

        for (auto&& [chunkId, replicas] : std::move(state->MasterReplicas)) {
            EmplaceOrCrash(result, chunkId, std::move(replicas));
        }

        if (!sequoiaReplicasOrError.IsOK()) {
            for (const auto& chunkId : state->ChunkIdsToFetchReplicasFromSequoia) {
                result[chunkId] = TError(sequoiaReplicasOrError);
            }
            return result;
        }

        for (const auto& [chunkId, sequoiaReplicas] : sequoiaReplicasOrError.Value()) {
            auto approvedReplicasIt = state->ApprovedMasterReplicasForPendingValidationChunks.find(chunkId);
            if (approvedReplicasIt != state->ApprovedMasterReplicasForPendingValidationChunks.end()) {
                ValidateSequoiaReplicaFetch(state, chunkId, sequoiaReplicas, approvedReplicasIt->second);
                // We can only validate replicas when they are stored on master, so do not bother merging and take master replicas,
                // as there are three possible cases:
                // - the two lists are the same (and then there is no need to merge)
                // - there are more master replicas (again we can just take master replicas then)
                // - there are more Sequoia replicas (and this is a bug, so lets alert it, take master replicas and investigate later)
                continue;
            }

            auto replicasIt = result.find(chunkId);
            YT_LOG_ALERT_AND_THROW_IF(
                replicasIt == result.end(),
                "Master replicas were not fetched for chunk during combined master and Sequoia replica fetch (ChunkId: %v)",
                chunkId);

            auto& replicas = replicasIt->second.Value();
            replicas.insert(replicas.end(), sequoiaReplicas.begin(), sequoiaReplicas.end());
            KeepFirstStateOfDuplicatedReplicas(replicas);
        }
        return result;
    }

    void ValidateSequoiaReplicaFetch(
        TReplicaFetchStatePtr state,
        TChunkId chunkId,
        std::vector<TSequoiaChunkReplica> sequoiaReplicas,
        std::vector<TSequoiaChunkReplica> masterReplicas) const
    {
        YT_LOG_ALERT_AND_THROW_IF(
            !state->ConfigForValidation,
            "No Sequoia replicas config is found during Sequoia replica fetch validation");

        auto chunkSequoiaConfig = GetChunkSequoiaConfig(chunkId, state->ConfigForValidation);

        SortUnique(sequoiaReplicas);
        SortUnique(masterReplicas);

        YT_LOG_TRACE("Validating chunk replicas (ChunkId: %v, MasterReplicas: %v, SequoiaReplicas: %v, CommitTimestamp: %v)",
            chunkId,
            masterReplicas,
            sequoiaReplicas,
            state->Timestamp);

        if (masterReplicas != sequoiaReplicas) {
            if (!chunkSequoiaConfig.AllowExtraMasterReplicasDuringValidation) {
                YT_LOG_ALERT("Master and Sequoia replicas differ (ChunkId: %v, MasterReplicas: %v, SequoiaReplicas: %v, CommitTimestamp: %v)",
                    chunkId,
                    masterReplicas,
                    sequoiaReplicas,
                    state->Timestamp);
            } else {
                for (auto sequoiaReplica : sequoiaReplicas) {
                    if (std::find(masterReplicas.begin(), masterReplicas.end(), sequoiaReplica) == masterReplicas.end()) {
                        YT_LOG_ALERT("Extra Sequoia replica found (ChunkId: %v, MasterReplicas: %v, ExtraSequoiaReplicas: %v, CommitTimestamp: %v)",
                            chunkId,
                            masterReplicas,
                            sequoiaReplica,
                            state->Timestamp);
                    }
                }
            }
        }
    }

    const TDynamicSequoiaChunkReplicasConfigPtr& GetDynamicConfig() const
    {
        VerifyPersistentStateRead();

        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas;
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
            auto chunkReplicasKey = BuildChunkReplicasRecordKey(chunkId);
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

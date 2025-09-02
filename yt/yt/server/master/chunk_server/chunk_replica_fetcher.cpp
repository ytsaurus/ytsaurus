#include "chunk_replica_fetcher.h"
#include "public.h"
#include "private.h"
#include "chunk.h"
#include "chunk_location.h"
#include "config.h"
#include "data_node_tracker.h"
#include "domestic_medium.h"
#include "chunk_manager.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

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
            ->GetSequoiaClient()
            ->SelectRows<NRecords::TLocationReplicas>({
                .WhereConjuncts = {
                    Format("cell_tag = %v", Bootstrap_->GetCellTag()),
                    Format("node_id = %v", nodeId),
                    Format("location_index = %v", locationIndex),
                }
            }).Apply(BIND([retriableErrorCodes] (const TErrorOr<std::vector<NRecords::TLocationReplicas>>& result) {
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
            ->GetSequoiaClient()
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

    TErrorOr<TChunkLocationPtrWithReplicaInfoList> GetChunkReplicas(
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
    TChunkToLocationPtrWithReplicaInfoList GetChunkReplicas(
        const std::vector<TEphemeralObjectPtr<TChunk>>& chunks,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());

        auto sequoiaChunkIds = FilterSequoiaChunkIds(chunks);
        auto sequoiaReplicasOrError = WaitForFast(DoGetOnlySequoiaChunkReplicas(sequoiaChunkIds, includeUnapproved));
        return GetReplicas(chunks, sequoiaReplicasOrError, sequoiaChunkIds);
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
            auto masterReplicas = chunk->StoredReplicas();
            std::vector<TSequoiaChunkReplica> replicas;
            for (const auto& masterReplica : masterReplicas) {
                auto location = masterReplica.GetPtr();
                TSequoiaChunkReplica replica;
                replica.ChunkId = chunk->GetId();
                replica.ReplicaIndex = masterReplica.GetReplicaIndex();
                replica.NodeId = location->GetNode()->GetId();
                replica.LocationIndex = location->GetIndex();
                replica.ReplicaState = masterReplica.GetReplicaState();
                replicas.push_back(replica);
            }
            TChunkLocationPtrWithReplicaInfoList replicaList(masterReplicas.begin(), masterReplicas.end());
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

    TFuture<std::vector<TSequoiaChunkReplica>> GetApprovedSequoiaChunkReplicas(const std::vector<TChunkId>& chunkIds) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.ChunkId, idMapping.StoredReplicas};
        return DoGetSequoiaReplicas(chunkIds, columnFilter, [] (const NRecords::TChunkReplicas& replicaRecord) {
            return replicaRecord.StoredReplicas;
        });
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetUnapprovedSequoiaChunkReplicas(const std::vector<TChunkId>& chunkIds) const override
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
            ->GetSequoiaClient()
            ->LookupRows<NRecords::TUnapprovedChunkReplicasKey>(recordKeys, columnFilter)
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
            ->GetSequoiaClient()
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
        bool force = false) const
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
            ? GetUnapprovedSequoiaChunkReplicas(sequoiaChunkIds)
            : MakeFuture<std::vector<TSequoiaChunkReplica>>({});
        auto replicasFuture = GetApprovedSequoiaChunkReplicas(sequoiaChunkIds);
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

    TChunkLocationPtrWithReplicaInfoList FilterAliveReplicas(const std::vector<TSequoiaChunkReplica>& replicas) const override
    {
        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        TChunkLocationPtrWithReplicaInfoList aliveReplicas;
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

            aliveReplicas.emplace_back(location, replica.ReplicaIndex, replica.ReplicaState);
        }
        return aliveReplicas;
    }

    TChunkToLocationPtrWithReplicaInfoList GetReplicas(
        const std::vector<TEphemeralObjectPtr<TChunk>>& chunks,
        const TErrorOr<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>>& sequoiaReplicasOrError,
        const std::vector<TChunkId>& sequoiaChunkIds) const
    {
        VerifyPersistentStateRead();

        TChunkToLocationPtrWithReplicaInfoList result;
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
            auto masterReplicas = chunk->StoredReplicas();
            TChunkLocationPtrWithReplicaInfoList replicaList(masterReplicas.begin(), masterReplicas.end());
            auto [it, inserted] = result.emplace(chunk->GetId(), replicaList);

            if (inserted) {
                continue;
            }

            if (!it->second.IsOK()) {
                continue;
            }

            auto& replicas = it->second.Value();
            replicas.insert(replicas.end(), masterReplicas.begin(), masterReplicas.end());

            SortUniqueBy(replicas, [] (const auto& replica) {
                auto location = replica.GetPtr();

                auto replicaIndex = replica.GetReplicaIndex();
                auto nodeId = location->GetNode()->GetId();
                auto locationUuid = location->GetUuid();
                return std::tuple(replicaIndex, nodeId, locationUuid);
            });
        }

        return result;
    }

    TFuture<std::vector<TSequoiaChunkReplica>> DoGetSequoiaReplicas(
        const std::vector<TChunkId>& chunkIds,
        const TColumnFilter& columnFilter,
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

        const auto& config = GetDynamicConfig();
        const auto& retriableErrorCodes = config->RetriableErrorCodes;

        return Bootstrap_
            ->GetSequoiaClient()
            ->LookupRows<NRecords::TChunkReplicasKey>(keys, columnFilter)
            .Apply(BIND([extractReplicas, retriableErrorCodes] (const TErrorOr<std::vector<std::optional<NRecords::TChunkReplicas>>>& replicaRecordsOrError) {
                ThrowOnSequoiaReplicasError(replicaRecordsOrError, retriableErrorCodes);
                const auto& replicas = replicaRecordsOrError.ValueOrThrow();
                return ParseReplicas(replicas, extractReplicas);
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    TFuture<std::vector<TSequoiaChunkReplica>> DoGetSequoiaLastSeenReplicas(TChunkId chunkId) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{idMapping.ChunkId, idMapping.LastSeenReplicas};
        return DoGetSequoiaReplicas({chunkId}, columnFilter, [] (const NRecords::TChunkReplicas& replicaRecord) {
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

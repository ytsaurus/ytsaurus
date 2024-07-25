#include "chunk_replica_fetcher.h"
#include "public.h"
#include "private.h"
#include "chunk.h"
#include "chunk_location.h"
#include "config.h"
#include "data_node_tracker.h"
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

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkServer {

using namespace NYson;
using namespace NCellMaster;
using namespace NSequoiaClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TSequoiaChunkReplica ParseReplica(TYsonPullParserCursor* cursor, TChunkId chunkId)
{
    TSequoiaChunkReplica chunkReplica;
    chunkReplica.ChunkId = chunkId;

    auto consume = [&] (EYsonItemType type, const auto& fillField) {
        const auto& current = cursor->GetCurrent();
        if (current.GetType() != type) {
            THROW_ERROR_EXCEPTION("Invalid YSON item type while parsing Sequoia replicas: expected %Qlv, actual %Qlv",
                type,
                current.GetType());
        }
        fillField(current);
        cursor->Next();
    };

    consume(EYsonItemType::BeginList, [] (const TYsonItem&) {});
    consume(EYsonItemType::StringValue, [&] (const TYsonItem& current) {
        chunkReplica.LocationUuid = TGuid::FromString(current.UncheckedAsString());
    });
    consume(EYsonItemType::Int64Value, [&] (const TYsonItem& current) {
        chunkReplica.ReplicaIndex = current.UncheckedAsInt64();
    });
    consume(EYsonItemType::Uint64Value, [&] (const TYsonItem& current) {
        chunkReplica.NodeId = TNodeId(current.UncheckedAsUint64());
    });
    consume(EYsonItemType::EndList, [] (const TYsonItem&) {});

    return chunkReplica;
}

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

        TMemoryInput input(extractedReplicas.AsStringBuf().data(), extractedReplicas.AsStringBuf().size());
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            replicas.push_back(ParseReplica(cursor, chunkId));
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

    bool CanHaveSequoiaReplicas(TRealChunkLocation* location) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        if (!IsObjectAlive(location)) {
            return false;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        int mediumIndex = location->GetEffectiveMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || !medium->IsDomestic()) {
            return false;
        }

        auto domesticMedium = medium->AsDomestic();
        if (!domesticMedium->GetEnableSequoiaReplicas()) {
            return false;
        }

        return true;
    }

    bool CanHaveSequoiaReplicas(TChunkId chunkId, int probability) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (IsJournalChunkId(chunkId)) {
            return false;
        }

        return static_cast<int>(EntropyFromId(chunkId) % 100) < probability;
    }

    bool CanHaveSequoiaReplicas(TChunkId chunkId) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return false;
        }

        auto probability = config->ReplicasPercentage;
        return CanHaveSequoiaReplicas(chunkId, probability);
    }

    bool IsSequoiaChunkReplica(TChunkId chunkId, TChunkLocationUuid locationUuid) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        auto* location = dataNodeTracker->FindChunkLocationByUuid(locationUuid);
        if (!IsObjectAlive(location)) {
            return false;
        }

        return IsSequoiaChunkReplica(chunkId, location);
    }

    bool IsSequoiaChunkReplica(TChunkId chunkId, TRealChunkLocation* location) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        if (!CanHaveSequoiaReplicas(chunkId)) {
            return false;
        }

        return CanHaveSequoiaReplicas(location);
    }


    TFuture<std::vector<NRecords::TLocationReplicas>> GetSequoiaLocationReplicas(
        TNodeId nodeId,
        TChunkLocationUuid locationUuid) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return MakeFuture<std::vector<NRecords::TLocationReplicas>>({});
        }

        return Bootstrap_
            ->GetSequoiaClient()
            ->SelectRows<NRecords::TLocationReplicas>({
                .WhereConjuncts = {
                    Format("cell_tag = %v", Bootstrap_->GetCellTag()),
                    Format("node_id = %v", nodeId),
                    Format("location_uuid = %Qv", locationUuid),
                }
            });
    }

    TFuture<std::vector<NRecords::TLocationReplicas>> GetSequoiaNodeReplicas(TNodeId nodeId) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return MakeFuture<std::vector<NRecords::TLocationReplicas>>({});
        }

        return Bootstrap_
            ->GetSequoiaClient()
            ->SelectRows<NRecords::TLocationReplicas>({
                .WhereConjuncts = {
                    Format("cell_tag = %v", Bootstrap_->GetCellTag()),
                    Format("node_id = %v", nodeId),
                }
            });
    }

    TFuture<std::vector<TNodeId>> GetLastSeenReplicas(const TEphemeralObjectPtr<TChunk>& chunk) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

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
        Bootstrap_->VerifyPersistentStateRead();

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

    TFuture<TChunkLocationPtrWithReplicaInfoList> GetChunkReplicasAsync(
        TEphemeralObjectPtr<TChunk> chunk,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        auto chunkId = chunk->GetId();

        // This is so stupid.
        std::vector<TEphemeralObjectPtr<TChunk>> chunks;
        chunks.emplace_back(std::move(chunk));
        return GetChunkReplicasAsync(std::move(chunks), includeUnapproved)
            .Apply(BIND([chunkId] (const TChunkToLocationPtrWithReplicaInfoList& sequoiaReplicas) {
                return GetOrCrash(sequoiaReplicas, chunkId)
                    .ValueOrThrow();
            }));
    }
    TFuture<TChunkToLocationPtrWithReplicaInfoList> GetChunkReplicasAsync(
        std::vector<TEphemeralObjectPtr<TChunk>> chunks,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        auto sequoiaChunkIds = FilterSequoiaChunkIds(chunks);
        if (sequoiaChunkIds.empty()) {
            TChunkToLocationPtrWithReplicaInfoList result;
            for (const auto& chunk : chunks) {
                auto masterReplicas = chunk->StoredReplicas();
                TChunkLocationPtrWithReplicaInfoList replicaList(masterReplicas.begin(), masterReplicas.end());
                EmplaceOrCrash(result, chunk->GetId(), replicaList);
            }
            return MakeFuture(std::move(result));
        }

        return DoGetOnlySequoiaChunkReplicas(sequoiaChunkIds, includeUnapproved)
            .Apply(BIND([sequoiaChunkIds = std::move(sequoiaChunkIds), chunks = std::move(chunks), this, this_ = MakeStrong(this)] (const TErrorOr<THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList>>& sequoiaReplicasOrError) mutable {
                return GetReplicas(
                    chunks,
                    sequoiaReplicasOrError,
                    sequoiaChunkIds);
            })
            .AsyncViaGuarded(
                Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ChunkManager),
                TError("Error fetching Sequoia replicas")));
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetSequoiaChunkReplicas(const std::vector<TChunkId>& chunkIds) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        YT_VERIFY(idMapping.ChunkId && idMapping.StoredReplicas);
        TColumnFilter columnFilter{*idMapping.ChunkId, *idMapping.StoredReplicas};
        return DoGetSequoiaReplicas(chunkIds, columnFilter, [] (const NRecords::TChunkReplicas& replicaRecord) {
            return replicaRecord.StoredReplicas;
        });
    }

    TFuture<THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList>> GetOnlySequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        bool includeUnapproved) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        std::vector<TChunkId> sequoiaChunkIds;
        for (auto chunkId : chunkIds) {
            if (CanHaveSequoiaReplicas(chunkId)) {
                sequoiaChunkIds.push_back(chunkId);
            }
        }
        SortUnique(sequoiaChunkIds);

        return DoGetOnlySequoiaChunkReplicas(sequoiaChunkIds, includeUnapproved);
    }

    TFuture<std::vector<TSequoiaChunkReplica>> GetUnapprovedSequoiaChunkReplicas(const std::vector<TChunkId>& chunkIds) const override
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        if (!GetDynamicConfig()->Enable) {
            return MakeFuture<std::vector<TSequoiaChunkReplica>>({});
        }

        std::vector<NRecords::TUnapprovedChunkReplicasKey> keys;
        for (auto chunkId : chunkIds) {
            NRecords::TUnapprovedChunkReplicasKey chunkReplicasKey{
                .ChunkId = chunkId,
            };
            keys.push_back(chunkReplicasKey);
        }

        auto lastOKConfirmationTime = TInstant::Now() - Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ReplicaApproveTimeout;
        const auto& idMapping = NRecords::TUnapprovedChunkReplicasDescriptor::Get()->GetIdMapping();
        TColumnFilter columnFilter{*idMapping.ChunkId, *idMapping.StoredReplicas, *idMapping.ConfirmationTime};
        return Bootstrap_
            ->GetSequoiaClient()
            ->LookupRows<NRecords::TUnapprovedChunkReplicasKey>(keys, columnFilter)
            .Apply(BIND([lastOKConfirmationTime] (const std::vector<std::optional<NRecords::TUnapprovedChunkReplicas>>& replicaRecords) {
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

private:
    TBootstrap* const Bootstrap_;

    TFuture<THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList>> DoGetOnlySequoiaChunkReplicas(
        const std::vector<TChunkId>& sequoiaChunkIds,
        bool includeUnapproved) const
    {
        YT_VERIFY(!HasMutationContext());
        Bootstrap_->VerifyPersistentStateRead();

        THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList> result;
        for (auto chunkId : sequoiaChunkIds) {
            result[chunkId] = TChunkLocationPtrWithReplicaInfoList();
        }

        if (!GetDynamicConfig()->FetchReplicasFromSequoia) {
            return MakeFuture(std::move(result));
        }

        auto unapprovedReplicasFuture = includeUnapproved
            ? GetUnapprovedSequoiaChunkReplicas(sequoiaChunkIds)
            : MakeFuture<std::vector<TSequoiaChunkReplica>>({});
        auto replicasFuture = GetSequoiaChunkReplicas(sequoiaChunkIds);
        std::vector futures({replicasFuture, unapprovedReplicasFuture});
        return AllSucceeded(futures)
            .Apply(BIND([result = std::move(result), this, this_ = MakeStrong(this)] (const std::vector<std::vector<TSequoiaChunkReplica>>& sequoiaReplicas) mutable {
                const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
                for (const auto& replicas : sequoiaReplicas) {
                    for (const auto& replica : replicas) {
                        auto chunkId = replica.ChunkId;
                        auto locationUuid = replica.LocationUuid;
                        auto* location = dataNodeTracker->FindChunkLocationByUuid(locationUuid);
                        if (!IsObjectAlive(location)) {
                            YT_LOG_ALERT("Found Sequoia chunk replica with a non-existent location (ChunkId: %v, LocationUuid: %v)",
                                chunkId,
                                locationUuid);
                            continue;
                        }

                        TChunkLocationPtrWithReplicaInfo chunkLocationWithReplicaInfo(
                            location,
                            replica.ReplicaIndex,
                            EChunkReplicaState::Generic);
                        result[chunkId].push_back(chunkLocationWithReplicaInfo);
                    }
                }

                return result;
            })
            .AsyncViaGuarded(
                Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ChunkManager),
                TError("Error fetching Sequoia replicas")));
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
        Bootstrap_->VerifyPersistentStateRead();

        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas;
    }

    TChunkToLocationPtrWithReplicaInfoList GetReplicas(
        const std::vector<TEphemeralObjectPtr<TChunk>>& chunks,
        const TErrorOr<THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList>>& sequoiaReplicasOrError,
        const std::vector<TChunkId>& sequoiaChunkIds) const
    {
        Bootstrap_->VerifyPersistentStateRead();

        TChunkToLocationPtrWithReplicaInfoList result;
        if (!sequoiaReplicasOrError.IsOK()) {
            for (auto chunkId : sequoiaChunkIds) {
                EmplaceOrCrash(result, chunkId, TError(sequoiaReplicasOrError));
            }
        } else {
            for (auto& [chunkId, replicas] : sequoiaReplicasOrError.Value()) {
                EmplaceOrCrash(result, chunkId, std::move(replicas));
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

            // TODO(aleksandra-zh): remove lambda (or some stuff from it) when there are no imaginary locations.
            SortUniqueBy(replicas, [] (const auto& replica) {
                auto location = replica.GetPtr();

                auto replicaIndex = replica.GetReplicaIndex();
                auto isImaginary = location->IsImaginary();

                // These are for imaginary.
                auto nodeId = GetObjectId(location->GetNode());
                auto effectiveMediumIndex = isImaginary ? location->GetEffectiveMediumIndex() : 0;

                // This is for real.
                auto uuid = isImaginary ? TChunkLocationUuid() : location->AsReal()->GetUuid();
                return std::tuple(replicaIndex, isImaginary, nodeId, effectiveMediumIndex, uuid);
            });
        }

        return result;
    }

    TFuture<std::vector<TSequoiaChunkReplica>> DoGetSequoiaReplicas(
        const std::vector<TChunkId>& chunkIds,
        const TColumnFilter& columnFilter,
        const std::function<NYson::TYsonString(const NRecords::TChunkReplicas&)>& extractReplicas) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<NRecords::TChunkReplicasKey> keys;
        for (auto chunkId : chunkIds) {
            NRecords::TChunkReplicasKey chunkReplicasKey{
                .ChunkId = chunkId,
            };
            keys.push_back(chunkReplicasKey);
        }

        return Bootstrap_
            ->GetSequoiaClient()
            ->LookupRows<NRecords::TChunkReplicasKey>(keys, columnFilter)
            .Apply(BIND([extractReplicas] (const std::vector<std::optional<NRecords::TChunkReplicas>>& replicaRecords) {
                return ParseReplicas(replicaRecords, extractReplicas);
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    TFuture<std::vector<TSequoiaChunkReplica>> DoGetSequoiaLastSeenReplicas(TChunkId chunkId) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& idMapping = NRecords::TChunkReplicasDescriptor::Get()->GetIdMapping();
        YT_VERIFY(idMapping.ChunkId && idMapping.LastSeenReplicas);
        TColumnFilter columnFilter{*idMapping.ChunkId, *idMapping.LastSeenReplicas};
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

#include "cypress_integration.h"

#include "private.h"
#include "chunk_location.h"
#include "chunk.h"
#include "chunk_view.h"
#include "chunk_list.h"
#include "chunk_replicator.h"
#include "domestic_medium.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/data_node_tracker.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/lib/object_server/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYPath;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkLocationMap
    : public TVirtualSinglecellMapBase
{
public:
    TVirtualChunkLocationMap(
        TBootstrap* bootstrap,
        INodePtr owningNode,
        const TChunkLocationUuidMap* locations)
        : TVirtualSinglecellMapBase(bootstrap, std::move(owningNode))
        , ChunkLocations_(locations)
    { }

private:
    const TChunkLocationUuidMap* const ChunkLocations_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TString> keys;
        keys.reserve(std::min(sizeLimit, std::ssize(*ChunkLocations_)));
        for (auto [locationUuid, location] : *ChunkLocations_) {
            if (std::ssize(keys) >= sizeLimit) {
                break;
            }
            keys.push_back(ToString(locationUuid));
        }
        return keys;
    }

    i64 GetSize() const override
    {
        return std::ssize(*ChunkLocations_);
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto* const* location = ChunkLocations_->FindPtr(TChunkLocationUuid::FromString(key));
        if (!location || !IsObjectAlive(*location)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(*location);
    }
};

INodeTypeHandlerPtr CreateChunkLocationMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkLocationMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            YT_VERIFY(owningNode);
            const auto& nodeTracker = bootstrap->GetDataNodeTracker();

            const TChunkLocationUuidMap* chunkLocations = nullptr;
            if (auto shardIndex = owningNode->Attributes().Find<int>("chunk_location_shard_index")) {
                chunkLocations = &nodeTracker->ChunkLocationUuidMapShard(*shardIndex);
            } else {
                chunkLocations = &nodeTracker->ChunkLocationUuidMap();
            }

            return New<TVirtualChunkLocationMap>(
                bootstrap,
                owningNode,
                chunkLocations);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualChunkMap(TBootstrap* bootstrap, INodePtr owningNode, EObjectType type)
        : TVirtualMulticellMapBase(bootstrap, owningNode)
        , Type_(type)
    { }

private:
    const EObjectType Type_;

    std::vector<TObjectId> GetFilteredChunkIds(i64 sizeLimit) const
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();
        switch (Type_) {
            case EObjectType::LocalLostChunkMap:
                return ToObjectIds(chunkReplicator->LostChunks(), sizeLimit);
            case EObjectType::LocalLostVitalChunkMap:
                return ToObjectIds(chunkReplicator->LostVitalChunks(), sizeLimit);
            case EObjectType::LocalPrecariousChunkMap:
                return ToObjectIds(chunkReplicator->PrecariousChunks(), sizeLimit);
            case EObjectType::LocalPrecariousVitalChunkMap:
                return ToObjectIds(chunkReplicator->PrecariousVitalChunks(), sizeLimit);
            case EObjectType::LocalOverreplicatedChunkMap:
                return ToObjectIds(chunkReplicator->OverreplicatedChunks(), sizeLimit);
            case EObjectType::LocalUnderreplicatedChunkMap:
                return ToObjectIds(chunkReplicator->UnderreplicatedChunks(), sizeLimit);
            case EObjectType::LocalDataMissingChunkMap:
                return ToObjectIds(chunkReplicator->DataMissingChunks(), sizeLimit);
            case EObjectType::LocalParityMissingChunkMap:
                return ToObjectIds(chunkReplicator->ParityMissingChunks(), sizeLimit);
            case EObjectType::LocalQuorumMissingChunkMap:
                return ToObjectIds(chunkReplicator->QuorumMissingChunks(), sizeLimit);
            case EObjectType::LocalUnsafelyPlacedChunkMap:
                return ToObjectIds(chunkReplicator->UnsafelyPlacedChunks(), sizeLimit);
            case EObjectType::LocalInconsistentlyPlacedChunkMap:
                return ToObjectIds(chunkReplicator->InconsistentlyPlacedChunks(), sizeLimit);
            case EObjectType::LocalUnexpectedOverreplicatedChunkMap:
                return ToObjectIds(chunkReplicator->UnexpectedOverreplicatedChunks(), sizeLimit);
            case EObjectType::LocalReplicaTemporarilyUnavailableChunkMap:
                return ToObjectIds(chunkReplicator->TemporarilyUnavailableChunks(), sizeLimit);
            case EObjectType::ForeignChunkMap:
                return ToObjectIds(chunkManager->ForeignChunks(), sizeLimit);
            case EObjectType::LocalOldestPartMissingChunkMap:
                return ToObjectIds(chunkReplicator->OldestPartMissingChunks(), sizeLimit);
            default:
                YT_ABORT();
        }
    }

    bool FilteredChunksContain(TChunk* chunk) const
    {
        if (NHydra::HasMutationContext()) {
            THROW_ERROR_EXCEPTION("Mutating request through virtual map is forbidden");
        }

        auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(chunk);

        Bootstrap_->GetHydraFacade()->RequireLeader();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();

        // This is context switch, chunk may die.
        auto replicasOrError = chunkManager->GetChunkReplicas(ephemeralChunk);
        // TODO(aleksandra-zh): maybe do smth else.
        if (!replicasOrError.IsOK()) {
            return false;
        }

        const auto& chunkReplicas = replicasOrError.Value();

        auto checkReplicatorCrossMediumStatus = [&] (
            ECrossMediumChunkStatus status,
            bool localMap,
            bool vitalMap = false)
        {
            if (localMap && !chunkReplicator->ShouldProcessChunk(chunk)) {
                return false;
            }

            auto chunkStatus = chunkReplicator->ComputeCrossMediumChunkStatus(chunk, chunkReplicas);
            if (None(chunkStatus & status)) {
                return false;
            }

            return !vitalMap || chunkReplicator->IsDurabilityRequired(chunk, chunkReplicas);
        };

        auto checkReplicatorStatus = [&] (
            EChunkStatus status,
            bool localMap,
            bool vitalMap = false)
        {
            if (localMap && !chunkReplicator->ShouldProcessChunk(chunk)) {
                return false;
            }

            auto chunkStatus = chunkReplicator->ComputeChunkStatuses(chunk, chunkReplicas);
            auto aggregateStatus = EChunkStatus::None;
            for (auto [mediumIndex, mediumState] : chunkStatus) {
                aggregateStatus |= mediumState;
            }

            if (None(aggregateStatus & status)) {
                return false;
            }

            return !vitalMap || chunkReplicator->IsDurabilityRequired(chunk, chunkReplicas);
        };

        switch (Type_) {
            case EObjectType::LostChunkMap:
                return checkReplicatorStatus(EChunkStatus::Lost, /*localMap*/ false);
            case EObjectType::LocalLostChunkMap:
                return checkReplicatorStatus(EChunkStatus::Lost, /*localMap*/ true);
            case EObjectType::LostVitalChunkMap:
                return checkReplicatorStatus(EChunkStatus::Lost, /*localMap*/ false, /*vitalMap*/ true);
            case EObjectType::LocalLostVitalChunkMap:
                return checkReplicatorStatus(EChunkStatus::Lost, /*localMap*/ true, /*vitalMap*/ true);
            case EObjectType::PrecariousChunkMap:
                return checkReplicatorCrossMediumStatus(ECrossMediumChunkStatus::Precarious, /*localMap*/ false);
            case EObjectType::LocalPrecariousChunkMap:
                return checkReplicatorCrossMediumStatus(ECrossMediumChunkStatus::Precarious, /*localMap*/ true);
            case EObjectType::PrecariousVitalChunkMap:
                return checkReplicatorCrossMediumStatus(ECrossMediumChunkStatus::Precarious, /*localMap*/ false, /*vitalMap*/ true);
            case EObjectType::LocalPrecariousVitalChunkMap:
                return checkReplicatorCrossMediumStatus(ECrossMediumChunkStatus::Precarious, /*localMap*/ true, /*vitalMap*/ true);
            case EObjectType::OverreplicatedChunkMap:
                return checkReplicatorStatus(EChunkStatus::Overreplicated, /*localMap*/ false);
            case EObjectType::LocalOverreplicatedChunkMap:
                return checkReplicatorStatus(EChunkStatus::Overreplicated, /*localMap*/ true);
            case EObjectType::UnderreplicatedChunkMap:
                return checkReplicatorStatus(EChunkStatus::Underreplicated, /*localMap*/ false);
            case EObjectType::LocalUnderreplicatedChunkMap:
                return checkReplicatorStatus(EChunkStatus::Underreplicated, /*localMap*/ true);
            case EObjectType::DataMissingChunkMap:
                return checkReplicatorStatus(EChunkStatus::DataMissing, /*localMap*/ false);
            case EObjectType::LocalDataMissingChunkMap:
                return checkReplicatorStatus(EChunkStatus::DataMissing, /*localMap*/ true);
            case EObjectType::ParityMissingChunkMap:
                return checkReplicatorStatus(EChunkStatus::ParityMissing, /*localMap*/ false);
            case EObjectType::LocalParityMissingChunkMap:
                return checkReplicatorStatus(EChunkStatus::ParityMissing, /*localMap*/ true);
            case EObjectType::QuorumMissingChunkMap:
                return checkReplicatorCrossMediumStatus(ECrossMediumChunkStatus::QuorumMissing, /*localMap*/ false);
            case EObjectType::LocalQuorumMissingChunkMap:
                return checkReplicatorCrossMediumStatus(ECrossMediumChunkStatus::QuorumMissing, /*localMap*/ true);
            case EObjectType::UnsafelyPlacedChunkMap:
                return checkReplicatorStatus(EChunkStatus::UnsafelyPlaced, /*localMap*/ false);
            case EObjectType::LocalUnsafelyPlacedChunkMap:
                return checkReplicatorStatus(EChunkStatus::UnsafelyPlaced, /*localMap*/ true);
            case EObjectType::InconsistentlyPlacedChunkMap:
                return checkReplicatorStatus(EChunkStatus::InconsistentlyPlaced, /*localMap*/ false);
            case EObjectType::LocalInconsistentlyPlacedChunkMap:
                return checkReplicatorStatus(EChunkStatus::InconsistentlyPlaced, /*localMap*/ true);
            case EObjectType::UnexpectedOverreplicatedChunkMap:
                return checkReplicatorStatus(EChunkStatus::UnexpectedOverreplicated, /*localMap*/ false);
            case EObjectType::LocalUnexpectedOverreplicatedChunkMap:
                return checkReplicatorStatus(EChunkStatus::UnexpectedOverreplicated, /*localMap*/ true);
            case EObjectType::ReplicaTemporarilyUnavailableChunkMap:
                return checkReplicatorStatus(EChunkStatus::TemporarilyUnavailable, /*localMap*/ false);
            case EObjectType::LocalReplicaTemporarilyUnavailableChunkMap:
                return checkReplicatorStatus(EChunkStatus::TemporarilyUnavailable, /*localMap*/ true);
            case EObjectType::ForeignChunkMap:
                return chunkManager->ForeignChunks().contains(chunk);
            case EObjectType::OldestPartMissingChunkMap:
                // TODO(gritukan): This is hard to implement. Think about remote proxy to peer.
                return false;
            case EObjectType::LocalOldestPartMissingChunkMap:
                return chunkReplicator->OldestPartMissingChunks().contains(chunk);
            default:
                YT_ABORT();
        }
    }

    i64 GetFilteredChunkCount() const
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();
        switch (Type_) {
            case EObjectType::LocalLostChunkMap:
                return chunkReplicator->LostChunks().size();
            case EObjectType::LocalLostVitalChunkMap:
                return chunkReplicator->LostVitalChunks().size();
            case EObjectType::LocalPrecariousChunkMap:
                return chunkReplicator->PrecariousChunks().size();
            case EObjectType::LocalPrecariousVitalChunkMap:
                return chunkReplicator->PrecariousVitalChunks().size();
            case EObjectType::LocalOverreplicatedChunkMap:
                return chunkReplicator->OverreplicatedChunks().size();
            case EObjectType::LocalUnderreplicatedChunkMap:
                return chunkReplicator->UnderreplicatedChunks().size();
            case EObjectType::LocalDataMissingChunkMap:
                return chunkReplicator->DataMissingChunks().size();
            case EObjectType::LocalParityMissingChunkMap:
                return chunkReplicator->ParityMissingChunks().size();
            case EObjectType::LocalQuorumMissingChunkMap:
                return chunkReplicator->QuorumMissingChunks().size();
            case EObjectType::LocalUnsafelyPlacedChunkMap:
                return chunkReplicator->UnsafelyPlacedChunks().size();
            case EObjectType::LocalInconsistentlyPlacedChunkMap:
                return chunkReplicator->InconsistentlyPlacedChunks().size();
            case EObjectType::LocalUnexpectedOverreplicatedChunkMap:
                return chunkReplicator->UnexpectedOverreplicatedChunks().size();
            case EObjectType::LocalReplicaTemporarilyUnavailableChunkMap:
                return chunkReplicator->TemporarilyUnavailableChunks().size();
            case EObjectType::ForeignChunkMap:
                return chunkManager->ForeignChunks().size();
            case EObjectType::LocalOldestPartMissingChunkMap:
                return chunkReplicator->OldestPartMissingChunks().size();
            default:
                YT_ABORT();
        }
    }

    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        if (Type_ == EObjectType::ChunkMap) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            return MakeFuture(ToObjectIds(GetValues(chunkManager->Chunks(), sizeLimit)));
        } else if (IsLocal()) {
            return MakeFuture(GetFilteredChunkIds(sizeLimit));
        } else {
            Bootstrap_->GetHydraFacade()->RequireLeader();

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto channels = chunkManager->GetChunkReplicatorChannels();

            std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> responseFutures;
            responseFutures.reserve(channels.size());
            for (const auto& channel : channels) {
                auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
                auto batchReq = proxy.ExecuteBatch();
                auto req = TCypressYPathProxy::Enumerate(GetWellKnownPath(GetLocalChunkMapType()));
                req->set_limit(sizeLimit);
                batchReq->AddRequest(req, "enumerate");
                responseFutures.push_back(batchReq->Invoke());
            }

            return AllSucceeded(std::move(responseFutures))
                .Apply(BIND([=] (const std::vector<TObjectServiceProxy::TRspExecuteBatchPtr>& batchRsps) {
                    std::vector<TObjectId> keys;
                    for (const auto& batchRsp : batchRsps) {
                        auto rspOrError = batchRsp->GetResponse<TCypressYPathProxy::TRspEnumerate>("enumerate");
                        const auto& rsp = rspOrError.ValueOrThrow();
                        for (const auto& protoItem : rsp->items()) {
                            if (std::ssize(keys) >= sizeLimit) {
                                break;
                            }

                            auto chunkId = TChunkId::FromString(protoItem.key());
                            keys.push_back(chunkId);
                        }
                    }

                    return keys;
                }));
        }
    }

    bool IsValid(TObject* object) const override
    {
        auto type = object->GetType();
        if (type != EObjectType::Chunk &&
            type != EObjectType::ErasureChunk &&
            type != EObjectType::JournalChunk &&
            type != EObjectType::ErasureJournalChunk)
        {
            return false;
        }

        if (Type_ == EObjectType::ChunkMap) {
            return true;
        }

        auto* chunk = object->As<TChunk>();
        return FilteredChunksContain(chunk);
    }

    TFuture<i64> GetSize() const override
    {
        if (Type_ == EObjectType::ChunkMap) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            return MakeFuture<i64>(chunkManager->Chunks().GetSize());
        } else if (IsLocal()) {
            return MakeFuture(GetFilteredChunkCount());
        } else {
            Bootstrap_->GetHydraFacade()->RequireLeader();

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto channels = chunkManager->GetChunkReplicatorChannels();

            std::vector<TFuture<TIntrusivePtr<TObjectYPathProxy::TRspGet>>> responseFutures;
            responseFutures.reserve(channels.size());
            for (const auto& channel : channels) {
                auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
                auto req = TYPathProxy::Get(GetWellKnownPath(GetLocalChunkMapType()) + "/@count");
                responseFutures.push_back(proxy.Execute(req));
            }

            return AllSucceeded(std::move(responseFutures))
                .Apply(BIND([] (const std::vector<TIntrusivePtr<TObjectYPathProxy::TRspGet>>& rsps) {
                    i64 size = 0;
                    for (const auto& rsp : rsps) {
                        auto response = ConvertTo<INodePtr>(TYsonString{rsp->value()});
                        YT_VERIFY(response->GetType() == ENodeType::Int64);
                        size += response->AsInt64()->GetValue();
                    }

                    return size;
                }));
        }
    }

    virtual TFuture<std::vector<std::pair<NObjectClient::TCellTag, i64>>> FetchSizes() override
    {
        if (!IsMulticell()) {
            return GetSize()
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (i64 size) {
                    return std::vector{std::pair(Bootstrap_->GetMulticellManager()->GetCellTag(), size)};
                }));
        }

        return TVirtualMulticellMapBase::FetchSizes();
    }

    NYPath::TYPath GetWellKnownPath(EObjectType type) const
    {
        switch (type) {
            case EObjectType::ChunkMap:
                return "//sys/chunks";
            case EObjectType::LostChunkMap:
                return "//sys/lost_chunks";
            case EObjectType::LostVitalChunkMap:
                return "//sys/lost_vital_chunks";
            case EObjectType::PrecariousChunkMap:
                return "//sys/precarious_chunks";
            case EObjectType::PrecariousVitalChunkMap:
                return "//sys/precarious_vital_chunks";
            case EObjectType::OverreplicatedChunkMap:
                return "//sys/overreplicated_chunks";
            case EObjectType::UnderreplicatedChunkMap:
                return "//sys/underreplicated_chunks";
            case EObjectType::DataMissingChunkMap:
                return "//sys/data_missing_chunks";
            case EObjectType::ParityMissingChunkMap:
                return "//sys/parity_missing_chunks";
            case EObjectType::OldestPartMissingChunkMap:
                return "//sys/oldest_part_missing_chunks";
            case EObjectType::QuorumMissingChunkMap:
                return "//sys/quorum_missing_chunks";
            case EObjectType::UnsafelyPlacedChunkMap:
                return "//sys/unsafely_placed_chunks";
            case EObjectType::InconsistentlyPlacedChunkMap:
                return "//sys/inconsistently_placed_chunks";
            case EObjectType::UnexpectedOverreplicatedChunkMap:
                return "//sys/unexpected_overreplicated_chunks";
            case EObjectType::ReplicaTemporarilyUnavailableChunkMap:
                return "//sys/replica_temporarily_unavailable_chunks";
            case EObjectType::ForeignChunkMap:
                return "//sys/foreign_chunks";
            case EObjectType::LocalLostChunkMap:
                return "//sys/local_lost_chunks";
            case EObjectType::LocalLostVitalChunkMap:
                return "//sys/local_lost_vital_chunks";
            case EObjectType::LocalPrecariousChunkMap:
                return "//sys/local_precarious_chunks";
            case EObjectType::LocalPrecariousVitalChunkMap:
                return "//sys/local_precarious_vital_chunks";
            case EObjectType::LocalOverreplicatedChunkMap:
                return "//sys/local_overreplicated_chunks";
            case EObjectType::LocalUnderreplicatedChunkMap:
                return "//sys/local_underreplicated_chunks";
            case EObjectType::LocalDataMissingChunkMap:
                return "//sys/local_data_missing_chunks";
            case EObjectType::LocalParityMissingChunkMap:
                return "//sys/local_parity_missing_chunks";
            case EObjectType::LocalOldestPartMissingChunkMap:
                return "//sys/local_oldest_part_missing_chunks";
            case EObjectType::LocalQuorumMissingChunkMap:
                return "//sys/local_quorum_missing_chunks";
            case EObjectType::LocalUnsafelyPlacedChunkMap:
                return "//sys/local_unsafely_placed_chunks";
            case EObjectType::LocalInconsistentlyPlacedChunkMap:
                return "//sys/local_inconsistently_placed_chunks";
            case EObjectType::LocalUnexpectedOverreplicatedChunkMap:
                return "//sys/local_unexpected_overreplicated_chunks";
            case EObjectType::LocalReplicaTemporarilyUnavailableChunkMap:
                return "//sys/local_replica_temporarily_unavailable_chunks";
            default:
                YT_ABORT();
        }
    }

    NYPath::TYPath GetWellKnownPath() const override
    {
        return GetWellKnownPath(Type_);
    }

    EObjectType GetLocalChunkMapType() const
    {
        switch (Type_) {
            case EObjectType::LostChunkMap:
                return EObjectType::LocalLostChunkMap;
            case EObjectType::LostVitalChunkMap:
                return EObjectType::LocalLostVitalChunkMap;
            case EObjectType::PrecariousChunkMap:
                return EObjectType::LocalPrecariousVitalChunkMap;
            case EObjectType::PrecariousVitalChunkMap:
                return EObjectType::LocalPrecariousVitalChunkMap;
            case EObjectType::OverreplicatedChunkMap:
                return EObjectType::LocalOverreplicatedChunkMap;
            case EObjectType::UnderreplicatedChunkMap:
                return EObjectType::LocalUnderreplicatedChunkMap;
            case EObjectType::DataMissingChunkMap:
                return EObjectType::LocalDataMissingChunkMap;
            case EObjectType::ParityMissingChunkMap:
                return EObjectType::LocalParityMissingChunkMap;
            case EObjectType::OldestPartMissingChunkMap:
                return EObjectType::LocalOldestPartMissingChunkMap;
            case EObjectType::QuorumMissingChunkMap:
                return EObjectType::LocalQuorumMissingChunkMap;
            case EObjectType::UnsafelyPlacedChunkMap:
                return EObjectType::LocalUnsafelyPlacedChunkMap;
            case EObjectType::InconsistentlyPlacedChunkMap:
                return EObjectType::LocalInconsistentlyPlacedChunkMap;
            case EObjectType::UnexpectedOverreplicatedChunkMap:
                return EObjectType::LocalUnexpectedOverreplicatedChunkMap;
            case EObjectType::ReplicaTemporarilyUnavailableChunkMap:
                return EObjectType::LocalReplicaTemporarilyUnavailableChunkMap;
            default:
                YT_ABORT();
        }
    }

    bool IsMulticell() const
    {
        switch (Type_) {
            case EObjectType::LostChunkMap:
            case EObjectType::LostVitalChunkMap:
            case EObjectType::PrecariousChunkMap:
            case EObjectType::PrecariousVitalChunkMap:
            case EObjectType::OverreplicatedChunkMap:
            case EObjectType::UnderreplicatedChunkMap:
            case EObjectType::DataMissingChunkMap:
            case EObjectType::ParityMissingChunkMap:
            case EObjectType::OldestPartMissingChunkMap:
            case EObjectType::QuorumMissingChunkMap:
            case EObjectType::UnsafelyPlacedChunkMap:
            case EObjectType::InconsistentlyPlacedChunkMap:
            case EObjectType::UnexpectedOverreplicatedChunkMap:
            case EObjectType::ReplicaTemporarilyUnavailableChunkMap:
            case EObjectType::ChunkMap:
            case EObjectType::ForeignChunkMap:
                return true;
            default:
                return false;
        }
    }

    bool IsSharded() const
    {
        switch (Type_) {
            case EObjectType::LostChunkMap:
            case EObjectType::LostVitalChunkMap:
            case EObjectType::PrecariousChunkMap:
            case EObjectType::PrecariousVitalChunkMap:
            case EObjectType::OverreplicatedChunkMap:
            case EObjectType::UnderreplicatedChunkMap:
            case EObjectType::DataMissingChunkMap:
            case EObjectType::ParityMissingChunkMap:
            case EObjectType::OldestPartMissingChunkMap:
            case EObjectType::QuorumMissingChunkMap:
            case EObjectType::UnsafelyPlacedChunkMap:
            case EObjectType::InconsistentlyPlacedChunkMap:
            case EObjectType::UnexpectedOverreplicatedChunkMap:
            case EObjectType::ReplicaTemporarilyUnavailableChunkMap:
                return true;
            default:
                return false;
        }
    }

    bool IsLocal() const
    {
        return !IsSharded();
    }
};

INodeTypeHandlerPtr CreateChunkMapTypeHandler(
    TBootstrap* bootstrap,
    EObjectType type)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        type,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualChunkMap>(bootstrap, owningNode, type);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkViewMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return MakeFuture(ToObjectIds(GetValues(chunkManager->ChunkViews(), sizeLimit)));
    }

    bool IsValid(TObject* object) const override
    {
        return object->GetType() == EObjectType::ChunkView;
    }

    TFuture<i64> GetSize() const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return MakeFuture<i64>(chunkManager->ChunkViews().GetSize());
    }

    TYPath GetWellKnownPath() const override
    {
        return "//sys/chunk_views";
    }
};

INodeTypeHandlerPtr CreateChunkViewMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkViewMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualChunkViewMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return MakeFuture(ToObjectIds(GetValues(chunkManager->ChunkLists(), sizeLimit)));
    }

    bool IsValid(TObject* object) const override
    {
        return object->GetType() == EObjectType::ChunkList;
    }

    TFuture<i64> GetSize() const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return MakeFuture<i64>(chunkManager->ChunkLists().GetSize());
    }

    TYPath GetWellKnownPath() const override
    {
        return "//sys/chunk_lists";
    }
};

INodeTypeHandlerPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualChunkListMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualMediumMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<TString> GetKeys(i64 /*sizeLimit*/) const override
    {
        std::vector<TString> keys;
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (auto [mediumId, medium] : chunkManager->Media()) {
            keys.push_back(medium->GetName());
        }
        return keys;
    }

    i64 GetSize() const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->Media().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->FindMediumByName(TString(key));
        if (!IsObjectAlive(medium)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(medium);
    }
};

INodeTypeHandlerPtr CreateMediumMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::MediumMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualMediumMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#include "data_node_tracker.h"

#include "data_node_tracker_internal.h"
#include "chunk_manager.h"
#include "chunk_location.h"
#include "chunk_location_type_handler.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/alert_manager.h>
#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>
#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NDataNodeTrackerClient::NProto;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NYTree;

using NDataNodeTrackerClient::TChunkLocationDirectory;
using NNodeTrackerClient::NProto::TReqRegisterNode;
using NNodeTrackerClient::NProto::TRspRegisterNode;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

int GetChunkLocationShardIndex(TChunkLocationUuid uuid)
{
    return TDirectObjectIdHash()(uuid) % ChunkLocationShardCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTracker
    : public IDataNodeTracker
    , public IDataNodeTrackerInternal
    , public TMasterAutomatonPart
{
public:
    explicit TDataNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::DataNodeTracker)
    {
        RegisterMethod(BIND(&TDataNodeTracker::HydraIncrementalDataNodeHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TDataNodeTracker::HydraFullDataNodeHeartbeat, Unretained(this)));

        RegisterLoader(
            "DataNodeTracker.Keys",
            BIND(&TDataNodeTracker::LoadKeys, Unretained(this)));
        RegisterLoader(
            "DataNodeTracker.Values",
            BIND(&TDataNodeTracker::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "DataNodeTracker.Keys",
            BIND(&TDataNodeTracker::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "DataNodeTracker.Values",
            BIND(&TDataNodeTracker::SaveValues, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TDataNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeNodeUnregistered(BIND_NO_PROPAGATE(&TDataNodeTracker::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeZombified(BIND_NO_PROPAGATE(&TDataNodeTracker::OnNodeZombified, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChunkLocationTypeHandler(Bootstrap_, this));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND_NO_PROPAGATE(&TDataNodeTracker::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND_NO_PROPAGATE(&TDataNodeTracker::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        const auto& alertManager = Bootstrap_->GetAlertManager();
        alertManager->RegisterAlertSource(BIND(&TDataNodeTracker::GetAlerts, MakeStrong(this)));
    }

    void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        const auto& originalRequest = context->Request();
        auto nodeId = FromProto<TNodeId>(originalRequest.node_id());
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);
        auto locationDirectory = ParseLocationDirectoryOrThrow(node, this, originalRequest);
        THashSet<int> sequoiaLocationIndices;
        for (int i = 0; i < std::ssize(locationDirectory); ++i) {
            if (chunkManager->CanHaveSequoiaReplicas(locationDirectory[i])) {
                InsertOrCrash(sequoiaLocationIndices, i);
            }
        }

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto isSequoiaEnabled = config->SequoiaManager->Enable;
        auto sequoiaChunkProbability = config->ChunkManager->SequoiaChunkReplicasPercentage;

        auto splitRequest = BIND([=, sequoiaLocationIndices = std::move(sequoiaLocationIndices)] () {
            auto preparedRequest = NewWithOffloadedDtor<TFullHeartbeatRequest>(NRpc::TDispatcher::Get()->GetHeavyInvoker());
            preparedRequest->NonSequoiaRequest.CopyFrom(originalRequest);
            if (!isSequoiaEnabled) {
                return preparedRequest;
            }

            preparedRequest->NonSequoiaRequest.mutable_chunks()->Clear();

            preparedRequest->SequoiaRequest.set_node_id(originalRequest.node_id());
            preparedRequest->SequoiaRequest.mutable_location_directory()->CopyFrom(originalRequest.location_directory());

            for (const auto& chunkInfo : originalRequest.chunks()) {
                auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());
                auto locationIndex = chunkInfo.location_index();
                if (sequoiaLocationIndices.contains(locationIndex) && chunkManager->CanHaveSequoiaReplicas(chunkId, sequoiaChunkProbability)) {
                    *preparedRequest->SequoiaRequest.add_added_chunks() = chunkInfo;
                } else {
                    *preparedRequest->NonSequoiaRequest.add_chunks() = chunkInfo;
                }
            }

            return preparedRequest;
        });

        auto preparedRequest = WaitFor(splitRequest
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
            .Run())
            .ValueOrThrow();

        if (preparedRequest->SequoiaRequest.removed_chunks_size() + preparedRequest->SequoiaRequest.added_chunks_size() > 0) {
            for (const auto& protoChunkId : preparedRequest->SequoiaRequest.removed_chunks()) {
                if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                    *preparedRequest->SequoiaRequest.add_dead_chunk_ids() = protoChunkId.chunk_id();
                }
            }

            for (const auto& protoChunkId : preparedRequest->SequoiaRequest.added_chunks()) {
                if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                    *preparedRequest->SequoiaRequest.add_dead_chunk_ids() = protoChunkId.chunk_id();
                }
            }

            WaitFor(chunkManager->ModifySequoiaReplicas(preparedRequest->SequoiaRequest))
                .ThrowOnError();
        }

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            &preparedRequest->NonSequoiaRequest,
            &preparedRequest->NonSequoiaResponse,
            &TDataNodeTracker::HydraFullDataNodeHeartbeat,
            this);

        CommitMutationWithSemaphore(
            std::move(mutation),
            std::move(context),
            std::move(preparedRequest),
            FullHeartbeatSemaphore_);
    }

    void ProcessFullHeartbeat(
        TNode* node,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto& statistics = *request->mutable_statistics();
        PopulateChunkLocationStatistics(node, statistics.chunk_locations());

        node->SetDataNodeStatistics(std::move(statistics), chunkManager);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Data);

        if (Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            SerializeMediumDirectory(response->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, response->mutable_medium_overrides());
        }

        chunkManager->ProcessFullDataNodeHeartbeat(node, request, response);
    }

    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        const auto& originalRequest = context->Request();
        auto nodeId = FromProto<TNodeId>(originalRequest.node_id());
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);
        auto locationDirectory = ParseLocationDirectoryOrThrow(node, this, originalRequest);
        THashSet<int> sequoiaLocationIndices;
        for (int i = 0; i < std::ssize(locationDirectory); ++i) {
            if (chunkManager->CanHaveSequoiaReplicas(locationDirectory[i])) {
                InsertOrCrash(sequoiaLocationIndices, i);
            }
        }

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto isSequoiaEnabled = config->SequoiaManager->Enable;
        auto sequoiaChunkProbability = config->ChunkManager->SequoiaChunkReplicasPercentage;

        auto splitRequest = BIND([=, sequoiaLocationIndices = std::move(sequoiaLocationIndices)] () {
            auto preparedRequest = NewWithOffloadedDtor<TIncrementalHeartbeatRequest>(NRpc::TDispatcher::Get()->GetHeavyInvoker());
            preparedRequest->NonSequoiaRequest.CopyFrom(originalRequest);
            if (!isSequoiaEnabled) {
                return preparedRequest;
            }

            preparedRequest->NonSequoiaRequest.mutable_added_chunks()->Clear();
            preparedRequest->NonSequoiaRequest.mutable_removed_chunks()->Clear();

            preparedRequest->SequoiaRequest.set_node_id(originalRequest.node_id());
            preparedRequest->SequoiaRequest.mutable_location_directory()->CopyFrom(originalRequest.location_directory());

            for (const auto& chunkInfo : originalRequest.added_chunks()) {
                auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());
                auto locationIndex = chunkInfo.location_index();
                if (sequoiaLocationIndices.contains(locationIndex) && chunkManager->CanHaveSequoiaReplicas(chunkId, sequoiaChunkProbability)) {
                    *preparedRequest->SequoiaRequest.add_added_chunks() = chunkInfo;
                } else {
                    *preparedRequest->NonSequoiaRequest.add_added_chunks() = chunkInfo;
                }
            }

            for (const auto& chunkInfo : originalRequest.removed_chunks()) {
                auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());
                auto locationIndex = chunkInfo.location_index();
                if (sequoiaLocationIndices.contains(locationIndex) && chunkManager->CanHaveSequoiaReplicas(chunkId, sequoiaChunkProbability)) {
                    *preparedRequest->SequoiaRequest.add_removed_chunks() = chunkInfo;
                } else {
                    *preparedRequest->NonSequoiaRequest.add_removed_chunks() = chunkInfo;
                }
            }

            return preparedRequest;
        });

        auto preparedRequest = WaitFor(splitRequest
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
            .Run())
            .ValueOrThrow();

        if (preparedRequest->SequoiaRequest.removed_chunks_size() + preparedRequest->SequoiaRequest.added_chunks_size() > 0) {
            for (const auto& protoChunkId : preparedRequest->SequoiaRequest.removed_chunks()) {
                if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                    *preparedRequest->SequoiaRequest.add_dead_chunk_ids() = protoChunkId.chunk_id();
                }
            }

            for (const auto& protoChunkId : preparedRequest->SequoiaRequest.added_chunks()) {
                if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                    *preparedRequest->SequoiaRequest.add_dead_chunk_ids() = protoChunkId.chunk_id();
                }
            }

            WaitFor(chunkManager->ModifySequoiaReplicas(preparedRequest->SequoiaRequest))
                .ThrowOnError();
        }

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            &preparedRequest->NonSequoiaRequest,
            &preparedRequest->NonSequoiaResponse,
            &TDataNodeTracker::HydraIncrementalDataNodeHeartbeat,
            this);

        CommitMutationWithSemaphore(
            std::move(mutation),
            std::move(context),
            std::move(preparedRequest),
            IncrementalHeartbeatSemaphore_);
    }

    void ProcessIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto& statistics = *request->mutable_statistics();
        PopulateChunkLocationStatistics(node, statistics.chunk_locations());
        node->SetDataNodeStatistics(std::move(statistics), chunkManager);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Data);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            SerializeMediumDirectory(response->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, response->mutable_medium_overrides());

            node->SetDisableWriteSessionsReportedByNode(request->write_sessions_disabled());
            response->set_disable_write_sessions(node->AreWriteSessionsDisabled());
            node->SetDisableWriteSessionsSentToNode(node->AreWriteSessionsDisabled());
        }

        chunkManager->ProcessIncrementalDataNodeHeartbeat(node, request, response);
    }

    void ValidateRegisterNode(
        const TString& address,
        TReqRegisterNode* request) override
    {
        auto chunkLocationUuids = FromProto<std::vector<TChunkLocationUuid>>(request->chunk_location_uuids());

        {
            THashSet<TChunkLocationUuid> locationUuidSet;
            for (auto locationUuid : chunkLocationUuids) {
                if (!locationUuidSet.insert(locationUuid).second) {
                    THROW_ERROR_EXCEPTION("Duplicate chunk location uuid %v reported by node %Qv",
                        locationUuid,
                        address);
                }
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto locationUuid : chunkLocationUuids) {
            if (auto* existingLocation = FindChunkLocationByUuid(locationUuid); IsObjectAlive(existingLocation)) {
                objectManager->ValidateObjectLifeStage(existingLocation);
                if (auto* existingNode = existingLocation->GetNode(); IsObjectAlive(existingNode)) {
                    if (existingNode->GetDefaultAddress() != address) {
                        THROW_ERROR_EXCEPTION("Cannot register node %Qv: there is another cluster node %Qv with the same location uuid %v",
                            address,
                            existingNode->GetDefaultAddress(),
                            locationUuid);
                    }
                }
            }
        }

        // COMPAT(kvk1920)
        THROW_ERROR_EXCEPTION_IF(
            !chunkLocationUuids.empty() && !request->location_directory_supported(),
            "Cannot register node %Qv: location directory is required by master server but not supported by data node",
            address);
    }

    void ProcessRegisterNode(
        TNode* node,
        TReqRegisterNode* request,
        TRspRegisterNode* response) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        auto chunkLocationUuids = FromProto<std::vector<TChunkLocationUuid>>(request->chunk_location_uuids());

        if (Bootstrap_->IsPrimaryMaster()) {
            for (auto locationUuid : chunkLocationUuids) {
                if (IsObjectAlive(FindChunkLocationByUuid(locationUuid))) {
                    continue;
                }

                auto req = TMasterYPathProxy::CreateObject();
                req->set_type(static_cast<int>(EObjectType::ChunkLocation));

                auto attributes = CreateEphemeralAttributes();
                attributes->Set("uuid", locationUuid);
                ToProto(req->mutable_object_attributes(), *attributes);

                const auto& rootService = Bootstrap_->GetObjectManager()->GetRootService();
                try {
                    SyncExecuteVerb(rootService, req);
                } catch (const std::exception& ex) {
                    YT_LOG_ALERT(ex,
                        "Failed to create chunk location for a node (NodeAddress: %v, LocationUuid: %v)",
                        node->GetDefaultAddress(),
                        locationUuid);
                    throw;
                }
            }
        }

        if (Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            auto* dataNodeInfoExt = response->MutableExtension(NNodeTrackerClient::NProto::TDataNodeInfoExt::data_node_info_ext);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            SerializeMediumDirectory(dataNodeInfoExt->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, dataNodeInfoExt->mutable_medium_overrides());

            dataNodeInfoExt->set_require_location_uuids(false);
        }

        node->ClearChunkLocations();

        if (!node->UseImaginaryChunkLocations()) {
            node->ChunkLocations().reserve(chunkLocationUuids.size());
        }

        for (auto locationUuid : chunkLocationUuids) {
            auto* location = FindChunkLocationByUuid(locationUuid);
            if (!IsObjectAlive(location)) {
                YT_LOG_ALERT(
                    "Missing chunk location for node (NodeAddress: %v, LocationUuid: %v)",
                    node->GetDefaultAddress(),
                    locationUuid);
                continue;
            }

            if (auto* existingNode = location->GetNode(); IsObjectAlive(existingNode) && existingNode != node) {
                // It was already checked in DataNodeTracker::ValidateRegisterNode().
                YT_LOG_FATAL("Chunk location is already bound to another node (NodeAddress: %v, LocationUuid: %v, BoundNodeAddress: %v)",
                    node->GetDefaultAddress(),
                    locationUuid,
                    existingNode->GetDefaultAddress());
            } else {
                location->SetNode(node);
                node->AddRealChunkLocation(location);
            }

            location->SetState(EChunkLocationState::Online);
        }

        node->ChunkLocations().shrink_to_fit();
    }


    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChunkLocation, TRealChunkLocation);

    TEntityMap<TRealChunkLocation>* MutableChunkLocations() override
    {
        return &ChunkLocationMap_;
    }

    TRealChunkLocation* FindChunkLocationByUuid(TChunkLocationUuid locationUuid) override
    {
        auto it = ChunkLocationUuidToLocation_.find(locationUuid);
        return it == ChunkLocationUuidToLocation_.end() ? nullptr : it->second;
    }

    TRealChunkLocation* GetChunkLocationByUuid(TChunkLocationUuid locationUuid) override
    {
        return GetOrCrash(ChunkLocationUuidToLocation_, locationUuid);
    }

    TRealChunkLocation* CreateChunkLocation(
        TChunkLocationUuid locationUuid,
        TObjectId hintId) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto locationId = objectManager->GenerateId(EObjectType::ChunkLocation, hintId);

        auto locationHolder = TPoolAllocator::New<TRealChunkLocation>(locationId);
        auto* location = ChunkLocationMap_.Insert(locationId, std::move(locationHolder));
        location->SetUuid(locationUuid);

        if (Bootstrap_->IsSecondaryMaster()) {
            location->SetForeign();
        }

        objectManager->RefObject(location);

        RegisterChunkLocationUuid(location);

        YT_LOG_DEBUG("Chunk location created (LocationId: %v, LocationUuid: %v)",
            locationId,
            locationUuid);

        return location;
    }

    void DestroyChunkLocation(TRealChunkLocation* location) override
    {
        auto* node = location->GetNode();

        YT_LOG_DEBUG("Chunk location destroyed (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
            location->GetId(),
            location->GetUuid(),
            node ? node->GetDefaultAddress() : "<null>");

        if (node) {
            if (node->GetAggregatedState() != ENodeState::Offline) {
                YT_LOG_ALERT("Destroying chunk location of a non-offline node (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
                    location->GetId(),
                    location->GetUuid(),
                    node->GetDefaultAddress());
            }
            node->RemoveRealChunkLocation(location);
            location->SetNode(nullptr);
        }

        UnregisterChunkLocationUuid(location->GetUuid());
    }

    const TChunkLocationUuidMap& ChunkLocationUuidMap() const override
    {
        return ChunkLocationUuidToLocation_;
    }

    const TChunkLocationUuidMap& ChunkLocationUuidMapShard(int shardIndex) const override
    {
        return ShardedChunkLocationUuidToLocation_[shardIndex];
    }

private:
    const TAsyncSemaphorePtr FullHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    NHydra::TEntityMap<TRealChunkLocation> ChunkLocationMap_;
    TChunkLocationUuidMap ChunkLocationUuidToLocation_;
    std::array<TChunkLocationUuidMap, ChunkLocationShardCount> ShardedChunkLocationUuidToLocation_;

    THashMap<TChunkLocationUuid, TError> LocationAlerts_;

    struct TFullHeartbeatRequest
        : public TRefCounted
    {
        TReqFullHeartbeat NonSequoiaRequest;
        TRspFullHeartbeat NonSequoiaResponse;

        TReqModifyReplicas SequoiaRequest;
        TRspModifyReplicas SequoiaResponse;
    };

    struct TIncrementalHeartbeatRequest
        : public TRefCounted
    {
        TReqIncrementalHeartbeat NonSequoiaRequest;
        TRspIncrementalHeartbeat NonSequoiaResponse;

        TReqModifyReplicas SequoiaRequest;
        TRspModifyReplicas SequoiaResponse;
    };

    TChunkLocationUuidMap& GetChunkLocationShard(TChunkLocationUuid uuid)
    {
        auto shardIndex = GetChunkLocationShardIndex(uuid);
        return ShardedChunkLocationUuidToLocation_[shardIndex];
    }

    void RegisterChunkLocationUuid(TRealChunkLocation* location)
    {
        auto uuid = location->GetUuid();
        EmplaceOrCrash(ChunkLocationUuidToLocation_, uuid, location);
        auto& shard = GetChunkLocationShard(uuid);
        EmplaceOrCrash(shard, uuid, location);
    }

    void UnregisterChunkLocationUuid(TChunkLocationUuid uuid)
    {
        EraseOrCrash(ChunkLocationUuidToLocation_, uuid);
        auto& shard = GetChunkLocationShard(uuid);
        EraseOrCrash(shard, uuid);
    }

    std::vector<TError> GetAlerts() const
    {
        std::vector<TError> alerts;
        alerts.reserve(LocationAlerts_.size());
        std::transform(
            LocationAlerts_.begin(),
            LocationAlerts_.end(),
            std::back_inserter(alerts),
            [] (const auto& alert) {
                return alert.second;
            }
        );
        return alerts;
    }

    void ValidateLocationDirectory(
        const TNode* node,
        const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& protoDirectory,
        bool fullHeartbeat)
    {
        auto locationDirectory = FromProto<TChunkLocationDirectory>(protoDirectory);
        YT_ASSERT(locationDirectory.IsValid());

        TCompactVector<TChunkLocation*, TypicalChunkLocationCount> locations;

        for (auto uuid : locationDirectory.Uuids()) {
            auto* location = FindChunkLocationByUuid(uuid);
            if (!location) {
                YT_LOG_ALERT(
                    "Data node reported %v heartbeat with invalid location directory: "
                    "location does not exist (NodeAddress: %v, LocationUuid: %v)",
                    fullHeartbeat ? "full" : "incremental",
                    node->GetDefaultAddress(),
                    uuid);
                THROW_ERROR_EXCEPTION(
                    "Heartbeats with unknown location in location directory are invalid")
                    << TErrorAttribute("location_uuid", uuid);
            }

            auto* locationNode = location->GetNode();

            if (!locationNode) {
                YT_LOG_ALERT(
                    "Data node reported %v heartbeat with invalid location directory: "
                    "location does not have owning node "
                    "(NodeAddress: %v, LocationUuid: %v)",
                    fullHeartbeat ? "full" : "incremental",
                    node->GetDefaultAddress(),
                    uuid);
                THROW_ERROR_EXCEPTION(
                    "Heartbeats with dangling locations in location directory are invalid")
                    << TErrorAttribute("location_uuid", uuid);
            }

            if (locationNode != node) {
                YT_LOG_ALERT(
                    "Data node reported %v heartbeat with invalid location directory: "
                    "location belongs to another node "
                    "(NodeAddress: %v, LocationUuid: %v, AnotherNodeAddress: %v)",
                    fullHeartbeat ? "full" : "incremental",
                    node->GetDefaultAddress(),
                    uuid,
                    locationNode->GetDefaultAddress());
                THROW_ERROR_EXCEPTION(
                    "Heartbeat's location directory cannot contain location which belongs to other node")
                    << TErrorAttribute("location_uuid", uuid)
                    << TErrorAttribute("node", locationNode->GetDefaultAddress());
            }

            locations.push_back(location);
        }
    }

    void HydraIncrementalDataNodeHeartbeat(
        const TCtxIncrementalHeartbeatPtr& /*context*/,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (!node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process an incremental data node heartbeat until full data node heartbeat is sent");
        }

        ValidateLocationDirectory(
            node,
            request->location_directory(),
            /*fullHeartbeat*/ false);

        YT_PROFILE_TIMING("/node_tracker/incremental_data_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing incremental data node heartbeat (NodeId: %v, Address: %v, State: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState());

            nodeTracker->UpdateLastSeenTime(node);

            ProcessIncrementalHeartbeat(node, request, response);
        }
    }

    void HydraFullDataNodeHeartbeat(
        const TCtxFullHeartbeatPtr& /*context*/,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response)
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Full data node heartbeat is already sent");
        }

        ValidateLocationDirectory(
            node,
            request->location_directory(),
            /*fullHeartbeat*/ true);

        YT_PROFILE_TIMING("/node_tracker/full_data_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing full data node heartbeat (NodeId: %v, Address: %v, State: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState());

            nodeTracker->UpdateLastSeenTime(node);

            ProcessFullHeartbeat(node, request, response);
        }
    }

    void OnNodeUnregistered(TNode* node)
    {
        for (auto* location : node->RealChunkLocations()) {
            location->SetState(EChunkLocationState::Offline);
        }
    }

    void OnNodeZombified(TNode* node)
    {
        auto& realLocations = node->RealChunkLocations();
        for (auto* location : realLocations) {
            location->SetNode(nullptr);
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* location : realLocations) {
                objectManager->RemoveObject(location);
            }
        }

        node->ClearChunkLocations();
    }

    void UpdateLocationDiskFamilyAlert(TRealChunkLocation* location)
    {
        int mediumIndex = location->Statistics().medium_index();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        auto locationUuid = location->GetUuid();
        if (!medium) {
            YT_LOG_ALERT("Location medium is unknown (LocationUuid: %v, MediumIndex: %v)",
                locationUuid,
                mediumIndex);
            return;
        }
        if (medium->IsOffshore()) {
            YT_LOG_ALERT(
                "Location medium is offshore "
                "(LocationUuid: %v, MediumIndex: %v, MediumName: %v, MediumType: %v)",
                locationUuid,
                medium->GetIndex(),
                medium->GetName(),
                medium->GetType());
            return;
        }

        const auto& diskFamilyWhitelist = medium->AsDomestic()->DiskFamilyWhitelist();
        auto diskFamily = location->Statistics().disk_family();
        if (diskFamilyWhitelist &&
            !std::binary_search(
                diskFamilyWhitelist->begin(),
                diskFamilyWhitelist->end(),
                diskFamily))
        {
            YT_LOG_ALERT("Inconsistent medium (LocationUuid: %v, DiskFamily: %v, Medium: %v, DiskFamilyWhitelist: %v)",
                locationUuid,
                medium->GetName(),
                diskFamilyWhitelist,
                diskFamily);
            LocationAlerts_[locationUuid] = TError("Inconsistent medium")
                << TErrorAttribute("location_uuid", locationUuid)
                << TErrorAttribute("medium_name", medium->GetName())
                << TErrorAttribute("disk_family_whitelist", diskFamilyWhitelist)
                << TErrorAttribute("disk_family", diskFamily);
        } else {
            LocationAlerts_.erase(locationUuid);
        }
    }

    void PopulateChunkLocationStatistics(TNode* node, const auto& statistics)
    {
        for (const auto& chunkLocationStatistics : statistics) {
            auto locationUuid = FromProto<TChunkLocationUuid>(chunkLocationStatistics.location_uuid());
            auto* location = FindChunkLocationByUuid(locationUuid);
            if (!IsObjectAlive(location)) {
                YT_LOG_ALERT("Node reports statistics for non-existing chunk location (NodeAddress: %v, LocationUuid: %v)",
                    node->GetDefaultAddress(),
                    locationUuid);
                continue;
            }
            location->Statistics() = chunkLocationStatistics;
            UpdateLocationDiskFamilyAlert(location);
        }
    }

    template <class TRequestPtr, class TContextPtr>
    void CommitMutationWithSemaphore(
        std::unique_ptr<TMutation> mutation,
        TContextPtr context,
        TRequestPtr preparedRequest,
        const TAsyncSemaphorePtr& semaphore)
    {
        auto timeBefore = NProfiling::GetInstant();

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto expectedMutationCommitDuration = config->CellMaster->ExpectedMutationCommitDuration;

        semaphore->AsyncAcquire().SubscribeUnique(
            BIND([=, mutation = std::move(mutation), context = std::move(context), preparedRequest = std::move(preparedRequest)] (TErrorOr<TAsyncSemaphoreGuard>&& guardOrError) {
                if (!guardOrError.IsOK()) {
                    context->Reply(TError("Failed to acquire semaphore") << guardOrError);
                    return;
                }

                auto requestTimeout = context->GetTimeout();
                auto timeAfter = NProfiling::GetInstant();
                if (requestTimeout && timeAfter + expectedMutationCommitDuration >= timeBefore + *requestTimeout) {
                    context->Reply(TError(NYT::EErrorCode::Timeout, "Semaphore acquisition took too long"));
                    return;
                }

                auto result = WaitFor(mutation->Commit());
                if (!result.IsOK()) {
                    context->Reply(result);
                    return;
                }

                auto* response = &context->Response();
                response->CopyFrom(std::move(preparedRequest->NonSequoiaResponse));
                context->Reply();
            }).Via(EpochAutomatonInvoker_));
    }

    const TDynamicDataNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->DataNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        FullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentFullHeartbeats);
        IncrementalHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentIncrementalHeartbeats);
    }

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        ChunkLocationMap_.Clear();
        ChunkLocationUuidToLocation_.clear();
        for (auto& shard : ShardedChunkLocationUuidToLocation_) {
            shard.clear();
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ChunkLocationMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        ChunkLocationMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkLocationMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        ChunkLocationMap_.LoadValues(context);
    }

    TChunkLocationId ChunkLocationIdFromUuid(TChunkLocationUuid uuid)
    {
        auto id = ReplaceCellTagInId(
            ReplaceTypeInId(uuid, EObjectType::ChunkLocation),
            Bootstrap_->GetPrimaryCellTag());
        id.Parts32[3] &= 0x3fff;
        return id;
    }

    void OnEpochFinished()
    {
        for (auto [locationId, location] : ChunkLocationMap_) {
            location->SetBeingDisposed(false);
        }
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        OnEpochFinished();
    }

    void OnStopFollowing() override
    {
        TMasterAutomatonPart::OnStopFollowing();

        OnEpochFinished();
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (auto [locationId, location] : ChunkLocationMap_) {
            RegisterChunkLocationUuid(location);
        }
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* object : GetValuesSortedByKey(ChunkLocationMap_)) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(object, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* object : GetValuesSortedByKey(ChunkLocationMap_)) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(object, cellTag);
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TDataNodeTracker, ChunkLocation, TRealChunkLocation, ChunkLocationMap_);

////////////////////////////////////////////////////////////////////////////////

IDataNodeTrackerPtr CreateDataNodeTracker(TBootstrap* bootstrap)
{
    return New<TDataNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

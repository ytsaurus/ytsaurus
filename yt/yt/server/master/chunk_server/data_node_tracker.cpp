#include "data_node_tracker.h"

#include "data_node_tracker_internal.h"
#include "chunk_manager.h"
#include "chunk_location.h"
#include "chunk_location_type_handler.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

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

using NNodeTrackerClient::NProto::TReqRegisterNode;
using NNodeTrackerClient::NProto::TRspRegisterNode;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTracker
    : public IDataNodeTracker
    , public IDataNodeTrackerInternal
    , public TMasterAutomatonPart
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(
        TNode* node,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response),
        FullHeartbeat);
    DEFINE_SIGNAL_OVERRIDE(void(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response),
        IncrementalHeartbeat);

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
        configManager->SubscribeConfigChanged(BIND(&TDataNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeNodeUnregistered(BIND(&TDataNodeTracker::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeZombified(BIND(&TDataNodeTracker::OnNodeZombified, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChunkLocationTypeHandler(Bootstrap_, this));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TDataNodeTracker::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TDataNodeTracker::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }
    }

    void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TDataNodeTracker::HydraFullDataNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), FullHeartbeatSemaphore_);
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

        FullHeartbeat_.Fire(node, request, response);
    }

    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TDataNodeTracker::HydraIncrementalDataNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), IncrementalHeartbeatSemaphore_);
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
            response->set_disable_write_sessions(node->GetDisableWriteSessions());
            node->SetDisableWriteSessionsSentToNode(node->GetDisableWriteSessions());
        }

        IncrementalHeartbeat_.Fire(node, request, response);
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
                        THROW_ERROR_EXCEPTION("Cannot register node %Qv: there is a registered node %Qv with the same location uuid %v",
                            address,
                            existingNode->GetDefaultAddress(),
                            locationUuid);
                    }
                }
            }
        }
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
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), ex, "Failed to create chunk location for a node (NodeAddress: %v, LocationUuid: %v)",
                        node->GetDefaultAddress(),
                        locationUuid);
                    throw;
                }
            }
        }

        for (auto* location : node->ChunkLocations()) {
            location->SetState(EChunkLocationState::Dangling);
        }

        for (auto locationUuid : chunkLocationUuids) {
            auto* location = FindChunkLocationByUuid(locationUuid);
            if (!IsObjectAlive(location)) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Missing chunk location for node (NodeAddress: %v, LocationUuid: %v)",
                    node->GetDefaultAddress(),
                    locationUuid);
                return;
            }

            if (auto* existingNode = location->GetNode(); IsObjectAlive(existingNode)) {
                if (existingNode != node) {
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Chunk location is already bound to another node (NodeAddress: %v, LocationUuid: %v, BoundNodeAddress: %v)",
                        node->GetDefaultAddress(),
                        locationUuid,
                        existingNode->GetDefaultAddress());
                    return;
                }
            } else {
                location->SetNode(node);
                node->ChunkLocations().push_back(location);
            }

            location->SetState(EChunkLocationState::Online);
        }

        if (Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            auto* dataNodeInfoExt = response->MutableExtension(NNodeTrackerClient::NProto::TDataNodeInfoExt::data_node_info_ext);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            SerializeMediumDirectory(dataNodeInfoExt->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, dataNodeInfoExt->mutable_medium_overrides());
        }
    }


    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChunkLocation, TChunkLocation)

    TEntityMap<TChunkLocation>* MutableChunkLocations() override
    {
        return &ChunkLocationMap_;
    }

    TChunkLocation* FindChunkLocationByUuid(TChunkLocationUuid locationUuid) override
    {
        auto it = ChunkLocationUuidToLocation_.find(locationUuid);
        return it == ChunkLocationUuidToLocation_.end() ? nullptr : it->second;
    }

    TChunkLocation* CreateChunkLocation(
        TChunkLocationUuid locationUuid,
        TObjectId hintId) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto locationId = objectManager->GenerateId(EObjectType::ChunkLocation, hintId);

        auto locationHolder = TPoolAllocator::New<TChunkLocation>(locationId);
        auto* location = ChunkLocationMap_.Insert(locationId, std::move(locationHolder));
        location->SetUuid(locationUuid);

        if (Bootstrap_->IsSecondaryMaster()) {
            location->SetForeign();
        }

        objectManager->RefObject(location);

        EmplaceOrCrash(ChunkLocationUuidToLocation_, locationUuid, location);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk location created (LocationId: %v, LocationUuid: %v)",
            locationId,
            locationUuid);

        return location;
    }

    void DestroyChunkLocation(TChunkLocation* location) override
    {
        auto* node = location->GetNode();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk location destroyed (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
            location->GetId(),
            location->GetUuid(),
            node ? node->GetDefaultAddress() : "<null>");

        if (node) {
            if (node->GetAggregatedState() != ENodeState::Offline) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Destroying chunk location of a non-offline node (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
                    location->GetId(),
                    location->GetUuid(),
                    node->GetDefaultAddress());
            }
            std::erase(node->ChunkLocations(), location);
            location->SetNode(nullptr);
        }

        EraseOrCrash(ChunkLocationUuidToLocation_, location->GetUuid());
    }

private:
    const TAsyncSemaphorePtr FullHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    // COMPAT(babenko)
    bool NeedCreateChunkLocations_ = false;
    NHydra::TEntityMap<TChunkLocation> ChunkLocationMap_;
    THashMap<TChunkLocationUuid, TChunkLocation*> ChunkLocationUuidToLocation_;


    void HydraIncrementalDataNodeHeartbeat(
        const TCtxIncrementalHeartbeatPtr& /*context*/,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (!node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process an incremental data node heartbeat until full data node heartbeat is sent");
        }

        YT_PROFILE_TIMING("/node_tracker/incremental_data_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing incremental data node heartbeat (NodeId: %v, Address: %v, State: %v, %v",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            nodeTracker->UpdateLastSeenTime(node);

            ProcessIncrementalHeartbeat(node, request, response);
        }
    }

    void HydraFullDataNodeHeartbeat(
        const TCtxFullHeartbeatPtr& /*context*/,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Full data node heartbeat is already sent");
        }

        YT_PROFILE_TIMING("/node_tracker/full_data_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing full data node heartbeat (NodeId: %v, Address: %v, State: %v, %v",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            nodeTracker->UpdateLastSeenTime(node);

            ProcessFullHeartbeat(node, request, response);
        }
    }

    void OnNodeUnregistered(TNode* node)
    {
        for (auto* location : node->ChunkLocations()) {
            location->SetState(EChunkLocationState::Offline);
        }
    }

    void OnNodeZombified(TNode* node)
    {
        auto locations = std::exchange(node->ChunkLocations(), {});
        for (auto* location : locations) {
            location->SetNode(nullptr);
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* location : locations) {
                objectManager->RemoveObject(location);
            }
        }
    }

    void PopulateChunkLocationStatistics(TNode* node, const auto& statistics)
    {
        for (const auto& chunkLocationStatistics : statistics) {
            auto locationUuid = FromProto<TChunkLocationUuid>(chunkLocationStatistics.location_uuid());
            auto* location = FindChunkLocationByUuid(locationUuid);
            if (!IsObjectAlive(location)) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Node reports statistics for non-existing chunk location (NodeAddress: %v, LocationUuid: %v)",
                    node->GetDefaultAddress(),
                    locationUuid);
                continue;
            }
            location->Statistics() = chunkLocationStatistics;
        }
    }

    void CommitMutationWithSemaphore(
        std::unique_ptr<TMutation> mutation,
        NRpc::IServiceContextPtr context,
        const TAsyncSemaphorePtr& semaphore)
    {
        auto handler = BIND([mutation = std::move(mutation), context = std::move(context)] (TAsyncSemaphoreGuard) {
            Y_UNUSED(WaitFor(mutation->CommitAndReply(context)));
        });

        semaphore->AsyncAcquire(handler, EpochAutomatonInvoker_);
    }

    const TDynamicDataNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->DataNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        FullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentFullHeartbeats);
        IncrementalHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentIncrementalHeartbeats);
    }



    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        ChunkLocationMap_.Clear();
        ChunkLocationUuidToLocation_.clear();
        NeedCreateChunkLocations_ = true;
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
        // NB: This is just "false" since we did not have DataNodeTracker.* snapshot parts
        // prior to EMasterReign::ChunkLocation.
        NeedCreateChunkLocations_ = context.GetVersion() < EMasterReign::ChunkLocation;
    }

    TChunkLocationId ChunkLocationIdFromUuid(TChunkLocationUuid uuid)
    {
        auto id = ReplaceCellTagInId(
            ReplaceTypeInId(uuid, EObjectType::ChunkLocation),
            Bootstrap_->GetPrimaryCellTag());
        id.Parts32[3] &= 0x3fff;
        return id;
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (auto [locationId, location] : ChunkLocationMap_) {
            EmplaceOrCrash(ChunkLocationUuidToLocation_, location->GetUuid(), location);
        }

        // COMPAT(babenko)
        if (NeedCreateChunkLocations_) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            for (auto [nodeId, node] : nodeTracker->Nodes()) {
                for (auto locationUuid : node->CompatChunkLocationUuids()) {
                    auto locationId = ChunkLocationIdFromUuid(locationUuid);
                    auto* location = CreateChunkLocation(locationUuid, locationId);
                    location->SetNode(node);
                    node->ChunkLocations().push_back(location);
                    YT_LOG_DEBUG("Chunk location migrated to object (LocationUuid: %v, LocationId: %v, NodeAddress: %v)",
                        locationUuid,
                        locationId,
                        node->GetDefaultAddress());
                }
            }
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

DEFINE_ENTITY_MAP_ACCESSORS(TDataNodeTracker, ChunkLocation, TChunkLocation, ChunkLocationMap_)

////////////////////////////////////////////////////////////////////////////////

IDataNodeTrackerPtr CreateDataNodeTracker(TBootstrap* bootstrap)
{
    return New<TDataNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

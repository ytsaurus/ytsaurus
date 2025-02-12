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
#include <yt/yt/server/master/chunk_server/chunk_replica_fetcher.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>
#include <yt/yt/server/master/chunk_server/proto/data_node_tracker.pb.h>

#include <yt/yt/server/master/object_server/object_service.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>
#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>
#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <ranges>

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

static constexpr auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

int GetChunkLocationShardIndex(TChunkLocationUuid uuid)
{
    return GetShardIndex<ChunkLocationShardCount>(uuid);
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
        RegisterMethod(BIND_NO_PROPAGATE(&TDataNodeTracker::HydraIncrementalDataNodeHeartbeat, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TDataNodeTracker::HydraFullDataNodeHeartbeat, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TDataNodeTracker::HydraProcessLocationFullHeartbeat, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TDataNodeTracker::HydraFinalizeFullHeartbeatSession, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TDataNodeTracker::HydraCleanExpiredDanglingLocations, Unretained(this)));

        RegisterLoader(
            "DataNodeTracker.Keys",
            BIND_NO_PROPAGATE(&TDataNodeTracker::LoadKeys, Unretained(this)));
        RegisterLoader(
            "DataNodeTracker.Values",
            BIND_NO_PROPAGATE(&TDataNodeTracker::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "DataNodeTracker.Keys",
            BIND_NO_PROPAGATE(&TDataNodeTracker::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "DataNodeTracker.Values",
            BIND_NO_PROPAGATE(&TDataNodeTracker::SaveValues, Unretained(this)));
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

    // COMPAT(danilalexeev): YT-23781.
    template <class TFullHeartbeatContextPtr>
    void DoProcessFullHeartbeat(TFullHeartbeatContextPtr context, TRange<TChunkLocation*> locationDirectory)
    {
        static_assert(
            std::is_same_v<TFullHeartbeatContextPtr, TCtxFullHeartbeatPtr> ||
            std::is_same_v<TFullHeartbeatContextPtr, TCtxLocationFullHeartbeatPtr>);

        using TFullHeartbeatRequest = TFullHeartbeatRequest<
            typename TFullHeartbeatContextPtr::TUnderlying::TTypedRequest::TMessage,
            typename TFullHeartbeatContextPtr::TUnderlying::TTypedResponse::TMessage>;

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        const auto& originalRequest = context->Request();
        THashSet<int> sequoiaLocationIndices;
        for (int i = 0; i < std::ssize(locationDirectory); ++i) {
            if (chunkReplicaFetcher->CanHaveSequoiaReplicas(locationDirectory[i])) {
                InsertOrCrash(sequoiaLocationIndices, i);
            }
        }

        const auto& sequoiaChunkReplicasConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas;
        auto isSequoiaEnabled = sequoiaChunkReplicasConfig->Enable;
        auto sequoiaChunkProbability = sequoiaChunkReplicasConfig->ReplicasPercentage;

        auto splitRequest = BIND([&, sequoiaLocationIndices = std::move(sequoiaLocationIndices)] {
            auto preparedRequest = NewWithOffloadedDtor<TFullHeartbeatRequest>(NRpc::TDispatcher::Get()->GetHeavyInvoker());
            preparedRequest->NonSequoiaRequest.CopyFrom(originalRequest);
            if (!isSequoiaEnabled) {
                return preparedRequest;
            }

            preparedRequest->NonSequoiaRequest.mutable_chunks()->Clear();

            preparedRequest->SequoiaRequest->set_node_id(originalRequest.node_id());
            for (auto* location : locationDirectory) {
                ToProto(preparedRequest->SequoiaRequest->add_location_directory(), location->GetUuid());
            }

            for (const auto& chunkInfo : originalRequest.chunks()) {
                auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
                auto locationIndex = chunkInfo.location_index();
                if (sequoiaLocationIndices.contains(locationIndex) && chunkReplicaFetcher->CanHaveSequoiaReplicas(chunkIdWithIndex.Id, sequoiaChunkProbability)) {
                    *preparedRequest->SequoiaRequest->add_added_chunks() = chunkInfo;
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

        if (preparedRequest->SequoiaRequest->removed_chunks_size() + preparedRequest->SequoiaRequest->added_chunks_size() > 0) {
            auto modifyDeadChunks = BIND([&] {
                for (const auto& protoChunkId : preparedRequest->SequoiaRequest->removed_chunks()) {
                    if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                        *preparedRequest->SequoiaRequest->add_dead_chunk_ids() = protoChunkId.chunk_id();
                    }
                }

                for (const auto& protoChunkId : preparedRequest->SequoiaRequest->added_chunks()) {
                    if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                        *preparedRequest->SequoiaRequest->add_dead_chunk_ids() = protoChunkId.chunk_id();
                    }
                }
            });

            const auto& objectService = Bootstrap_->GetObjectService();
            WaitFor(modifyDeadChunks
                .AsyncVia(objectService->GetLocalReadOffloadInvoker())
                .Run())
                .ThrowOnError();

            WaitFor(chunkManager->ModifySequoiaReplicas(std::move(preparedRequest->SequoiaRequest)))
                .ThrowOnError();
        }

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        hydraFacade->CommitMutationWithSemaphore(
            std::is_same_v<TFullHeartbeatContextPtr, TCtxFullHeartbeatPtr>
                ? FullHeartbeatSemaphore_
                : LocationFullHeartbeatSemaphore_,
            context,
            BIND([=, this, this_ = MakeStrong(this)] {
                return CreateMutation(
                    Bootstrap_->GetHydraFacade()->GetHydraManager(),
                    &preparedRequest->NonSequoiaRequest,
                    &preparedRequest->NonSequoiaResponse,
                    [&] {
                        if constexpr (std::is_same_v<TFullHeartbeatContextPtr, TCtxFullHeartbeatPtr>) {
                            return &TDataNodeTracker::HydraFullDataNodeHeartbeat;
                        } else {
                            return &TDataNodeTracker::HydraProcessLocationFullHeartbeat;
                        }
                    }(),
                    this);
            }),
            BIND([=] (const TMutationResponse& /*response*/) {
                auto* response = &context->Response();
                response->Swap(&preparedRequest->NonSequoiaResponse);
                context->Reply();
            }));
    }

    // COMPAT(danilalexeev): YT-23781.
    void ProcessFullHeartbeat(
        const TNode* node,
        const TCtxFullHeartbeatPtr& context) override
    {
        auto locationDirectory = ParseLocationDirectoryOrThrow(node, this, context->Request());
        DoProcessFullHeartbeat(context, locationDirectory);
    }

    void ProcessLocationFullHeartbeat(
        const TNode* node,
        const TCtxLocationFullHeartbeatPtr& context) override
    {
        if (!GetDynamicConfig()->EnablePerLocationFullHeartbeats) {
            THROW_ERROR_EXCEPTION("Per-location full data node heartbeats are disabled");
        }

        auto* location = ParseLocationOrThrow(node, this, context->Request());
        DoProcessFullHeartbeat(context, {location});
    }

    void FinalizeFullHeartbeatSession(
        const TNode* /*node*/,
        const TCtxFinalizeFullHeartbeatSessionPtr& context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TDataNodeTracker::HydraFinalizeFullHeartbeatSession,
            this);
        mutation->SetCurrentTraceContext();

        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    // COMPAT(danilalexeev): YT-23781.
    void ProcessFullHeartbeat(
        TNode* node,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response)
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
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        const auto& originalRequest = context->Request();
        auto nodeId = FromProto<TNodeId>(originalRequest.node_id());
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);
        auto locationDirectory = ParseLocationDirectoryOrThrow(node, this, originalRequest);
        THashSet<int> sequoiaLocationIndices;
        for (int i = 0; i < std::ssize(locationDirectory); ++i) {
            if (chunkReplicaFetcher->CanHaveSequoiaReplicas(locationDirectory[i])) {
                InsertOrCrash(sequoiaLocationIndices, i);
            }
        }

        const auto& sequoiaChunkReplicasConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas;
        auto isSequoiaEnabled = sequoiaChunkReplicasConfig->Enable;
        auto sequoiaChunkProbability = sequoiaChunkReplicasConfig->ReplicasPercentage;

        auto splitRequest = BIND([=, sequoiaLocationIndices = std::move(sequoiaLocationIndices)] {
            auto preparedRequest = NewWithOffloadedDtor<TIncrementalHeartbeatRequest>(NRpc::TDispatcher::Get()->GetHeavyInvoker());
            preparedRequest->NonSequoiaRequest.CopyFrom(originalRequest);
            if (!isSequoiaEnabled) {
                return preparedRequest;
            }

            preparedRequest->NonSequoiaRequest.mutable_added_chunks()->Clear();
            preparedRequest->NonSequoiaRequest.mutable_removed_chunks()->Clear();

            preparedRequest->SequoiaRequest->set_node_id(originalRequest.node_id());
            preparedRequest->SequoiaRequest->mutable_location_directory()->CopyFrom(originalRequest.location_directory());

            for (const auto& chunkInfo : originalRequest.added_chunks()) {
                auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
                auto locationIndex = chunkInfo.location_index();
                if (sequoiaLocationIndices.contains(locationIndex) && chunkReplicaFetcher->CanHaveSequoiaReplicas(chunkIdWithIndex.Id, sequoiaChunkProbability)) {
                    *preparedRequest->SequoiaRequest->add_added_chunks() = chunkInfo;
                } else {
                    *preparedRequest->NonSequoiaRequest.add_added_chunks() = chunkInfo;
                }
            }

            for (const auto& chunkInfo : originalRequest.removed_chunks()) {
                auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
                auto locationIndex = chunkInfo.location_index();
                if (sequoiaLocationIndices.contains(locationIndex) && chunkReplicaFetcher->CanHaveSequoiaReplicas(chunkIdWithIndex.Id, sequoiaChunkProbability)) {
                    *preparedRequest->SequoiaRequest->add_removed_chunks() = chunkInfo;
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

        if (preparedRequest->SequoiaRequest->removed_chunks_size() + preparedRequest->SequoiaRequest->added_chunks_size() > 0) {
            auto modifyDeadChunks = BIND([&] {
                for (const auto& protoChunkId : preparedRequest->SequoiaRequest->removed_chunks()) {
                    if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                        *preparedRequest->SequoiaRequest->add_dead_chunk_ids() = protoChunkId.chunk_id();
                    }
                }

                for (const auto& protoChunkId : preparedRequest->SequoiaRequest->added_chunks()) {
                    if (!IsObjectAlive(chunkManager->FindChunk(FromProto<TChunkId>(protoChunkId.chunk_id())))) {
                        *preparedRequest->SequoiaRequest->add_dead_chunk_ids() = protoChunkId.chunk_id();
                    }
                }
            });

            const auto& objectService = Bootstrap_->GetObjectService();
            WaitFor(modifyDeadChunks
                .AsyncVia(objectService->GetLocalReadOffloadInvoker())
                .Run())
                .ThrowOnError();

            WaitFor(chunkManager->ModifySequoiaReplicas(std::move(preparedRequest->SequoiaRequest)))
                .ThrowOnError();
        }

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        hydraFacade->CommitMutationWithSemaphore(
            IncrementalHeartbeatSemaphore_,
            context,
            BIND([=, this, this_ = MakeStrong(this)] {
                return CreateMutation(
                    Bootstrap_->GetHydraFacade()->GetHydraManager(),
                    &preparedRequest->NonSequoiaRequest,
                    &preparedRequest->NonSequoiaResponse,
                    &TDataNodeTracker::HydraIncrementalDataNodeHeartbeat,
                    this);
            }),
            BIND([=] (const TMutationResponse& /*response*/) {
                auto* response = &context->Response();
                response->Swap(&preparedRequest->NonSequoiaResponse);
                context->Reply();
            }));
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
        const std::string& address,
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
                if (auto existingNode = existingLocation->GetNode(); IsObjectAlive(existingNode)) {
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
        auto isPrimaryMaster = Bootstrap_->IsPrimaryMaster();

        if (isPrimaryMaster) {
            for (auto locationUuid : chunkLocationUuids) {
                if (IsObjectAlive(FindChunkLocationByUuid(locationUuid))) {
                    continue;
                }

                auto req = TMasterYPathProxy::CreateObject();
                req->set_type(ToProto(EObjectType::ChunkLocation));

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

        ReplicateChunkLocations(node, chunkLocationUuids);
        MakeLocationsOnline(node);

        if (isPrimaryMaster) {
            auto* dataNodeInfoExt = response->MutableExtension(NNodeTrackerClient::NProto::TDataNodeInfoExt::data_node_info_ext);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            SerializeMediumDirectory(dataNodeInfoExt->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, dataNodeInfoExt->mutable_medium_overrides());

            dataNodeInfoExt->set_require_location_uuids(false);
            dataNodeInfoExt->set_per_location_full_heartbeats_enabled(GetDynamicConfig()->EnablePerLocationFullHeartbeats);
        }
    }

    void ReplicateChunkLocations(
        TNode* node,
        const std::vector<TChunkLocationUuid>& chunkLocationUuids) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        for (auto location : node->ChunkLocations()) {
            location->SetState(EChunkLocationState::Dangling);
            location->SetNode(nullptr);
        }
        node->ChunkLocations().clear();

        for (auto locationUuid : chunkLocationUuids) {
            auto* location = FindChunkLocationByUuid(locationUuid);
            if (!IsObjectAlive(location)) {
                YT_LOG_ALERT(
                    "Missing chunk location for node (NodeAddress: %v, LocationUuid: %v)",
                    node->GetDefaultAddress(),
                    locationUuid);
                continue;
            }

            if (auto existingNode = location->GetNode(); IsObjectAlive(existingNode) && existingNode != node) {
                // It was already checked in DataNodeTracker::ValidateRegisterNode().
                YT_LOG_FATAL("Chunk location is already bound to another node (NodeAddress: %v, LocationUuid: %v, BoundNodeAddress: %v)",
                    node->GetDefaultAddress(),
                    locationUuid,
                    existingNode->GetDefaultAddress());
            } else {
                // Location is not dangling anymore.
                auto mutationContext = GetCurrentMutationContext();

                location->SetNode(node);
                location->SetState(EChunkLocationState::Offline);
                location->SetLastSeenTime(mutationContext->GetTimestamp());
                node->ChunkLocations().push_back(location);
            }
        }

        node->ChunkLocations().shrink_to_fit();
    }

    void MakeLocationsOnline(TNode* node) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        for (auto location : node->ChunkLocations()) {
            YT_VERIFY(location->GetState() == EChunkLocationState::Offline);
            location->SetState(EChunkLocationState::Online);
        }
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChunkLocation, TChunkLocation);

    TEntityMap<TChunkLocation>* MutableChunkLocations() override
    {
        return &ChunkLocationMap_;
    }

    TChunkLocation* FindChunkLocationByUuid(TChunkLocationUuid locationUuid) override
    {
        auto it = ChunkLocationUuidToLocation_.find(locationUuid);
        return it == ChunkLocationUuidToLocation_.end() ? nullptr : it->second;
    }

    TChunkLocation* GetChunkLocationByUuid(TChunkLocationUuid locationUuid) override
    {
        return GetOrCrash(ChunkLocationUuidToLocation_, locationUuid);
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

        RegisterChunkLocationUuid(location);

        YT_LOG_DEBUG("Chunk location created (LocationId: %v, LocationUuid: %v)",
            locationId,
            locationUuid);

        return location;
    }

    void DestroyChunkLocation(TChunkLocation* location) override
    {
        auto node = location->GetNode();

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
            std::erase(node->ChunkLocations(), location);
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
    const TAsyncSemaphorePtr LocationFullHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    NHydra::TEntityMap<TChunkLocation> ChunkLocationMap_;
    TChunkLocationUuidMap ChunkLocationUuidToLocation_;
    std::array<TChunkLocationUuidMap, ChunkLocationShardCount> ShardedChunkLocationUuidToLocation_;

    THashMap<TChunkLocationUuid, TError> LocationAlerts_;

    TPeriodicExecutorPtr DanglingLocationsCleaningExecutor_;
    // COMPAT(koloshmet)
    TInstant DanglingLocationsDefaultLastSeenTime_;

    template <class TReqFullHeartbeat, class TRspFullHeartbeat>
    struct TFullHeartbeatRequest
        : public TRefCounted
    {
        TReqFullHeartbeat NonSequoiaRequest;
        TRspFullHeartbeat NonSequoiaResponse;

        std::unique_ptr<TReqModifyReplicas> SequoiaRequest = std::make_unique<TReqModifyReplicas>();
    };

    struct TIncrementalHeartbeatRequest
        : public TRefCounted
    {
        TReqIncrementalHeartbeat NonSequoiaRequest;
        TRspIncrementalHeartbeat NonSequoiaResponse;

        std::unique_ptr<TReqModifyReplicas> SequoiaRequest = std::make_unique<TReqModifyReplicas>();
    };

    TChunkLocationUuidMap& GetChunkLocationShard(TChunkLocationUuid uuid)
    {
        auto shardIndex = GetChunkLocationShardIndex(uuid);
        return ShardedChunkLocationUuidToLocation_[shardIndex];
    }

    void RegisterChunkLocationUuid(TChunkLocation* location)
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
        std::ranges::copy(LocationAlerts_ | std::views::values, std::back_inserter(alerts));
        return alerts;
    }

    TInstant GetChunkLocationLastSeenTime(const TChunkLocation& location) const override
    {
        if (auto lastSeen = location.GetLastSeenTime(); lastSeen != TInstant::Max()) {
            return lastSeen;
        }
        if (auto customLastSeen = GetDynamicConfig()->DanglingLocationCleaner->DefaultLastSeenTime) {
            return *customLastSeen;
        }
        return DanglingLocationsDefaultLastSeenTime_;
    }

    void ValidateLocationDirectory(
        const TNode* node,
        const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& protoDirectory,
        bool fullHeartbeat)
    {
        auto locationDirectory = FromProto<TChunkLocationDirectory>(protoDirectory);
        YT_ASSERT(locationDirectory.IsValid());

        for (auto uuid : locationDirectory.Uuids()) {
            ValidateLocationUuid(node, uuid, fullHeartbeat);
        }
    }

    void ValidateLocationUuid(
        const TNode* node,
        TGuid uuid,
        bool fullHeartbeat)
    {
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

        auto locationNode = location->GetNode();
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

    // COMPAT(danilalexeev): YT-23781.
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

    void HydraProcessLocationFullHeartbeat(
        const TCtxLocationFullHeartbeatPtr& /*context*/,
        TReqLocationFullHeartbeat* request,
        TRspLocationFullHeartbeat* response)
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

        auto locationUuid = FromProto<TChunkLocationUuid>(request->location_uuid());
        ValidateLocationUuid(
            node,
            locationUuid,
            /*fullHeartbeat*/ true);

        YT_PROFILE_TIMING("/node_tracker/data_node_location_full_heartbeat_time") {
            YT_LOG_DEBUG("Processing data node location full heartbeat"
                " (NodeId: %v, Address: %v, LocationUuid: %v, State: %v)",
                nodeId,
                node->GetDefaultAddress(),
                locationUuid,
                node->GetLocalState());

            nodeTracker->UpdateLastSeenTime(node);

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            PopulateChunkLocationStatistics(node, request->statistics());

            chunkManager->ProcessLocationFullDataNodeHeartbeat(node, request, response);
        }
    }

    void HydraFinalizeFullHeartbeatSession(
        const TCtxFinalizeFullHeartbeatSessionPtr&  /*context*/,
        TReqFinalizeFullHeartbeatSession* request,
        TRspFinalizeFullHeartbeatSession* response)
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

        YT_LOG_DEBUG("Finalizing data node full heartbeat session (NodeId: %v, Address: %v)",
            nodeId,
            node->GetDefaultAddress());

        nodeTracker->UpdateLastSeenTime(node);

        auto& statistics = *request->mutable_statistics();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        node->SetDataNodeStatistics(std::move(statistics), chunkManager);

        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Data);

        if (Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            SerializeMediumDirectory(response->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, response->mutable_medium_overrides());
        }

        chunkManager->FinalizeDataNodeFullHeartbeatSession(node);
    }

    void HydraCleanExpiredDanglingLocations(NProto::TReqRemoveDanglingChunkLocations* request)
    {
        auto now = GetCurrentMutationContext()->GetTimestamp();
        auto expirationTimeout = GetDynamicConfig()->DanglingLocationCleaner->ExpirationTimeout;

        auto danglingLocationUuids = FromProto<std::vector<TChunkLocationUuid>>(request->chunk_location_uuids());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto uuid : danglingLocationUuids) {
            auto* location = ChunkLocationMap_.Find(uuid);
            if (!IsObjectAlive(location) || location->GetState() != EChunkLocationState::Dangling) {
                continue;
            }

            if (GetChunkLocationLastSeenTime(*location) + expirationTimeout < now) {
                objectManager->RemoveObject(location);
            }
        }
    }

    void OnNodeUnregistered(TNode* node)
    {
        auto mutationContext = GetCurrentMutationContext();

        for (auto location : node->ChunkLocations()) {
            location->SetState(EChunkLocationState::Offline);
            location->SetLastSeenTime(mutationContext->GetTimestamp());
        }
    }

    void OnNodeZombified(TNode* node)
    {
        for (auto location : node->ChunkLocations()) {
            location->SetNode(nullptr);
            location->SetState(EChunkLocationState::Dangling);
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto location : node->ChunkLocations()) {
                objectManager->RemoveObject(location);
            }
        }

        node->ChunkLocations().clear();
    }

    void UpdateLocationDiskFamilyAlert(TChunkLocation* location)
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
        auto diskFamily = FromProto<std::string>(location->Statistics().disk_family());
        if (diskFamilyWhitelist && !std::ranges::binary_search(*diskFamilyWhitelist, diskFamily)) {
            YT_LOG_ALERT("Inconsistent medium (LocationUuid: %v, Medium: %v, DiskFamily: %v, DiskFamilyWhitelist: %v)",
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

    void PopulateChunkLocationStatistics(
        TNode* node,
        const NNodeTrackerClient::NProto::TChunkLocationStatistics& statistics)
    {
        auto locationUuid = FromProto<TChunkLocationUuid>(statistics.location_uuid());
        auto* location = FindChunkLocationByUuid(locationUuid);
        if (!IsObjectAlive(location)) {
            YT_LOG_ALERT("Node reports statistics for non-existing chunk location (NodeAddress: %v, LocationUuid: %v)",
                node->GetDefaultAddress(),
                locationUuid);
            THROW_ERROR_EXCEPTION(
                "Chunk statistics reports with unknown location are invalid")
                << TErrorAttribute("location_uuid", locationUuid);
        }
        location->Statistics() = statistics;
        UpdateLocationDiskFamilyAlert(location);
    }

    template <std::ranges::input_range TStatisticsRange>
        requires std::same_as<
            std::ranges::range_value_t<TStatisticsRange>,
            NNodeTrackerClient::NProto::TChunkLocationStatistics>
    void PopulateChunkLocationStatistics(TNode* node, const TStatisticsRange& statistics)
    {
        for (const auto& chunkLocationStatistics : statistics) {
            PopulateChunkLocationStatistics(node, chunkLocationStatistics);
        }
    }

    const TDynamicDataNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->DataNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        FullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentFullHeartbeats);
        LocationFullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentLocationFullHeartbeats);
        IncrementalHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentIncrementalHeartbeats);

        if (DanglingLocationsCleaningExecutor_) {
            DanglingLocationsCleaningExecutor_->SetPeriod(GetDynamicConfig()->DanglingLocationCleaner->CleanupPeriod);
        }
    }

    void CleanDanglingLocations()
    {
        if (!GetDynamicConfig()->DanglingLocationCleaner->Enable) {
            return;
        }

        auto now = NProfiling::GetInstant();
        auto expirationTimeout = GetDynamicConfig()->DanglingLocationCleaner->ExpirationTimeout;
        auto cleaningLimit = GetDynamicConfig()->DanglingLocationCleaner->MaxLocationsToCleanPerIteration;

        constexpr auto defaultCleaningLimit = TDanglingLocationCleanerConfig::DefaultMaxLocationsToCleanPerIteration;
        TCompactVector<TGuid, defaultCleaningLimit> expiredDanglingLocations;

        for (auto [id, location] : ChunkLocationMap_) {
            if (std::ssize(expiredDanglingLocations) >= cleaningLimit) {
                break;
            }

            auto dangling = location->GetState() == EChunkLocationState::Dangling;
            auto expired = GetChunkLocationLastSeenTime(*location) + expirationTimeout < now;
            if (dangling && expired) {
                expiredDanglingLocations.push_back(id);
            }
        }

        if (!expiredDanglingLocations.empty()) {
            YT_LOG_INFO("Removing dangling chunk locations (ChunkLocationIds: %v)", expiredDanglingLocations);

            NProto::TReqRemoveDanglingChunkLocations request;
            ToProto(request.mutable_chunk_location_uuids(), expiredDanglingLocations);

            YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
                ->CommitAndLog(Logger()));
        }
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
        using NYT::Save;

        ChunkLocationMap_.SaveValues(context);
        // COMPAT(koloshmet)
        Save(context, DanglingLocationsDefaultLastSeenTime_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkLocationMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        ChunkLocationMap_.LoadValues(context);
        // COMPAT(koloshmet)
        if (context.GetVersion() >= EMasterReign::DanglingLocationsCleaning) {
            Load(context, DanglingLocationsDefaultLastSeenTime_);
        } else {
            DanglingLocationsDefaultLastSeenTime_ = TInstant::Max();
        }
    }

    TChunkLocationId ChunkLocationIdFromUuid(TChunkLocationUuid uuid)
    {
        auto id = ReplaceCellTagInId(
            ReplaceTypeInId(uuid, EObjectType::ChunkLocation),
            Bootstrap_->GetPrimaryCellTag());
        id.Parts32[3] &= 0x3fff;
        return id;
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            DanglingLocationsCleaningExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TDataNodeTracker::CleanDanglingLocations, MakeWeak(this)),
                GetDynamicConfig()->DanglingLocationCleaner->CleanupPeriod);
            DanglingLocationsCleaningExecutor_->Start();
        }
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

        if (DanglingLocationsCleaningExecutor_) {
            YT_UNUSED_FUTURE(DanglingLocationsCleaningExecutor_->Stop());
            DanglingLocationsCleaningExecutor_.Reset();
        }

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

        // COMPAT(koloshmet)
        if (DanglingLocationsDefaultLastSeenTime_ == TInstant::Max()) {
            DanglingLocationsDefaultLastSeenTime_ = GetCurrentHydraContext()->GetTimestamp();
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

DEFINE_ENTITY_MAP_ACCESSORS(TDataNodeTracker, ChunkLocation, TChunkLocation, ChunkLocationMap_);

////////////////////////////////////////////////////////////////////////////////

IDataNodeTrackerPtr CreateDataNodeTracker(TBootstrap* bootstrap)
{
    return New<TDataNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

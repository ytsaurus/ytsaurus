#include "data_node_tracker.h"

#include "data_node_tracker_internal.h"
#include "domestic_medium.h"
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
#include <yt/yt/server/master/node_tracker_server/node_disposal_manager.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/helpers.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>
#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/misc/id_generator.h>

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
using namespace NSequoiaClient;
using namespace NYTree;

using NDataNodeTrackerClient::TChunkLocationDirectory;
using NNodeTrackerClient::NProto::TReqRegisterNode;
using NNodeTrackerClient::NProto::TRspRegisterNode;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

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
        RegisterMethod(BIND_NO_PROPAGATE(&TDataNodeTracker::HydraRemapChunkLocationIds, Unretained(this)));

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
        nodeTracker->SubscribeNodeRestarted(BIND_NO_PROPAGATE(&TDataNodeTracker::OnNodeRestarted, MakeWeak(this)));
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
    void DoProcessFullHeartbeat(
        const TNode* node,
        TFullHeartbeatContextPtr context,
        TRange<TChunkLocationIndex> locationDirectory)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        static_assert(
            std::is_same_v<TFullHeartbeatContextPtr, TCtxFullHeartbeatPtr> ||
            std::is_same_v<TFullHeartbeatContextPtr, TCtxLocationFullHeartbeatPtr>);

        auto preparedRequest = SplitRequest(context, locationDirectory);

        if (Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas->Enable) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();

            if constexpr (std::is_same_v<TFullHeartbeatContextPtr, TCtxLocationFullHeartbeatPtr>) {
                auto locationUuid = FromProto<TChunkLocationUuid>(preparedRequest->NonSequoiaRequest.location_uuid());
                auto* location = FindAndValidateLocation<true>(node, locationUuid);

                if ((preparedRequest->NonSequoiaRequest.is_validation() && GetDynamicConfig()->ValidateSequoiaReplicas) ||
                    location->GetState() == EChunkLocationState::Restarted ||
                    Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas->UseLocationReplacementForLocationFullHeartbeat)
                {
                    auto replaceLocationRequest = std::make_unique<TReqReplaceLocationReplicas>();
                    replaceLocationRequest->set_node_id(ToProto(node->GetId()));
                    replaceLocationRequest->set_location_index(ToProto(location->GetIndex()));
                    *replaceLocationRequest->mutable_chunks() = std::move(*preparedRequest->SequoiaRequest->mutable_added_chunks());

                    if (preparedRequest->NonSequoiaRequest.is_validation()) {
                        // We will process validation request as normal location replacement.
                        // If replicas are different for some chunks, it will be alerted.
                        // If fix_sequoia_replicas_if_replica_validation_failed flag is enabled, replicas will be fixed according to the request.
                        replaceLocationRequest->set_is_validation(true);
                    }

                    preparedRequest->SequoiaRequest.reset();

                    WaitFor(chunkManager->ReplaceSequoiaLocationReplicas(
                        ESequoiaTransactionType::FullHeartbeat,
                        std::move(replaceLocationRequest)))
                        .ThrowOnError();
                }

                if (preparedRequest->NonSequoiaRequest.is_validation()) {
                    // If Sequoia validation is enabled, we should have already validated Sequoia replicas.
                    // If Sequoia validation is disabled, we do not need to process any Sequoia replicas.
                    preparedRequest->SequoiaRequest.reset();
                }
            }

            auto& sequoiaRequest = preparedRequest->SequoiaRequest;
            // We will reset Sequoia request in case the location replacement is applied.
            if (sequoiaRequest && sequoiaRequest->removed_chunks_size() + sequoiaRequest->added_chunks_size() > 0) {

                WaitFor(chunkManager->ModifySequoiaReplicas(ESequoiaTransactionType::FullHeartbeat, std::move(sequoiaRequest)))
                    .ThrowOnError();
            }
        }

        const auto& config = GetDynamicConfig();
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        i64 chunkReplicasCount = 0;
        if constexpr (std::is_same_v<TFullHeartbeatContextPtr, TCtxFullHeartbeatPtr>) {
            for (const auto& statistics : preparedRequest->NonSequoiaRequest.per_location_chunk_counts()) {
                chunkReplicasCount += statistics.chunk_count();
            }
        } else {
            chunkReplicasCount += preparedRequest->NonSequoiaRequest.chunks_size();
        }
        auto semaphoreSlotsToAquire = config->EnableChunkReplicasThrottlingInHeartbeats ? chunkReplicasCount : 1;
        const auto& semaphore = config->EnableChunkReplicasThrottlingInHeartbeats
            ? FullHeartbeatPerReplicasSemaphore_
            : (std::is_same_v<TFullHeartbeatContextPtr, TCtxFullHeartbeatPtr>
                ? FullHeartbeatSemaphore_
                : LocationFullHeartbeatSemaphore_);

        auto mutationBuilder = BIND([=, this, this_ = MakeStrong(this)] {
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
        });
        auto replyCallback = BIND([=] (const TMutationResponse& /*response*/) {
            auto* response = &context->Response();
            response->Swap(&preparedRequest->NonSequoiaResponse);
            context->Reply();
        });
        hydraFacade->CommitMutationWithSemaphore(
            semaphore,
            std::move(context),
            std::move(mutationBuilder),
            std::move(replyCallback),
            semaphoreSlotsToAquire);
    }

    // COMPAT(danilalexeev): YT-23781.
    void ProcessFullHeartbeat(
        const TNode* node,
        const TCtxFullHeartbeatPtr& context) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (node->GetLocalState() == ENodeState::Restarted) {
            YT_LOG_ALERT("Restarted node sent full heartbeat (NodeId: %v, NodeAddress: %v)",
                node->GetId(),
                node->GetDefaultAddress());
            THROW_ERROR_EXCEPTION("Full data node heartbeats are not supported for restarted nodes, the node will be disposed for standard registration");
        }

        ValidateHeartbeatRequest(node, context->Request());
        auto locationDirectory = ParseLocationDirectory(node, context->Request());
        DoProcessFullHeartbeat(node, context, locationDirectory);
    }

    void ProcessLocationFullHeartbeat(
        const TNode* node,
        const TCtxLocationFullHeartbeatPtr& context) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (!GetDynamicConfig()->EnablePerLocationFullHeartbeats) {
            THROW_ERROR_EXCEPTION("Per-location full data node heartbeats are disabled");
        }

        ValidateHeartbeatRequest(node, context->Request());

        auto locationUuid = FromProto<TChunkLocationUuid>(context->Request().location_uuid());
        auto* location = FindAndValidateLocation<true>(node, locationUuid);

        DoProcessFullHeartbeat(node, context, {location->GetIndex()});
    }

    void FinalizeFullHeartbeatSession(
        const TNode* node,
        const TCtxFinalizeFullHeartbeatSessionPtr& context) override
    {
        auto reportedLocationUuids = GetLocationsReportedInStatistics(context->Request().statistics());

        std::vector<TFuture<TRspModifyReplicas>> sequoiaReplaceLocationReplicasFutures;

        for (const auto& location : node->ChunkLocations()) {
            if (!std::ranges::binary_search(reportedLocationUuids, location->GetUuid())) {
                // Some locations may send no location heartbeats.
                // These locations are not present in location statistics.
                // However, if these locations are restarted, we will need to clean up all chunks on them.
                if (location->GetState() == EChunkLocationState::Restarted) {
                    if (node->GetLocalState() != ENodeState::Restarted) {
                        YT_LOG_ALERT("Non restarted node has restarted location (NodeId: %v, NodeAddress: %v, NodeLocalState: %v, LocationUuid: %v)",
                            node->GetId(),
                            node->GetDefaultAddress(),
                            node->GetLocalState(),
                            location->GetUuid());
                        THROW_ERROR_EXCEPTION("Non restarted node has restarted location %v", location->GetUuid());
                    }

                    // We first clean up Sequoia chunks, and then in mutation we will clean non-Sequoia chunks.
                    if (Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas->Enable) {
                        auto replaceLocationRequest = std::make_unique<TReqReplaceLocationReplicas>();
                        replaceLocationRequest->set_node_id(ToProto(node->GetId()));
                        replaceLocationRequest->set_location_index(ToProto(location->GetIndex()));

                        auto chunkManager = Bootstrap_->GetChunkManager();
                        sequoiaReplaceLocationReplicasFutures.push_back(chunkManager->ReplaceSequoiaLocationReplicas(
                            ESequoiaTransactionType::FullHeartbeat,
                            std::move(replaceLocationRequest)));
                    }
                }
            } else if (location->GetState() != EChunkLocationState::Online) {
                // All locations that are present in statistics must be already reported in per locations heartbeats.
                // For these locations we verify that they are online.
                // The restarted locations also should be turned into online state even for disabled flag VerifyAllLocationsAreReportedInFullHeartbeats.

                // COMPAT(grphil) always throw error after all nodes are updated to fresh 25.3.
                if (GetDynamicConfig()->VerifyAllLocationsAreReportedInFullHeartbeats ||
                    location->GetState() == EChunkLocationState::Restarted) {
                    THROW_ERROR_EXCEPTION(
                        "Node has not reported all locations, location %v has %Qlv state",
                        location->GetUuid(),
                        location->GetState());
                }
            }
        }

        WaitFor(AllSet(std::move(sequoiaReplaceLocationReplicasFutures))
            .Apply(BIND([](const std::vector<TErrorOr<TRspModifyReplicas>> responses) {
                for (const auto& response : responses) {
                    response.ThrowOnError();
                }
            })))
            .ThrowOnError();

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
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        const auto& originalRequest = context->Request();
        auto nodeId = FromProto<TNodeId>(originalRequest.node_id());
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        ValidateHeartbeatRequest(node, originalRequest);
        auto locationDirectory = ParseLocationDirectory(node, originalRequest);

        auto preparedRequest = SplitRequest(context, locationDirectory);

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        if (preparedRequest->SequoiaRequest->removed_chunks_size() + preparedRequest->SequoiaRequest->added_chunks_size() > 0) {
            YT_LOG_TRACE("There are Sequoia replicas for this request (NodeId: %v)", nodeId);
            WaitFor(chunkManager->ModifySequoiaReplicas(ESequoiaTransactionType::IncrementalHeartbeat, std::move(preparedRequest->SequoiaRequest)))
                .ThrowOnError();
        } else {
            YT_LOG_TRACE("No Sequoia replicas for this request (NodeId: %v)", nodeId);
        }

        const auto& config = GetDynamicConfig();
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        auto chunkReplicasCount = preparedRequest->NonSequoiaRequest.added_chunks_size() + preparedRequest->NonSequoiaRequest.removed_chunks_size();
        auto semaphoreSlotsToAquire = config->EnableChunkReplicasThrottlingInHeartbeats ? chunkReplicasCount : 1;
        const auto& semaphore = config->EnableChunkReplicasThrottlingInHeartbeats
            ? IncrementalHeartbeatPerReplicasSemaphore_
            : IncrementalHeartbeatSemaphore_;

        auto mutationBuilder = BIND([=, this, this_ = MakeStrong(this)] {
            return CreateMutation(
                Bootstrap_->GetHydraFacade()->GetHydraManager(),
                &preparedRequest->NonSequoiaRequest,
                &preparedRequest->NonSequoiaResponse,
                &TDataNodeTracker::HydraIncrementalDataNodeHeartbeat,
                this);
        });
        auto replyCallback = BIND([=] (const TMutationResponse& /*response*/) {
            auto* response = &context->Response();
            response->Swap(&preparedRequest->NonSequoiaResponse);
            context->Reply();
        });
        hydraFacade->CommitMutationWithSemaphore(
            semaphore,
            std::move(context),
            std::move(mutationBuilder),
            std::move(replyCallback),
            semaphoreSlotsToAquire);
    }

    void ProcessIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

        for (auto locationUuid : chunkLocationUuids) {
            if (auto* existingLocation = FindChunkLocationByUuid(locationUuid)) {
                if (!IsObjectAlive(existingLocation)) {
                    // I am not sure ressurecting location here is a safe option as locations are
                    // created on primary master and replicated to secondary, so we'll have to ensure
                    // this location is created on secondary during resurrection and I'd rather wait
                    // for it to die.
                    THROW_ERROR_EXCEPTION("Cannot register node %Qv: there is a zombie location %v, please wait for it to die",
                        address,
                        locationUuid);
                } else {
                    ValidateObjectActive(existingLocation);
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
        }
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

        switch (node->GetLocalState()) {
            case ENodeState::Offline:
                ReplicateChunkLocations(node, chunkLocationUuids);
                MakeLocationsRegistered(node);
                break;
            case ENodeState::Restarted:
                ProcessRestartedNodeChunkLocations(node, chunkLocationUuids);
                break;
            default:
                YT_LOG_ALERT("Unexpected node state during registration (NodeAddress: %v, State: %v)",
                    node->GetDefaultAddress(),
                    node->GetLocalState());
        }

        if (isPrimaryMaster) {
            auto* dataNodeInfoExt = response->MutableExtension(NNodeTrackerClient::NProto::TDataNodeInfoExt::data_node_info_ext);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            SerializeMediumDirectory(dataNodeInfoExt->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, dataNodeInfoExt->mutable_medium_overrides());

            dataNodeInfoExt->set_require_location_uuids(false);
            dataNodeInfoExt->set_per_location_full_heartbeats_enabled(GetDynamicConfig()->EnablePerLocationFullHeartbeats);
            dataNodeInfoExt->set_location_indexes_in_heartbeats_enabled(
                GetDynamicConfig()->EnableLocationIndexesInDataNodeHeartbeats
                && request->location_indexes_in_heartbeats_supported());
            if (dataNodeInfoExt->location_indexes_in_heartbeats_enabled()) {
                SerializeLocationIndexes(this, chunkLocationUuids, dataNodeInfoExt->mutable_location_indexes());
            }
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
                const auto* mutationContext = GetCurrentMutationContext();

                location->SetNode(node);
                location->SetState(EChunkLocationState::Offline);
                location->SetLastSeenTime(mutationContext->GetTimestamp());
                node->ChunkLocations().push_back(location);
            }
        }

        node->ChunkLocations().shrink_to_fit();
    }

    void MakeLocationsRegistered(TNode* node) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        for (auto location : node->ChunkLocations()) {
            YT_VERIFY(location->GetState() == EChunkLocationState::Offline);
            location->SetState(EChunkLocationState::Registered);
        }
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChunkLocation, TChunkLocation);

    TEntityMap<TChunkLocation>* MutableChunkLocations() override
    {
        return &ChunkLocationMap_;
    }

    TChunkLocation* FindChunkLocationByUuid(TChunkLocationUuid locationUuid) const override
    {
        VerifyPersistentStateRead();

        auto it = ChunkLocationUuidToLocation_.find(locationUuid);
        return it == ChunkLocationUuidToLocation_.end() ? nullptr : it->second;
    }

    TChunkLocation* GetChunkLocationByUuid(TChunkLocationUuid locationUuid) const override
    {
        VerifyPersistentStateRead();

        auto* location = GetOrCrash(ChunkLocationUuidToLocation_, locationUuid);
        if (!IsObjectAlive(location)) {
            YT_LOG_ALERT("Zombie location is found using GetChunkLocationByUuid (LocationUuid: %v)",
                locationUuid);
        }
        return location;
    }

    TChunkLocation* FindChunkLocationByIndex(TChunkLocationIndex locationIndex) const override
    {
        VerifyPersistentStateRead();

        auto locationId = ObjectIdFromChunkLocationIndex(locationIndex);
        return ChunkLocationMap_.Find(locationId);
    }

    TChunkLocation* GetChunkLocationByIndex(TChunkLocationIndex locationIndex) const override
    {
        VerifyPersistentStateRead();

        auto location = FindChunkLocationByIndex(locationIndex);
        YT_VERIFY(IsObjectAlive(location));
        return location;
    }

    TChunkLocationIndex GenerateChunkLocationIndex()
    {
        // It seems easier to recover from this verify than from having different ids.
        YT_VERIFY(!Bootstrap_->IsSecondaryMaster());
        return GenerateCounterId(ChunkLocationIdGenerator_, InvalidChunkLocationIndex, MaxChunkLocationIndex);
    }

    TObjectId ObjectIdFromChunkLocationIndex(TChunkLocationIndex chunkLocationIndex) const
    {
        return NNodeTrackerClient::ObjectIdFromChunkLocationIndex(
            chunkLocationIndex,
            Bootstrap_->GetMulticellManager()->GetPrimaryCellTag());
    }

    TChunkLocationIndex ChunkLocationIndexFromObjectId(TObjectId objectId)
    {
        return NNodeTrackerClient::ChunkLocationIndexFromObjectId(objectId);
    }

    TChunkLocation* CreateChunkLocation(
        TChunkLocationUuid locationUuid,
        TObjectId hintId) override
    {
        YT_VERIFY(hintId || Bootstrap_->IsPrimaryMaster());

        auto it = ChunkLocationUuidToLocation_.find(locationUuid);
        if (it != ChunkLocationUuidToLocation_.end()) {
            auto* oldLocation = it->second;
            if (!IsObjectAlive(oldLocation)) {
                YT_LOG_ALERT("Creating location with existing uuid (Uuid: %v)",
                    locationUuid);
                MaybeUnregisterChunkLocationUuid(oldLocation->GetId(), locationUuid);
            } else {
                YT_ABORT();
            }
        }
        auto objectId = hintId ? hintId : ObjectIdFromChunkLocationIndex(GenerateChunkLocationIndex());

        auto locationHolder = TPoolAllocator::New<TChunkLocation>(objectId);
        auto* location = ChunkLocationMap_.Insert(objectId, std::move(locationHolder));
        location->SetUuid(locationUuid);

        if (Bootstrap_->IsSecondaryMaster()) {
            location->SetForeign();
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(location);

        RegisterChunkLocationUuid(location);

        auto locationIndex = ChunkLocationIndexFromObjectId(objectId);
        YT_LOG_DEBUG("Chunk location created (ObjectId: %v, LocationIndex: %v, LocationUuid: %v)",
            objectId,
            locationIndex,
            locationUuid);

        return location;
    }

    void RemoveLocationFromNode(TChunkLocation* location)
    {
        auto node = location->GetNode();
        if (node) {
            if (node->GetAggregatedState() != ENodeState::Offline) {
                YT_LOG_ALERT("Removing chunk location of a non-offline node (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
                    location->GetId(),
                    location->GetUuid(),
                    node->GetDefaultAddress());
            }
            std::erase(node->ChunkLocations(), location);
            location->SetNode(nullptr);
        }
    }

    void ZombifyChunkLocation(TChunkLocation* location) override
    {
        auto node = location->GetNode();
        RemoveLocationFromNode(location);

        YT_LOG_DEBUG("Chunk location zombified (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
            location->GetId(),
            location->GetUuid(),
            node ? node->GetDefaultAddress() : "<null>");
    }

    void DestroyChunkLocation(TChunkLocation* location) override
    {
        auto node = location->GetNode();
        if (node) {
            YT_LOG_ALERT("Zombie location is bound to node (LocationId: %v, LocationUuid: %v, NodeAddress: %v)",
            location->GetId(),
            location->GetUuid(),
            node->GetDefaultAddress());

            RemoveLocationFromNode(location);
        }

        YT_LOG_DEBUG("Chunk location destroyed (LocationId: %v, LocationUuid: %v)",
            location->GetId(),
            location->GetUuid());


        // COMPAT(aleksandra-zh): change to UnregisterChunkLocationUuid.
        MaybeUnregisterChunkLocationUuid(location->GetId(), location->GetUuid());
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
    const TAsyncSemaphorePtr FullHeartbeatPerReplicasSemaphore_ = New<TAsyncSemaphore>(/*totalSlots*/ 0, /*enableOverdraft*/ true);
    const TAsyncSemaphorePtr IncrementalHeartbeatPerReplicasSemaphore_ = New<TAsyncSemaphore>(/*totalSlots*/ 0, /*enableOverdraft*/ true);
    // COMPAT(cherepashka)
    const TAsyncSemaphorePtr FullHeartbeatSemaphore_ = New<TAsyncSemaphore>(/*totalSlots*/ 0);
    // COMPAT(cherepashka)
    const TAsyncSemaphorePtr LocationFullHeartbeatSemaphore_ = New<TAsyncSemaphore>(/*totalSlots*/ 0);
    // COMPAT(cherepashka)
    const TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_ = New<TAsyncSemaphore>(/*totalSlots*/ 0);

    TIdGenerator ChunkLocationIdGenerator_;
    bool PatchChunkLocationIds_ = false;

    NHydra::TEntityMap<TChunkLocation> ChunkLocationMap_;
    TChunkLocationUuidMap ChunkLocationUuidToLocation_;
    std::array<TChunkLocationUuidMap, ChunkLocationShardCount> ShardedChunkLocationUuidToLocation_;

    THashMap<TChunkLocationUuid, TError> LocationAlerts_;

    TPeriodicExecutorPtr DanglingLocationsCleaningExecutor_;
    // COMPAT(koloshmet)
    TInstant DanglingLocationsDefaultLastSeenTime_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    template <class THeartbeatContextPtr>
    struct THeartbeatRequest
        : public TRefCounted
    {
        using TReqHeartbeat = THeartbeatContextPtr::TUnderlying::TTypedRequest::TMessage;
        using TRspHeartbeat = THeartbeatContextPtr::TUnderlying::TTypedResponse::TMessage;

        TReqHeartbeat NonSequoiaRequest;
        TRspHeartbeat NonSequoiaResponse;

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

    // COMPAT(aleksandra-zh).
    void MaybeUnregisterChunkLocationUuid(TObjectId locationId, TChunkLocationUuid uuid)
    {
        // For locations zombified before the update, but destroyed after.
        auto uuidToLocationIt = ChunkLocationUuidToLocation_.find(uuid);
        if (uuidToLocationIt == ChunkLocationUuidToLocation_.end()) {
            YT_LOG_ALERT("Zombie location is not present in ChunkLocationUuidToLocation_ (LocationUuid: %v)",
                uuid);
        } else {
            if (uuidToLocationIt->second->GetId() != locationId) {
                YT_LOG_ALERT("There is already a new location in ChunkLocationUuidToLocation_ (LocationId: %v, LocationUuid: %v)",
                    locationId,
                    uuid);
                return;
            }
            ChunkLocationUuidToLocation_.erase(uuidToLocationIt);
        }

        auto& shard = GetChunkLocationShard(uuid);
        auto shardIt = shard.find(uuid);
        if (shardIt == shard.end()) {
            YT_LOG_ALERT("Zombie location is not present in shard (LocationUuid: %v)",
                uuid);
        } else {
            shard.erase(shardIt);
        }
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

    template <bool FullHeartbeat>
    TChunkLocation* FindAndValidateLocation(const TNode* node, TGuid uuid) const
    {
        VerifyPersistentStateRead();

        auto* location = FindChunkLocationByUuid(uuid);
        if (!IsObjectAlive(location)) {
            YT_LOG_ALERT(
                "Data node reported %v heartbeat with invalid location directory: "
                "location does not exist (NodeAddress: %v, LocationUuid: %v)",
                FullHeartbeat ? "full" : "incremental",
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
                FullHeartbeat ? "full" : "incremental",
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
                FullHeartbeat ? "full" : "incremental",
                node->GetDefaultAddress(),
                uuid,
                locationNode->GetDefaultAddress());
            THROW_ERROR_EXCEPTION(
                "Heartbeat's location directory cannot contain location which belongs to other node")
                << TErrorAttribute("location_uuid", uuid)
                << TErrorAttribute("node", locationNode->GetDefaultAddress());
        }
        return location;
    }

    template <bool FullHeartbeat>
    THashSet<NNodeTrackerClient::TChunkLocationIndex> ValidateAndGetLocationIndexes(
        const auto& chunkInfos,
        int locationDirectorySize,
        const auto& nodeAddress) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        THashSet<NNodeTrackerClient::TChunkLocationIndex> locationIndexes;
        for (const auto& chunkInfo : chunkInfos) {
            using TChunkInfo = std::decay_t<decltype(chunkInfo)>;
            static_assert(std::is_same_v<TChunkInfo, NChunkClient::NProto::TChunkAddInfo> || std::is_same_v<TChunkInfo, NChunkClient::NProto::TChunkRemoveInfo>,
                "TChunkInfo must be either TChunkAddInfo or TChunkRemoveInfo");

            auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());

            if constexpr (FullHeartbeat) {
                if (chunkInfo.caused_by_medium_change()) {
                    YT_LOG_ALERT(
                        "Data node reported full heartbeat with medium change chunk "
                        "(ChunkId: %v, NodeAddress: %v)",
                        chunkId,
                        nodeAddress);
                    THROW_ERROR_EXCEPTION("Full heartbeat from node %v contains chunk %v with medium change",
                        nodeAddress,
                        chunkId);
                }
            }

            if (chunkInfo.has_location_index()) {
                auto locationIndex = FromProto<NNodeTrackerClient::TChunkLocationIndex>(chunkInfo.location_index());
                if (chunkInfo.has_location_directory_index()) {
                    YT_LOG_ALERT(
                        "Data node reported heartbeat with both location index and location directory index "
                        "(ChunkId: %v, NodeAddress: %v, LocationIndex: %v)",
                        chunkId,
                        nodeAddress,
                        locationIndex);

                    THROW_ERROR_EXCEPTION("Heartbeat contains both location index and location directory index");
                }

                locationIndexes.insert(locationIndex);
                YT_LOG_TRACE("Data node reported heartbeat with location index (ChunkId: %v, NodeAddress: %v, LocationIndex: %v)",
                    chunkId,
                    nodeAddress,
                    locationIndex);
            } else {
                // COMPAT(grphil): remove after location directory is deprecated
                if (!chunkInfo.has_location_directory_index()) {
                    YT_LOG_ALERT(
                        "Data node reported heartbeat with no location index or location directory index "
                        "(ChunkId: %v, NodeAddress: %v)",
                        chunkId,
                        nodeAddress);

                    THROW_ERROR_EXCEPTION("Heartbeat contains no location index or location directory index");
                }
                if (chunkInfo.location_directory_index() < 0 || chunkInfo.location_directory_index() >= locationDirectorySize) {
                    YT_LOG_ALERT(
                        "Data node reported heartbeat with invalid location directory index "
                        "(ChunkId: %v, NodeAddress: %v, LocationIndex: %v)",
                        chunkId,
                        nodeAddress,
                        chunkInfo.location_directory_index());

                    THROW_ERROR_EXCEPTION("Heartbeat contains an incorrect location index");
                }
            }
        }
        return locationIndexes;
    }

    template <class TRequest>
    void ValidateHeartbeatRequest(
        const TNode* node,
        const TRequest& request) const
    {
        VerifyPersistentStateRead();

        static_assert(
            std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqFullHeartbeat>> ||
            std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat>> ||
            std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqLocationFullHeartbeat>>,
            "TRequest must be either TReqFullHeartbeat, TReqIncrementalHeartbeat or TReqLocationFullHeartbeat");

        constexpr bool fullHeartbeat = !std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat>>;

        const auto& nodeAddress = node->GetDefaultAddress();
        auto getLocationIndices = BIND([&] {
            if constexpr (std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqFullHeartbeat>>) {
                return ValidateAndGetLocationIndexes<fullHeartbeat>(request.chunks(), request.location_directory_size(), nodeAddress);
            } else if constexpr (std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqLocationFullHeartbeat>>) {
                return ValidateAndGetLocationIndexes<fullHeartbeat>(request.chunks(), 1, nodeAddress);
            } else {
                auto addIndexes = ValidateAndGetLocationIndexes<fullHeartbeat>(request.added_chunks(), request.location_directory_size(), nodeAddress);
                auto removeIndexes = ValidateAndGetLocationIndexes<fullHeartbeat>(request.removed_chunks(), request.location_directory_size(), nodeAddress);
                addIndexes.insert(removeIndexes.begin(), removeIndexes.end());
                return addIndexes;
            }
        });

        auto locationIndexes = WaitFor(std::move(getLocationIndices)
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
            .Run())
            .ValueOrThrow();

        for (auto locationIndex : locationIndexes) {
            auto location = FindChunkLocationByIndex(locationIndex);
            if (!IsObjectAlive(location)) {
                YT_LOG_ALERT(
                    "Data node reported heartbeat with invalid location index "
                    "(NodeAddress: %v, LocationIndex: %v)",
                    node->GetDefaultAddress(),
                    locationIndex);

                THROW_ERROR_EXCEPTION("Heartbeat contains an incorrect location index %v",
                    locationIndex);
            }
        }
    }

    template <class TRequest>
    TCompactVector<TChunkLocationIndex, TypicalChunkLocationCount> ParseLocationDirectory(
        const TNode* node,
        const TRequest& request) const
    {
        static_assert(
            std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqFullHeartbeat>> ||
            std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat>>,
            "TRequest must be either TReqFullHeartbeat or TReqIncrementalHeartbeat");

        constexpr bool fullHeartbeat = !std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat>>;

        auto rawLocationDirectory = FromProto<NDataNodeTrackerClient::TChunkLocationDirectory>(request.location_directory());

        TCompactVector<TChunkLocationIndex, TypicalChunkLocationCount> locationDirectoryIndexes;
        locationDirectoryIndexes.resize(request.location_directory_size());

        for (size_t index = 0; index < rawLocationDirectory.Uuids().size(); index++) {
            const auto& uuid = rawLocationDirectory.Uuids()[index];
            auto* location = FindAndValidateLocation<fullHeartbeat>(node, uuid);
            locationDirectoryIndexes[index] = location->GetIndex();
        }
        return locationDirectoryIndexes;
    }

    template <class THeartbeatContextPtr>
    TIntrusivePtr<THeartbeatRequest<THeartbeatContextPtr>> SplitRequest(
        THeartbeatContextPtr context,
        TRange<TChunkLocationIndex> locationDirectory) const
    {
        static_assert(
            std::is_same_v<THeartbeatContextPtr, TCtxFullHeartbeatPtr> ||
            std::is_same_v<THeartbeatContextPtr, TCtxLocationFullHeartbeatPtr> ||
            std::is_same_v<THeartbeatContextPtr, TCtxIncrementalHeartbeatPtr>);

        const auto& sequoiaChunkReplicasConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->SequoiaChunkReplicas;
        auto isSequoiaEnabled = sequoiaChunkReplicasConfig->Enable;
        const auto sequoiaChunkProbability = sequoiaChunkReplicasConfig->ReplicasPercentage;

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        auto doSplitRequest = BIND([&] {
            auto& originalRequest = context->Request();

            auto preparedRequest = NewWithOffloadedDtor<THeartbeatRequest<THeartbeatContextPtr>>(NRpc::TDispatcher::Get()->GetHeavyInvoker());

            auto& sequoiaRequest = preparedRequest->SequoiaRequest;
            preparedRequest->NonSequoiaRequest.CopyFrom(originalRequest);
            sequoiaRequest->set_node_id(originalRequest.node_id());

            auto splitChunks = [&] (const auto& chunkInfos) {
                for (auto chunkInfo : chunkInfos) {
                    using TChunkInfo = std::decay_t<decltype(chunkInfo)>;
                    auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
                    if (!chunkInfo.has_location_index()) {
                        auto locationDirectoryIndex = chunkInfo.location_directory_index();
                        chunkInfo.set_location_index(ToProto(locationDirectory[locationDirectoryIndex]));
                    }

                    if (isSequoiaEnabled && chunkReplicaFetcher->CanHaveSequoiaReplicas(chunkIdWithIndex.Id, sequoiaChunkProbability)) {
                        if constexpr (std::is_same_v<TChunkInfo, NChunkClient::NProto::TChunkAddInfo>) {
                            *sequoiaRequest->add_added_chunks() = std::move(chunkInfo);
                        } else {
                            *sequoiaRequest->add_removed_chunks() = std::move(chunkInfo);
                        }
                    } else {
                        if constexpr (std::is_same_v<THeartbeatContextPtr, TCtxIncrementalHeartbeatPtr>) {
                            if constexpr (std::is_same_v<TChunkInfo, NChunkClient::NProto::TChunkAddInfo>) {
                                *preparedRequest->NonSequoiaRequest.add_added_chunks() = std::move(chunkInfo);
                            } else {
                                *preparedRequest->NonSequoiaRequest.add_removed_chunks() = std::move(chunkInfo);
                            }
                        } else {
                            *preparedRequest->NonSequoiaRequest.add_chunks() = std::move(chunkInfo);
                        }
                    }
                }
            };

            // TODO(grphil): do this only if sequoia is enabled after location indexes are enabled everywhere
            if constexpr (std::is_same_v<THeartbeatContextPtr, TCtxIncrementalHeartbeatPtr>) {
                preparedRequest->NonSequoiaRequest.mutable_added_chunks()->Clear();
                preparedRequest->NonSequoiaRequest.mutable_removed_chunks()->Clear();
                splitChunks(originalRequest.added_chunks());
                splitChunks(originalRequest.removed_chunks());
            } else {
                preparedRequest->NonSequoiaRequest.mutable_chunks()->Clear();
                splitChunks(originalRequest.chunks());
            }

            // TODO(aleksandra-zh): this is a temporary hack until something better is done
            // with heavy request destruction hopefully in 25.4.
            using TRequestType = std::decay_t<decltype(originalRequest)>;
            TRequestType soonToBeDeadRequest;
            soonToBeDeadRequest.Swap(&originalRequest);

            return preparedRequest;
        });

        return WaitFor(std::move(doSplitRequest)
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
            .Run())
            .ValueOrThrow();
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

        YT_PROFILE_TIMING("/node_tracker/incremental_data_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing incremental data node heartbeat (NodeId: %v, Address: %v, State: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState());

            nodeTracker->UpdateLastSeenTime(node);
            nodeTracker->UpdateLastDataHeartbeatTime(node);

            ProcessIncrementalHeartbeat(node, request, response);
        }

        auto* mutationContext = GetCurrentMutationContext();

        if (auto time = node->GetNextValidationFullHeartbeatTime();
            GetDynamicConfig()->EnableValidationFullHeartbeats &&
            (!time.has_value() || *time < mutationContext->GetTimestamp()))
        {
            auto random = mutationContext->RandomGenerator()->Generate<ui64>();

            node->SetNextValidationFullHeartbeatTime(
                mutationContext->GetTimestamp() +
                GetDynamicConfig()->ValidationFullHeartbeatPeriod +
                TDuration::MilliSeconds(random % GetDynamicConfig()->ValidationFullHeartbeatSplay.MilliSeconds()));

            YT_LOG_DEBUG(
                "%v validation full heartbeat session for node (NodeId: %v, Address: %v, Time: %v)",
                time.has_value() ? "Rescheduling" : "Scheduling initial",
                nodeId,
                node->GetDefaultAddress(),
                node->GetNextValidationFullHeartbeatTime());

            response->set_schedule_validation_full_heartbeat_session(time.has_value());
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

        for (const auto& location : node->ChunkLocations()) {
            // We have checked that node state is not restarted, so there should not be any locations with restarted state.
            if (location->GetState() != EChunkLocationState::Registered) {
                YT_LOG_ALERT(
                    "Node has reported full heartbeat for location with invalid state (NodeAddress: %v, NodeId: %v, LocationUuid: %v, LocationState: %v)",
                    node->GetDefaultAddress(),
                    nodeId,
                    location->GetUuid(),
                    location->GetState());
                THROW_ERROR_EXCEPTION(
                    "Node has reported full heartbeat for location that has %Qlv state",
                    location->GetState());
            }
        }

        YT_PROFILE_TIMING("/node_tracker/full_data_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing full data node heartbeat (NodeId: %v, Address: %v, State: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState());

            nodeTracker->UpdateLastSeenTime(node);
            nodeTracker->UpdateLastDataHeartbeatTime(node);

            ProcessFullHeartbeat(node, request, response);
        }
    }

    void HydraProcessLocationFullHeartbeat(
        const TCtxLocationFullHeartbeatPtr& /*context*/,
        TReqLocationFullHeartbeat* request,
        TRspLocationFullHeartbeat* response)
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        auto validation = request->is_validation();

        auto nodeId = FromProto<TNodeId>(request->node_id());
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (!validation && node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Full data node heartbeat is already sent");
        }

        auto locationUuid = FromProto<TChunkLocationUuid>(request->location_uuid());

        YT_PROFILE_TIMING("/node_tracker/data_node_location_full_heartbeat_time") {
            YT_LOG_DEBUG("Processing data node location full heartbeat "
                "(NodeId: %v, Address: %v, LocationUuid: %v, State: %v, Validation: %v)",
                nodeId,
                node->GetDefaultAddress(),
                locationUuid,
                node->GetLocalState(),
                validation);

            if (!validation) {
                nodeTracker->UpdateLastSeenTime(node);
                nodeTracker->UpdateLastDataHeartbeatTime(node);
                PopulateChunkLocationStatistics(node, request->statistics());
            }

            const auto& chunkManager = Bootstrap_->GetChunkManager();
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
        nodeTracker->UpdateLastDataHeartbeatTime(node);

        auto& statistics = *request->mutable_statistics();
        auto reportedLocationUuids = GetLocationsReportedInStatistics(statistics);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ProcessDataNodeLocationsInFinalizeHeartbeat(node, reportedLocationUuids);

        PopulateChunkLocationStatistics(node, statistics.chunk_locations());
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

        auto danglingLocationIds = FromProto<std::vector<TObjectId>>(request->chunk_location_ids());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto id : danglingLocationIds) {
            auto* location = ChunkLocationMap_.Find(id);
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

        node->SetNextValidationFullHeartbeatTime(std::nullopt);

        for (auto location : node->ChunkLocations()) {
            location->SetState(EChunkLocationState::Offline);
            location->SetLastSeenTime(mutationContext->GetTimestamp());
        }
    }

    void OnNodeRestarted(TNode* node)
    {
        auto mutationContext = GetCurrentMutationContext();

        node->SetNextValidationFullHeartbeatTime(std::nullopt);

        for (auto location : node->ChunkLocations()) {
            location->SetState(EChunkLocationState::Restarted);
            location->SetLastSeenTime(mutationContext->GetTimestamp());
        }
    }

    void OnNodeZombified(TNode* node)
    {
        node->SetNextValidationFullHeartbeatTime(std::nullopt);

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

    std::vector<TChunkLocationUuid> GetLocationsReportedInStatistics(
        const NNodeTrackerClient::NProto::TDataNodeStatistics& statistics)
    {
        std::vector<TChunkLocationUuid> reportedLocations(statistics.chunk_locations_size());
        std::ranges::transform(
            statistics.chunk_locations(),
            reportedLocations.begin(),
            [] (const NNodeTrackerClient::NProto::TChunkLocationStatistics& statistics) {
                return FromProto<TChunkLocationUuid>(statistics.location_uuid());
            });
        std::ranges::sort(reportedLocations);
        return reportedLocations;
    }

    const TDynamicDataNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->DataNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig)
    {
        FullHeartbeatPerReplicasSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentChunkReplicasDuringFullHeartbeat);
        IncrementalHeartbeatPerReplicasSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentChunkReplicasDuringIncrementalHeartbeat);
        FullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentFullHeartbeats);
        LocationFullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentLocationFullHeartbeats);
        IncrementalHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentIncrementalHeartbeats);

        if (DanglingLocationsCleaningExecutor_) {
            DanglingLocationsCleaningExecutor_->SetPeriod(GetDynamicConfig()->DanglingLocationCleaner->CleanupPeriod);
        }

        if (oldConfig->ChunkManager->DataNodeTracker->EnableValidationFullHeartbeats &&
            !GetDynamicConfig()->EnablePerLocationFullHeartbeats)
        {
            ResetScheduledValidationFullHeartbeats();
        }
    }

    void ResetScheduledValidationFullHeartbeats()
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [_, node] : nodeTracker->Nodes()) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            node->SetNextValidationFullHeartbeatTime(std::nullopt);
        }
    }

    void ProcessRestartedNodeChunkLocations(
        TNode* node,
        const std::vector<TChunkLocationUuid>& chunkLocationUuids)
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());
        YT_VERIFY(HasMutationContext());

        auto sortedLocationUuids = chunkLocationUuids;
        std::ranges::sort(sortedLocationUuids);

        for (auto location : node->ChunkLocations()) {
            if (location->GetState() != EChunkLocationState::Restarted) {
                YT_LOG_ALERT(
                    "Locations was not set restarted after node is set restarted "
                    "(LocationUuid: %v, LocationState: %v, NodeAddress: %v)",
                    location->GetUuid(),
                    location->GetState(),
                    node->GetDefaultAddress());
                location->SetState(EChunkLocationState::Restarted);
            }

            if (!std::ranges::binary_search(sortedLocationUuids, location->GetUuid())) {
                YT_LOG_ALERT(
                    "Restarted node has disappeared location (NodeAddress: %v, NodeId: %v, LocationUuid: %v)",
                    node->GetDefaultAddress(),
                    node->GetId(),
                    location->GetUuid());
                THROW_ERROR_EXCEPTION(
                    "Restarted node has disappeared location")
                    << TErrorAttribute("node_address", node->GetDefaultAddress())
                    << TErrorAttribute("node_id", node->GetId())
                    << TErrorAttribute("location_uuid", location->GetUuid());
            }
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
                location->SetLastSeenTime(mutationContext->GetTimestamp());
                node->ChunkLocations().push_back(location);
                if (location->GetState() != EChunkLocationState::Restarted) {
                    location->SetState(EChunkLocationState::Registered);
                    YT_LOG_DEBUG(
                        "New location appeared in restarted node (NodeAddress: %v, NodeId: %v, LocationUuid: %v)",
                        node->GetDefaultAddress(),
                        node->GetId(),
                        location->GetUuid());
                } else {
                    YT_LOG_DEBUG(
                        "Location has restarted in restarted node (NodeAddress: %v, NodeId: %v, LocationUuid: %v)",
                        node->GetDefaultAddress(),
                        node->GetId(),
                        location->GetUuid());
                }
            }
        }

        node->ChunkLocations().shrink_to_fit();
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
        TCompactVector<TObjectId, defaultCleaningLimit> expiredDanglingLocations;

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
            ToProto(request.mutable_chunk_location_ids(), expiredDanglingLocations);

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
        ChunkLocationIdGenerator_.Reset();
        PatchChunkLocationIds_ = false;
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
        Save(context, ChunkLocationIdGenerator_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkLocationMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        ChunkLocationMap_.LoadValues(context);
        Load(context, DanglingLocationsDefaultLastSeenTime_);

        if (context.GetVersion() >= EMasterReign::ChunkLocationCounterId) {
            Load(context, ChunkLocationIdGenerator_);
        }
        PatchChunkLocationIds_ = context.GetVersion() < EMasterReign::ChunkLocationCounterId;
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

    // COMPAT(aleksandra-zh)
    void RemapLocation(TObjectId oldLocationId, TObjectId newLocationId)
    {
        // This could be more efficient, but hopefully we'll be fine.
        auto location = ChunkLocationMap_.Release(oldLocationId);
        location->SetId(newLocationId);
        YT_LOG_DEBUG("Changing location id to counter location id (OldId: %v, NewId: %v, LocationUuid: %v)",
            oldLocationId,
            newLocationId,
            location->GetUuid());
        ChunkLocationMap_.Insert(newLocationId, std::move(location));
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (auto [locationId, location] : ChunkLocationMap_) {
            RegisterChunkLocationUuid(location);
        }

        // COMPAT(aleksandra-zh)
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (PatchChunkLocationIds_ && multicellManager->IsPrimaryMaster()) {
            std::vector<TObjectId> locationIds;
            for (auto locationId : GetKeys(ChunkLocationMap_)) {
                locationIds.push_back(locationId);
            }

            std::sort(locationIds.begin(), locationIds.end());

            // This is fat.
            NProto::TReqRemapChunkLocationIds request;
            for (auto oldLocationId : locationIds) {
                auto locationIndex = GenerateChunkLocationIndex();
                auto newLocationId = ObjectIdFromChunkLocationIndex(locationIndex);

                RemapLocation(oldLocationId, newLocationId);

                auto* remap = request.add_location_id_remap();
                ToProto(remap->mutable_old_location_id(), oldLocationId);
                ToProto(remap->mutable_new_location_id(), newLocationId);
            }
            multicellManager->PostToSecondaryMasters(request);
        }
    }

    void HydraRemapChunkLocationIds(NProto::TReqRemapChunkLocationIds* request)
    {
        for (const auto& protoRemap : request->location_id_remap()) {
            auto oldLocationId = FromProto<TObjectId>(protoRemap.old_location_id());
            auto newLocationId = FromProto<TObjectId>(protoRemap.new_location_id());

            RemapLocation(oldLocationId, newLocationId);
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

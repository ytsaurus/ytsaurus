#include "features.h"
#include "master.h"
#include "private.h"
#include "type_handler_detail.h"
#include "object.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/domestic_medium.h>

#include <yt/yt/server/master/maintenance_tracker_server/maintenance_request.h>
#include <yt/yt/server/master/maintenance_tracker_server/maintenance_tracker.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_discovery_manager.h>

#include <yt/yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/subject.h>
#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NObjectServer {

using namespace NSecurityServer;
using namespace NMaintenanceTrackerServer;
using namespace NNodeTrackerServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterProxy
    : public TNonversionedObjectProxyBase<TMasterObject>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TMasterObject>;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        DISPATCH_YPATH_SERVICE_METHOD(GetClusterMeta,
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermissionByAcl);
        DISPATCH_YPATH_SERVICE_METHOD(AddMaintenance);
        DISPATCH_YPATH_SERVICE_METHOD(RemoveMaintenance);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CreateObject)
    {
        DeclareMutating();

        auto type = CheckedEnumCast<EObjectType>(request->type());
        auto ignoreExisting = request->ignore_existing();

        context->SetRequestInfo("Type: %v, IgnoreExisting: %v",
            type,
            ignoreExisting);

        auto attributes = request->has_object_attributes()
            ? FromProto(request->object_attributes())
            : nullptr;

        const auto& objectManager = Bootstrap_->GetObjectManager();

        TObject* object = nullptr;
        if (ignoreExisting) {
            auto optionalExistingObject = objectManager->FindObjectByAttributes(type, attributes.Get());
            if (!optionalExistingObject) {
                THROW_ERROR_EXCEPTION("\"ignore_existing\" option is not supported for type %Qlv",
                    type);
            }

            if (*optionalExistingObject) {
                object = *optionalExistingObject;
                YT_LOG_DEBUG("Existing object returned (Id: %v)",
                    object->GetId());
            }
        }

        if (!object) {
            object = objectManager->CreateObject(
                /*hintId*/ NullObjectId,
                type,
                attributes.Get());

            YT_LOG_DEBUG("Object created (Id: %v, Type: %v)",
                object->GetId(),
                type);
        }

        ToProto(response->mutable_object_id(), object->GetId());

        const auto& handler = objectManager->GetHandler(object);
        if (Any(handler->GetFlags() & ETypeFlags::TwoPhaseCreation)) {
            response->set_two_phase_creation(true);
        }

        context->SetResponseInfo("ObjectId: %v", object->GetId());
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermissionByAcl)
    {
        DeclareNonMutating();

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto* user = request->has_user()
            ? securityManager->GetUserByNameOrThrow(request->user(), true /*activeLifeStageOnly*/)
            : securityManager->GetAuthenticatedUser();
        auto permission = EPermission(request->permission());

        bool ignoreMissingSubjects = request->ignore_missing_subjects();

        context->SetRequestInfo("User: %v, Permission: %v",
            user->GetName(),
            permission);

        auto aclNode = ConvertToNode(TYsonString(request->acl()));
        TAccessControlList acl;
        if (ignoreMissingSubjects) {
            auto [deserializedAcl, missingSubjects] =
                DeserializeAclGatherMissingSubjectsOrThrow(aclNode, securityManager);
            ToProto(response->mutable_missing_subjects(), missingSubjects);
            acl = std::move(deserializedAcl);
        } else {
            acl = DeserializeAclOrThrow(aclNode, securityManager);
        }

        auto result = securityManager->CheckPermission(user, permission, acl);

        response->set_action(static_cast<int>(result.Action));
        if (result.Subject) {
            ToProto(response->mutable_subject_id(), result.Subject->GetId());
            response->set_subject_name(result.Subject->GetName());
        }

        context->SetResponseInfo("Action: %v", result.Action);
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetClusterMeta)
    {
        auto populateNodeDirectory = request->populate_node_directory();
        auto populateClusterDirectory = request->populate_cluster_directory();
        auto populateMediumDirectory = request->populate_medium_directory();
        auto populateCellDirectory = request->populate_cell_directory();
        auto populateMasterCacheNodeAddresses = request->populate_master_cache_node_addresses();
        auto populateTimestampProviderNodeAddresses = request->populate_timestamp_provider_node_addresses();
        auto populateFeatures = request->populate_features();

        context->SetRequestInfo(
            "PopulateNodeDirectory: %v, "
            "PopulateClusterDirectory: %v, "
            "PopulateMediumDirectory: %v, "
            "PopulateCellDirectory: %v, "
            "PopulateMasterCacheNodeAddresses: %v, "
            "PopulateTimestampProviderNodeAddresses: %v, "
            "PopulateFeatures: %v",
            populateNodeDirectory,
            populateClusterDirectory,
            populateMediumDirectory,
            populateCellDirectory,
            populateMasterCacheNodeAddresses,
            populateTimestampProviderNodeAddresses,
            populateFeatures);

        if (populateNodeDirectory) {
            TNodeDirectoryBuilder builder(response->mutable_node_directory());
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            for (auto [nodeId, node] : nodeTracker->Nodes()) {
                if (!IsObjectAlive(node)) {
                    continue;
                }
                builder.Add(node);
            }
        }

        if (populateClusterDirectory) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& rootService = objectManager->GetRootService();
            auto mapNode = ConvertToNode(SyncYPathGet(rootService, NTabletClient::GetCypressClustersPath()))->AsMap();
            auto* protoClusterDirectory = response->mutable_cluster_directory();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                auto* protoItem = protoClusterDirectory->add_items();
                protoItem->set_name(key);
                protoItem->set_config(ConvertToYsonString(child).ToString());
            }
        }

        if (populateMediumDirectory) {
            SerializeMediumDirectory(response->mutable_medium_directory(), Bootstrap_->GetChunkManager());
        }

        if (populateCellDirectory) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            const auto& masterCellConnectionConfigs = multicellManager->GetMasterCellConnectionConfigs();
            // TODO(cherepashka): to add all fields from TMasterConnectionConfig into NProto::TCellDirectory.
            auto* protoCellDirectory = response->mutable_cell_directory();

            auto addCell = [&] (const NApi::NNative::TMasterConnectionConfigPtr& cellConfig) {
                auto* cellItem = protoCellDirectory->add_items();
                ToProto(cellItem->mutable_cell_id(), cellConfig->CellId);

                if (cellConfig->Addresses) {
                    ToProto(cellItem->mutable_addresses(), *cellConfig->Addresses);
                }

                auto roles = multicellManager->GetMasterCellRoles(CellTagFromId(cellConfig->CellId));
                for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
                    if (Any(roles & EMasterCellRoles(role))) {
                        cellItem->add_roles(static_cast<i32>(role));
                    }
                }
            };

            addCell(masterCellConnectionConfigs->PrimaryMaster);
            for (const auto& secondaryMasterConfig : masterCellConnectionConfigs->SecondaryMasters) {
                addCell(secondaryMasterConfig);
            }
        }

        if (populateMasterCacheNodeAddresses) {
            const auto& masterCacheNodeAddresses = Bootstrap_->GetNodeTracker()->GetNodeAddressesForRole(
                NNodeTrackerClient::ENodeRole::MasterCache);
            ToProto(response->mutable_master_cache_node_addresses(), masterCacheNodeAddresses);
        }

        if (populateTimestampProviderNodeAddresses) {
            const auto& timestampProviderAddresses = Bootstrap_->GetNodeTracker()->GetNodeAddressesForRole(
                NNodeTrackerClient::ENodeRole::TimestampProvider);
            ToProto(response->mutable_timestamp_provider_node_addresses(), timestampProviderAddresses);
        }

        if (populateFeatures) {
            const auto& configManager = Bootstrap_->GetConfigManager();
            const auto& chunkManagerConfig = configManager->GetConfig()->ChunkManager;
            response->set_features(CreateFeatureRegistryYson(chunkManagerConfig->DeprecatedCodecIds).ToString());
        }

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, AddMaintenance)
    {
        DeclareMutating();

        auto component = CheckedEnumCast<EMaintenanceComponent>(request->component());
        auto type = CheckedEnumCast<EMaintenanceType>(request->type());

        context->SetRequestInfo(
            "Component: %v, "
            "Address: %v, "
            "Type: %v, "
            "Comment: %v, "
            "SupportsPerTargetResponse: %v",
            component,
            request->address(),
            type,
            request->comment(),
            request->supports_per_target_response());

        if (component == EMaintenanceComponent::ClusterNode || component == EMaintenanceComponent::Host) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                THROW_ERROR_EXCEPTION("Cluster node maintenance can only be added via primary master");
            }
        }

        std::optional<NCypressServer::TNodeId> componentRegistry;
        if (request->has_component_registry_id()) {
            FromProto(&componentRegistry.emplace(), request->component_registry_id());
        }

        const auto& maintenanceTracker = Bootstrap_->GetMaintenanceTracker();
        auto ids = maintenanceTracker->AddMaintenance(
            component,
            request->address(),
            type,
            request->comment(),
            componentRegistry);

        // COMPAT(kvk1920): Compatibility with pre-24.2 native clients.
        if (!request->supports_per_target_response()) {
            ToProto(
                response->mutable_id(),
                ids.size() == 1 ? ids.begin()->second : TMaintenanceId{});
        } else {
            // Result have to be sorted here because of Persistent Response
            // Keeper. TCompactFlatMap is sorted so just check that nobody
            // changed the TMaintenanceIdPerTarget type alias.
            static_assert(std::is_same_v<TMaintenanceIdPerTarget, TCompactFlatMap<TString, TMaintenanceId, 1>>);
            YT_ASSERT(std::is_sorted(ids.begin(), ids.end()));

            for (const auto& [target, id] : ids) {
                auto* proto = response->add_ids();
                proto->set_target(target);
                ToProto(proto->mutable_id(), id);
            }
        }
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, RemoveMaintenance)
    {
        DeclareMutating();

        auto component = CheckedEnumCast<EMaintenanceComponent>(request->component());

        std::optional<TCompactSet<TMaintenanceId, TypicalMaintenanceRequestCount>> ids;
        if (!request->ids().empty()) {
            ids.emplace();
            for (auto id : request->ids()) {
                ids->insert(FromProto<TMaintenanceId>(id));
            }
        }

        std::optional<TStringBuf> user;
        if (request->has_user()) {
            user = request->user();
        } else if (request->mine()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            user = securityManager->GetAuthenticatedUser()->GetName();
        }

        std::optional<EMaintenanceType> type;
        if (request->has_type()) {
            type = CheckedEnumCast<EMaintenanceType>(request->type());
        }

        context->SetRequestInfo(
            "Component: %v, Address: %v, Ids: %v, User: %v, Type: %v, SupportsPerTargetResponse: %v",
            component,
            request->address(),
            ids ? std::optional(TCompactVector<TMaintenanceId, TypicalMaintenanceRequestCount>(ids->begin(), ids->end())) : std::nullopt,
            user,
            type,
            request->supports_per_target_response());

        if (component == EMaintenanceComponent::ClusterNode || component == EMaintenanceComponent::Host) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                THROW_ERROR_EXCEPTION("Cluster node maintenance can only be added via primary master");
            }
        }

        THROW_ERROR_EXCEPTION_IF(request->has_user() && request->mine(),
            "At most one of {\"mine\", \"user\"} can be specified");

        const auto& maintenanceTracker = Bootstrap_->GetMaintenanceTracker();
        std::optional<NCypressServer::TNodeId> componentRegistry;
        if (request->has_component_registry_id()) {
            FromProto(&componentRegistry.emplace(), request->component_registry_id());
        }
        auto removedMaintenanceCounts = maintenanceTracker->RemoveMaintenance(
            component,
            request->address(),
            ids,
            user,
            type,
            componentRegistry);

        auto fillMaintenanceCount = [] (
            auto* proto,
            EMaintenanceType type,
            const TMaintenanceCounts& counts) {
                proto->set_type(ToProto<int>(type));
                proto->set_count(counts[type]);
            };

        // COMPAT(kvk1920): Compatibility with pre-24.2 proxies.
        if (!request->supports_per_target_response()) {
            TMaintenanceCounts totalCounts;
            for (const auto& [target, counts] : removedMaintenanceCounts) {
                for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                    totalCounts[type] += counts[type];
                }
            }

            for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                fillMaintenanceCount(response->add_removed_maintenance_counts(), type, totalCounts);
            }
        } else {
            response->set_supports_per_target_response(true);
            for (const auto& [target, counts] : removedMaintenanceCounts) {
                auto* perTargetResponse = response->add_removed_maintenance_counts_per_target();
                perTargetResponse->set_target(target);
                for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                    fillMaintenanceCount(perTargetResponse->add_counts(), type, counts);
                }
            }
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateMasterProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TMasterObject* object)
{
    return New<TMasterProxy>(bootstrap, metadata, object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

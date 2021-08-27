#include "features.h"
#include "master.h"
#include "private.h"
#include "type_handler_detail.h"
#include "object.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/subject.h>
#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_discovery_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NObjectServer {

using namespace NSecurityServer;
using namespace NNodeTrackerServer;
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
    TMasterProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TMasterObject* object)
        : TBase(bootstrap, metadata, object)
    { }

private:
    using TBase = TNonversionedObjectProxyBase<TMasterObject>;

    bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        DISPATCH_YPATH_SERVICE_METHOD(GetClusterMeta,
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermissionByAcl);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CreateObject)
    {
        DeclareMutating();

        auto type = EObjectType(request->type());
        auto ignoreExisting = request->ignore_existing();

        context->SetRequestInfo("Type: %v, IgnoreExisting: %v",
            type,
            ignoreExisting);

        auto attributes = request->has_object_attributes()
            ? FromProto(request->object_attributes())
            : nullptr;

        const auto& objectManager = Bootstrap_->GetObjectManager();
        if (ignoreExisting) {
            auto maybeExistingObject = objectManager->FindObjectByAttributes(type, attributes.Get());
            if (!maybeExistingObject) {
                THROW_ERROR_EXCEPTION("\"ignore_existing\" option is not supported for type %Qlv",
                    type);
            }

            if (auto* existingObject = *maybeExistingObject) {
                auto existingObjectId = existingObject->GetId();
                ToProto(response->mutable_object_id(), existingObjectId);

                context->SetResponseInfo("ExistingObjectId: %v", existingObjectId);
                context->Reply();
                return;
            }
        }

        auto* object = objectManager->CreateObject(
            NullObjectId,
            type,
            attributes.Get());

        const auto& objectId = object->GetId();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object created (Id: %v, Type: %v)",
            objectId,
            type);

        ToProto(response->mutable_object_id(), objectId);

        context->SetResponseInfo("ObjectId: %v", objectId);
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

        std::vector<TString> missingSubjects;

        Deserialize(acl, aclNode, securityManager, ignoreMissingSubjects ? &missingSubjects : nullptr);

        ToProto(response->mutable_missing_subjects(), missingSubjects);

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
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto* protoMediumDirectory = response->mutable_medium_directory();
            for (auto [mediumId, medium] : chunkManager->Media()) {
                auto* protoItem = protoMediumDirectory->add_items();
                protoItem->set_index(medium->GetIndex());
                protoItem->set_name(medium->GetName());
                protoItem->set_priority(medium->GetPriority());
            }
        }

        if (populateCellDirectory) {
            const auto& cellMasterConfig = Bootstrap_->GetConfig();
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            auto* protoCellDirectory = response->mutable_cell_directory();

            auto addCell = [&] (const NElection::TCellConfigPtr& cellConfig) {
                auto* cellItem = protoCellDirectory->add_items();
                ToProto(cellItem->mutable_cell_id(), cellConfig->CellId);

                for (const auto& peerConfig : cellConfig->Peers) {
                    if (peerConfig.Address) {
                        cellItem->add_addresses(*peerConfig.Address);
                    }
                }

                auto roles = multicellManager->GetMasterCellRoles(CellTagFromId(cellConfig->CellId));
                for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
                    if (Any(roles & EMasterCellRoles(role))) {
                        cellItem->add_roles(static_cast<i32>(role));
                    }
                }
            };

            addCell(cellMasterConfig->PrimaryMaster);
            for (const auto& secondaryMasterConfig : cellMasterConfig->SecondaryMasters) {
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
};

IObjectProxyPtr CreateMasterProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TMasterObject* object)
{
    return New<TMasterProxy>(bootstrap, metadata, object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

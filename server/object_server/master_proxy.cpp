#include "master.h"
#include "private.h"
#include "type_handler_detail.h"
#include "object.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/subject.h>
#include <yt/server/security_server/acl.h>
#include <yt/server/security_server/user.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/server/node_tracker_server/node_tracker.h>
#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>

#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/medium.h>

#include <yt/ytlib/object_client/master_ypath.pb.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NObjectServer {

using namespace NTransactionServer;
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
    typedef TNonversionedObjectProxyBase<TMasterObject> TBase;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        DISPATCH_YPATH_HEAVY_SERVICE_METHOD(GetClusterMeta);
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermissionByAcl);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CreateObject)
    {
        DeclareMutating();

        auto type = EObjectType(request->type());

        context->SetRequestInfo("Type: %v",
            type);

        auto attributes = request->has_object_attributes()
            ? FromProto(request->object_attributes())
            : std::unique_ptr<IAttributeDictionary>();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->CreateObject(
            NullObjectId,
            type,
            attributes.get());

        const auto& objectId = object->GetId();

        LOG_DEBUG_UNLESS(IsRecovery(), "Object created (Id: %v, Type: %v)",
            objectId,
            type);

        ToProto(response->mutable_object_id(), objectId);

        context->SetResponseInfo("ObjectId: %v", objectId);
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermissionByAcl)
    {
        DeclareNonMutating();

        auto securityManager = Bootstrap_->GetSecurityManager();

        auto* user = request->has_user()
            ? securityManager->GetUserByNameOrThrow(request->user())
            : securityManager->GetAuthenticatedUser();
        auto permission = EPermission(request->permission());

        context->SetRequestInfo("User: %v, Permission: %v",
            user->GetName(),
            permission);

        auto aclNode = ConvertToNode(TYsonString(request->acl()));
        TAccessControlList acl;
        Deserialize(acl, aclNode, securityManager);

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
        context->SetRequestInfo(
            "PopulateNodeDirectory: %v, "
            "PopulateClusterDirectory: %v, "
            "PopulateMediumDirectory: %v",
            populateNodeDirectory,
            populateClusterDirectory,
            populateMediumDirectory);

        if (populateNodeDirectory) {
            TNodeDirectoryBuilder builder(response->mutable_node_directory());
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            for (const auto& pair : nodeTracker->Nodes()) {
                const auto* node = pair.second;
                if (!IsObjectAlive(node)) {
                    continue;
                }
                builder.Add(node);
            }
        }

        if (populateClusterDirectory) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& rootService = objectManager->GetRootService();
            auto mapNode = ConvertToNode(SyncYPathGet(rootService, "//sys/clusters"))->AsMap();
            auto* protoClusterDirectory = response->mutable_cluster_directory();
            for (const auto& pair : mapNode->GetChildren()) {
                auto* protoItem = protoClusterDirectory->add_items();
                protoItem->set_name(pair.first);
                protoItem->set_config(ConvertToYsonString(pair.second).GetData());
            }
        }

        if (populateMediumDirectory) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto* protoMediumDirectory = response->mutable_medium_directory();
            for (const auto& pair : chunkManager->Media()) {
                const auto* medium = pair.second;
                auto* protoItem = protoMediumDirectory->add_items();
                protoItem->set_index(medium->GetIndex());
                protoItem->set_name(medium->GetName());
                protoItem->set_priority(medium->GetPriority());
            }
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

} // namespace NObjectServer
} // namespace NYT

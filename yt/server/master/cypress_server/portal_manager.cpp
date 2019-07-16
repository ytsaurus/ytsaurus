#include "portal_manager.h"
#include "cypress_manager.h"
#include "portal_entrance_node.h"
#include "portal_entrance_type_handler.h"
#include "portal_exit_node.h"
#include "portal_exit_type_handler.h"
#include "virtual_portal_node_map.h"
#include "helpers.h"
#include "private.h"

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/cypress_server/proto/portal_manager.pb.h>

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/automaton.h>
#include <yt/server/master/cell_master/serialize.h>
#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/acl.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NHydra;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TPortalManager::TImpl
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::PortalManager)
    {
        RegisterLoader(
            "PortalManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "PortalManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "PortalManager.Keys",
            BIND(&TImpl::SaveValues, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "PortalManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraCreatePortalExit, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemovePortalEntrance, Unretained(this)));
}

    void Initialize()
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->RegisterHandler(CreatePortalEntranceTypeHandler(Bootstrap_));
        cypressManager->RegisterHandler(CreatePortalExitTypeHandler(Bootstrap_));
        cypressManager->RegisterHandler(CreatePortalEntranceMapTypeHandler(Bootstrap_));
        cypressManager->RegisterHandler(CreatePortalExitMapTypeHandler(Bootstrap_));
    }

    void RegisterEntranceNode(
        TPortalEntranceNode* node,
        const IAttributeDictionary& inheritedAttributes,
        const IAttributeDictionary& explicitAttributes)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto path = cypressManager->GetNodePath(node,  nullptr);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto effectiveAcl = securityManager->GetEffectiveAcl(node);

        // Turn off ACL inheritance, replace ACL with effective ACL.
        node->Acd().SetEntries(effectiveAcl);
        node->Acd().SetInherit(false);

        NProto::TReqCreatePortalExit request;
        ToProto(request.mutable_entrance_node_id(), node->GetId());
        ToProto(request.mutable_account_id(), node->GetAccount()->GetId());
        request.set_path(path);
        request.set_acl(ConvertToYsonString(effectiveAcl).GetData());
        ToProto(request.mutable_inherited_node_attributes(), inheritedAttributes);
        ToProto(request.mutable_explicit_node_attributes(), explicitAttributes);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, node->GetExitCellTag());

        YT_VERIFY(EntranceNodes_.emplace(node->GetId(), node).second);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Portal entrance registered (EntranceNodeId: %v, ExitCellTag: %v, Account: %v, Path: %v)",
            node->GetId(),
            node->GetExitCellTag(),
            node->GetAccount()->GetName(),
            path);
    }

    void DestroyEntranceNode(TPortalEntranceNode* node)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (EntranceNodes_.erase(node->GetId()) != 1) {
            return;
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Portal entrance unregistered (NodeId: %v)",
            node->GetId());
    }

    void DestroyExitNode(TPortalExitNode* node)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (ExitNodes_.erase(node->GetId()) != 1) {
            return;
        }

        auto entranceNodeId = MakePortalEntranceNodeId(node->GetId(), node->GetEntranceCellTag());

        NProto::TReqRemovePortalEntrance request;
        ToProto(request.mutable_entrance_node_id(), entranceNodeId);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, node->GetEntranceCellTag());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Portal exit unregistered (NodeId: %v)",
            node->GetId());
    }

    DEFINE_BYREF_RO_PROPERTY(TEntranceNodeMap, EntranceNodes);
    DEFINE_BYREF_RO_PROPERTY(TExitNodeMap, ExitNodes);

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& /*context*/) const
    { }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;
        Save(context, EntranceNodes_);
        Save(context, ExitNodes_);
    }


    void LoadKeys(NCellMaster::TLoadContext& /*context*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, EntranceNodes_);
        Load(context, ExitNodes_);
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        EntranceNodes_.clear();
        ExitNodes_.clear();
    }


    void HydraCreatePortalExit(NProto::TReqCreatePortalExit* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto entranceNodeId = FromProto<TObjectId>(request->entrance_node_id());
        auto accountId = FromProto<TAccountId>(request->account_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccount(accountId);

        TAccessControlList acl;
        Deserialize(acl, ConvertToNode(TYsonString(request->acl())), securityManager);

        auto explicitAttributes = FromProto(request->explicit_node_attributes());
        auto inheritedAttributes = FromProto(request->inherited_node_attributes());

        const auto& path = request->path();

        auto exitNodeId = MakePortalExitNodeId(entranceNodeId, Bootstrap_->GetCellTag());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& handler = cypressManager->GetHandler(EObjectType::PortalExit);
        auto* node = cypressManager->CreateNode(
            exitNodeId,
            NotReplicatedCellTag,
            handler,
            account,
            nullptr,
            inheritedAttributes.get(),
            explicitAttributes.get())->As<TPortalExitNode>();

        node->SetEntranceCellTag(CellTagFromId(entranceNodeId));

        // Turn off ACL inheritance, replace ACL with effective ACL.
        node->Acd().SetInherit(false);
        node->Acd().SetEntries(acl);

        node->SetPath(path);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(node);

        handler->FillAttributes(node, inheritedAttributes.get(), explicitAttributes.get());

        YT_VERIFY(ExitNodes_.emplace(node->GetId(), node).second);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Portal exit registered (ExitNodeId: %v, Account: %v, Path: %v)",
            exitNodeId,
            account->GetName(),
            path);
    }

    void HydraRemovePortalEntrance(NProto::TReqRemovePortalEntrance* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto entranceNodeId = FromProto<TObjectId>(request->entrance_node_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* entranceNode = cypressManager->FindNode(TVersionedObjectId(entranceNodeId));
        if (!IsObjectAlive(entranceNode)) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Attempt to remove a non-existing portal entrance node (EntranceNodeId: %v)",
                entranceNodeId);
            return;
        }

        auto* parentNode = entranceNode->GetParent();

        // XXX(babenko)
        auto entranceProxy = cypressManager->GetNodeProxy(entranceNode);
        auto parentProxy = cypressManager->GetNodeProxy(parentNode)->AsComposite();
        parentProxy->RemoveChild(entranceProxy);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Portal entrance removed (EntranceNodeId: %)",
            entranceNodeId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TPortalManager::TPortalManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TPortalManager::Initialize()
{
    Impl_->Initialize();
}

void TPortalManager::RegisterEntranceNode(
    TPortalEntranceNode* node,
    const IAttributeDictionary& inheritedAttributes,
    const IAttributeDictionary& explicitAttributes)
{
    Impl_->RegisterEntranceNode(
        node,
        inheritedAttributes,
        explicitAttributes);
}

void TPortalManager::DestroyEntranceNode(TPortalEntranceNode* node)
{
    Impl_->DestroyEntranceNode(node);
}

void TPortalManager::DestroyExitNode(TPortalExitNode* node)
{
    Impl_->DestroyExitNode(node);
}

DELEGATE_BYREF_RO_PROPERTY(TPortalManager, TPortalManager::TEntranceNodeMap, EntranceNodes, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TPortalManager, TPortalManager::TExitNodeMap, ExitNodes, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

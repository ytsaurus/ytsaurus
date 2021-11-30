#include "access_control_node_type_handler.h"
#include "access_control_node.h"
#include "access_control_node_proxy.h"

#include "node_detail.h"
#include "private.h"

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TAccessControlNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TAccessControlNode>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TAccessControlNode>;

public:
    using TBase::TBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::AccessControlNode;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TAccessControlNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateAccessControlNodeProxy(
            Bootstrap_,
            &Metadata_, 
            transaction,
            trunkNode);
    }

    std::unique_ptr<TAccessControlNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        auto ns = context.ExplicitAttributes->GetAndRemove<TString>(EInternedAttributeKey::Namespace.Unintern());

        auto implHolder = TBase::DoCreate(id, context);
        implHolder->SetNamespace(ns);

        YT_LOG_DEBUG("Access control node created (Id: %v, Namespace: %v)",
            id,
            ns);
        
        return implHolder;
    }

    void DoBranch(
        const TAccessControlNode* originatingNode,
        TAccessControlNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetNamespace(originatingNode->GetNamespace());
    }

    void DoMerge(
        TAccessControlNode* originatingNode,
        TAccessControlNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        YT_ASSERT(originatingNode->GetNamespace() == branchedNode->GetNamespace());
    }

    void DoClone(
        TAccessControlNode* sourceNode,
        TAccessControlNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        clonedTrunkNode->SetNamespace(sourceNode->GetNamespace());
    }

    void DoBeginCopy(
        TAccessControlNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);
        Save(*context, node->GetNamespace());
    }

    void DoEndCopy(
        TAccessControlNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(trunkNode, context, factory);

        trunkNode->SetNamespace(NYT::Load<TString>(*context));
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateAccessControlNodeTypeHandler(TBootstrap *bootstrap)
{
    return New<TAccessControlNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NCypressServer

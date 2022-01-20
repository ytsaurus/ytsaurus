#include "chaos_replicated_table_node_type_handler.h"
#include "chaos_replicated_table_node.h"
#include "chaos_replicated_table_node_proxy.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChaosServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableTypeHandler
    : public TCypressNodeTypeHandlerBase<TChaosReplicatedTableNode>
{
public:
    using TCypressNodeTypeHandlerBase::TCypressNodeTypeHandlerBase;

    EObjectType GetObjectType() const override
    {
        return EObjectType::ChaosReplicatedTable;
    }

    ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TChaosReplicatedTableNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateChaosReplicatedTableNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    std::unique_ptr<TChaosReplicatedTableNode> DoCreate(
        TVersionedNodeId id,
        const TCreateNodeContext& context) override
    {
        auto replicationCardId = context.ExplicitAttributes->GetAndRemove<TReplicationCardId>("replication_card_id");
        if (TypeFromId(replicationCardId) != EObjectType::ReplicationCard) {
            THROW_ERROR_EXCEPTION("Malformed replication card id");
        }

        auto implHolder = TCypressNodeTypeHandlerBase::DoCreate(id, context);
        implHolder->SetReplicationCardId(replicationCardId);

        return implHolder;
    }

    void DoBranch(
        const TChaosReplicatedTableNode* originatingNode,
        TChaosReplicatedTableNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TCypressNodeTypeHandlerBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetReplicationCardId(originatingNode->GetReplicationCardId());
    }

    void DoClone(
        TChaosReplicatedTableNode* sourceNode,
        TChaosReplicatedTableNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TCypressNodeTypeHandlerBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

        clonedTrunkNode->SetReplicationCardId(sourceNode->GetReplicationCardId());
    }

    void DoBeginCopy(
        TChaosReplicatedTableNode* node,
        TBeginCopyContext* context) override
    {
        TCypressNodeTypeHandlerBase::DoBeginCopy(node, context);

        using NYT::Save;
        Save(*context, node->GetReplicationCardId());
    }

    void DoEndCopy(
        TChaosReplicatedTableNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TCypressNodeTypeHandlerBase::DoEndCopy(trunkNode, context, factory);

        using NYT::Load;
        trunkNode->SetReplicationCardId(Load<TReplicationCardId>(*context));
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateChaosReplicatedTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TChaosReplicatedTableTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

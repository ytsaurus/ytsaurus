#include "partitioned_table_node_type_handler.h"

#include "partitioned_table_node.h"
#include "partitioned_table_node_proxy.h"

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/cypress_server/node_detail.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TPartitionedTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TPartitionedTableNode>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TPartitionedTableNode>;

public:
    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override
    {
        return EObjectType::PartitionedTable;
    }

    virtual NYTree::ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TPartitionedTableNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreatePartitionedTableNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual void DoBranch(
        const TPartitionedTableNode* originatingNode,
        TPartitionedTableNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);
    }

    virtual void DoMerge(
        TPartitionedTableNode* originatingNode,
        TPartitionedTableNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);
    }

    virtual void DoClone(
        TPartitionedTableNode* sourceNode,
        TPartitionedTableNode* clonedTrunkNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);
    }

    virtual bool HasBranchedChangesImpl(
        TPartitionedTableNode* originatingNode,
        TPartitionedTableNode* branchedNode) override
    {
        if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
            return true;
        }

        return false;
    }

    virtual void DoBeginCopy(
        TPartitionedTableNode* node,
        TBeginCopyContext* context) override
    {
        TBase::DoBeginCopy(node, context);
    }

    virtual void DoEndCopy(
        TPartitionedTableNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory) override
    {
        TBase::DoEndCopy(trunkNode, context, factory);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreatePartitionedTableTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return New<TPartitionedTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

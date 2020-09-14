#include "partitioned_table_node_proxy.h"

#include "partitioned_table_node.h"
#include "private.h"

#include <yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/server/master/object_server/object.h>

namespace NYT::NTableServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TPartitionedTableNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TPartitionedTableNode>
{
public:
    TPartitionedTableNodeProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TPartitionedTableNode* trunkNode)
    : TBase(
        bootstrap,
        metadata,
        transaction,
        trunkNode)
    { }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TPartitionedTableNode>;

    virtual ENodeType GetType() const override
    {
        return ENodeType::Entity;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreatePartitionedTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TPartitionedTableNode* trunkNode)
{
    return New<TPartitionedTableNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer



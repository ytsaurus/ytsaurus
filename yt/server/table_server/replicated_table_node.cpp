#include "replicated_table_node.h"
#include "table_node_type_handler_detail.h"

namespace NYT {
namespace NTableServer {

using namespace NObjectClient;
using namespace NCypressServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableNode::TReplicatedTableNode(const TVersionedNodeId& id)
    : TTableNode(id)
{ }

EObjectType TReplicatedTableNode::GetObjectType() const
{
    return EObjectType::ReplicatedTable;
}

void TReplicatedTableNode::Save(TSaveContext& context) const
{
    TTableNode::Save(context);

    using NYT::Save;
}

void TReplicatedTableNode::Load(TLoadContext& context)
{
    TTableNode::Load(context);

    using NYT::Load;
}

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateReplicatedTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TReplicatedTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT


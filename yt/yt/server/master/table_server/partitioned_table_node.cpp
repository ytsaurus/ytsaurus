#include "partitioned_table_node.h"

namespace NYT::NTableServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TPartitionedTableNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TPartitionedTableNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);
}

void TPartitionedTableNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

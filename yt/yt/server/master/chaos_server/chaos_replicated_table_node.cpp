#include "chaos_replicated_table_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChaosServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TChaosReplicatedTableNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TChaosReplicatedTableNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, ReplicationCardId_);
}

void TChaosReplicatedTableNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    Load(context, ReplicationCardId_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer


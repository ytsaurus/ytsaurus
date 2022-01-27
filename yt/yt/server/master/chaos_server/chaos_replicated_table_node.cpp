#include "chaos_replicated_table_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChaosServer {

using namespace NYTree;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

ENodeType TChaosReplicatedTableNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TChaosReplicatedTableNode::Save(TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, ReplicationCardId_);
    Save(context, OwnsReplicationCard_);
}

void TChaosReplicatedTableNode::Load(TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    Load(context, ReplicationCardId_);
    if (context.GetVersion() >= EMasterReign::OwnsReplicationCard) {
        Load(context, OwnsReplicationCard_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer


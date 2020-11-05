#include "portal_entrance_node.h"

namespace NYT::NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TPortalEntranceNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TPortalEntranceNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, RemovalStarted_);
    Save(context, ExitCellTag_);
}

void TPortalEntranceNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    Load(context, RemovalStarted_);
    Load(context, ExitCellTag_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

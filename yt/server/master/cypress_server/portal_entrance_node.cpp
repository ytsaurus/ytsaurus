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
    TCypressNodeBase::Save(context);

    using NYT::Save;
    Save(context, ExitCellTag_);
}

void TPortalEntranceNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    Load(context, ExitCellTag_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

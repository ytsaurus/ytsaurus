#include "rootstock_node.h"

namespace NYT::NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TRootstockNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TRootstockNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;

    Save(context, ScionId_);
}

void TRootstockNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;

    Load(context, ScionId_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#include "link_node.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TLinkNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TLinkNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, TargetPath_);
}

void TLinkNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    Load(context, TargetPath_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

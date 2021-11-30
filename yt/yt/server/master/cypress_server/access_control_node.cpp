#include "access_control_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TAccessControlNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TAccessControlNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    NYT::Save(context, Namespace_);
}

void TAccessControlNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    NYT::Load(context, Namespace_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

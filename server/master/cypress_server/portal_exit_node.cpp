#include "portal_exit_node.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

void TPortalExitNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, EntranceCellTag_);
    Save(context, Path_);
}

void TPortalExitNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    Load(context, EntranceCellTag_);
    Load(context, Path_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

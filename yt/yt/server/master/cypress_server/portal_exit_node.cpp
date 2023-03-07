#include "portal_exit_node.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

void TPortalExitNode::Save(NCellMaster::TSaveContext& context) const
{
    TMapNode::Save(context);

    using NYT::Save;
    Save(context, RemovalStarted_);
    Save(context, EntranceCellTag_);
    Save(context, Path_);
    Save(context, Key_);
    Save(context, ParentId_);
}

void TPortalExitNode::Load(NCellMaster::TLoadContext& context)
{
    TMapNode::Load(context);

    using NYT::Load;
    // COMPAT(babenko)
    if (context.GetVersion() >= NCellMaster::EMasterReign::TwoSidedPortalRemoval) {
        Load(context, RemovalStarted_);
    }
    Load(context, EntranceCellTag_);
    Load(context, Path_);
    // COMPAT(babenko)
    if (context.GetVersion() >= NCellMaster::EMasterReign::MorePortalAttributes) {
        Load(context, Key_);
        Load(context, ParentId_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

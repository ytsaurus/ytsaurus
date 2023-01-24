#include "scion_node.h"

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TScionNode::Save(TSaveContext& context) const
{
    TMapNode::Save(context);

    using NYT::Save;

    Save(context, RootstockId_);
    Save(context, RemovalStarted_);
    Save(context, Key_);
    Save(context, ParentId_);
    Save(context, Path_);
    Save(context, EffectiveInheritableAttributes_);
    Save(context, EffectiveAnnotationPath_);
    Save(context, DirectAcd_);
}

void TScionNode::Load(TLoadContext& context)
{
    TMapNode::Load(context);

    using NYT::Load;

    Load(context, RootstockId_);
    Load(context, RemovalStarted_);
    Load(context, Key_);
    Load(context, ParentId_);
    Load(context, Path_);
    Load(context, EffectiveInheritableAttributes_);
    Load(context, EffectiveAnnotationPath_);
    Load(context, DirectAcd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

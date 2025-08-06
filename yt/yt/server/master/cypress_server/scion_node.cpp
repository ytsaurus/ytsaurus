#include "scion_node.h"

#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TScionNode::TScionNode(TVersionedNodeId nodeId)
    : TSequoiaMapNode(nodeId)
    , DirectAcd_(this)
{ }

void TScionNode::Save(TSaveContext& context) const
{
    TSequoiaMapNode::Save(context);

    using NYT::Save;

    Save(context, RootstockId_);
    Save(context, EffectiveInheritableAttributes_);
    Save(context, EffectiveAnnotationPath_);
    Save(context, DirectAcd_);
}

void TScionNode::Load(TLoadContext& context)
{
    TSequoiaMapNode::Load(context);

    using NYT::Load;

    Load(context, RootstockId_);
    Load(context, EffectiveInheritableAttributes_);
    Load(context, EffectiveAnnotationPath_);
    Load(context, DirectAcd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

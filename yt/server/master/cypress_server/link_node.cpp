#include "link_node.h"
#include "shard.h"
#include "portal_exit_node.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/core/ypath/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NObjectClient;

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

TYPath TLinkNode::ComputeEffectiveTargetPath(const TYPath& targetPath, TCypressShard* shard)
{
    if (shard && shard->GetRoot()->GetType() == EObjectType::PortalExit) {
        const auto* portalExit = shard->GetRoot()->As<TPortalExitNode>();
        auto optionalSuffix = NYPath::TryComputeYPathSuffix(targetPath, portalExit->GetPath());
        if (!optionalSuffix) {
            THROW_ERROR_EXCEPTION("Link target path must start with %v",
                portalExit->GetPath());
        }
        return FromObjectId(portalExit->GetId()) + *optionalSuffix;
    } else {
        return targetPath;
    }
}

TYPath TLinkNode::ComputeEffectiveTargetPath() const
{
    return ComputeEffectiveTargetPath(TargetPath_, GetTrunkNode()->GetShard());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

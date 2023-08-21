#include "host.h"

#include "node.h"
#include "rack.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TString THost::GetLowercaseObjectName() const
{
    return Format("host %Qv", GetName());
}

TString THost::GetCapitalizedObjectName() const
{
    return Format("Host %Qv", GetName());
}

TString THost::GetObjectPath() const
{
    return Format("//sys/hosts/%v", GetName());
}

void THost::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, Rack_);
    Save(context, Nodes_);
}

void THost::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, Rack_);
    Load(context, Nodes_);
}

void THost::AddNode(TNode* node)
{
    Nodes_.push_back(node);
}

void THost::RemoveNode(TNode* node)
{
    auto nodeIt = std::find(Nodes_.begin(), Nodes_.end(), node);
    YT_VERIFY(nodeIt != Nodes_.end());
    Nodes_.erase(nodeIt);
}

TCompactVector<TNode*, 1> THost::GetNodesWithFlavor(ENodeFlavor flavor) const
{
    TCompactVector<TNode*, 1> result;
    for (auto* node : Nodes_) {
        if (node->Flavors().contains(flavor)) {
            result.push_back(node);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

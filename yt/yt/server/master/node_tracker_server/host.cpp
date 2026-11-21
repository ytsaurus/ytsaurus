#include "host.h"

#include "node.h"
#include "rack.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string THost::GetLowercaseObjectName() const
{
    return Format("host %Qv", GetName());
}

std::string THost::GetCapitalizedObjectName() const
{
    return Format("Host %Qv", GetName());
}

TYPath THost::GetObjectPath() const
{
    return Format("//sys/hosts/%v", NYPath::ToYPathLiteral(GetName()));
}

void THost::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, Rack_);
    PHOENIX_REGISTER_FIELD(3, Nodes_);
}

PHOENIX_DEFINE_TYPE(THost);

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
    for (auto node : Nodes_) {
        if (node->Flavors().contains(flavor)) {
            result.push_back(node);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

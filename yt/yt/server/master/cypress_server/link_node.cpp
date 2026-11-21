#include "link_node.h"
#include "shard.h"
#include "portal_exit_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

ENodeType TLinkNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TLinkNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressNode>();

    PHOENIX_REGISTER_FIELD(1, TargetPath_);
}

PHOENIX_DEFINE_TYPE(TLinkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

#include "portal_entrance_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TPortalEntranceNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TPortalEntranceNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressNode>();

    PHOENIX_REGISTER_FIELD(1, RemovalStarted_);
    PHOENIX_REGISTER_FIELD(2, ExitCellTag_);
}

PHOENIX_DEFINE_TYPE(TPortalEntranceNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

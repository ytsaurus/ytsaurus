#include "rootstock_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ENodeType TRootstockNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TRootstockNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressNode>();

    PHOENIX_REGISTER_FIELD(1, ScionId_);
}

PHOENIX_DEFINE_TYPE(TRootstockNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

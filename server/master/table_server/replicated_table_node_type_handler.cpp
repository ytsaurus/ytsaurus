#include "table_node.h"
#include "table_node_type_handler_detail.h"

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateReplicatedTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TReplicatedTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer


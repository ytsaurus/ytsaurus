#include "table_node.h"
#include "table_node_type_handler_detail.h"

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer


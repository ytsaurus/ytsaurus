#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cypress_server/public.h>

namespace NYT::NObjectServer  {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateEstimatedCreationTimeMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

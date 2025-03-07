#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateAccountMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateAccountResourceUsageLeaseMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateUserMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateGroupMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateNetworkProjectMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateProxyRoleMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NApi::EProxyKind proxyKind);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

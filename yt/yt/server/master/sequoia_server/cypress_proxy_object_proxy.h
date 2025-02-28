#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateCypressProxyObjectProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TCypressProxyObject* proxyObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer

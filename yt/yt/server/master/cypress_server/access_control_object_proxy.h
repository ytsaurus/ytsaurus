#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateAccessControlObjectProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TAccessControlObject* accessControlObject);

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateAccessControlObjectNamespaceProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TAccessControlObjectNamespace* accessControlObjectNamespace);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

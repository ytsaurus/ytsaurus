#pragma once

#include "private.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateUserProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TUser* user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT


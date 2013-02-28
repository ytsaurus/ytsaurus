#pragma once

#include "private.h"

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateAccountProxy(
    NCellMaster::TBootstrap* bootstrap,
    TAccount* account);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT


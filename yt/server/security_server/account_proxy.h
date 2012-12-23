#pragma once

#include "account.h"
#include "private.h"

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateAccountProxy(
    NCellMaster::TBootstrap* bootstrap,
    const TAccountId& id,
    TAccountMetaMap* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT


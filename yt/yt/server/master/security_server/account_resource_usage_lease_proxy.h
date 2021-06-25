#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateAccountResourceUsageLeaseProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TAccountResourceUsageLease* accountResourceUsageLease);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

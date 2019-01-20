#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/transaction_server/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateLockProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TLock* lock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

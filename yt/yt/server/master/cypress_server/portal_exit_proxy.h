#pragma once

#include "public.h"

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreatePortalExitProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TPortalExitNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

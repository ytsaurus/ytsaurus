#pragma once

#include "public.h"

#include <yt/server/cypress_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateTabletCellMapProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    NCypressServer::TMapNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

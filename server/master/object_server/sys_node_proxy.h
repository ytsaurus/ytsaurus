#pragma once

#include "public.h"

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateSysNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    NCypressServer::TMapNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

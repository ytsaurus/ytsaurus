#pragma once

#include "public.h"

#include <yt/server/cypress_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateSysNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    NCypressServer::TMapNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

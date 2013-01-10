#pragma once

#include "public.h"

#include <server/cypress_server/public.h>

#include <server/transaction_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateTableNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TTableNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

